"""KAN-1160: give MFTECmd CSV output a column TimeSketch will accept as the
event time.

The TimeSketch import client (`timesketch_import_client/importer.py`,
`add_data_frame`) accepts a CSV only if it has a column named exactly
`datetime`, or a column whose name contains the substring "time". Otherwise it
raises:

    ValueError: Need a field called datetime in the data frame that is
    formatted according using this format string: %Y-%m-%dT%H:%M:%S%z...

MFTECmd names the `$MFT` timestamp columns `Created0x10`, `LastModified0x10`,
`LastRecordChange0x10`, `LastAccess0x10` and the matching `0x30` pair. *None of
them contains "time"*, so every `$MFT` upload failed and the NTFS timeline was
always empty -- while the MFTECmd task itself reported success and left a
full-size artefact on disk. Measured on case-0305: 28/28 `$MFT` CSVs rejected.

The `$UsnJrnl$J` CSV is unaffected -- it carries `UpdateTimestamp`, which
satisfies the substring rule -- so this module deliberately leaves it alone.

Design (Stu, 2026-08-31): `datetime` is derived from *Created*, and all eight
original timestamp columns are kept unchanged alongside it. Created therefore
appears twice -- once as the TimeSketch pivot and once as its own column --
which keeps one event per MFT record (no row explosion, no duplicated events)
while every other timestamp stays queryable on that same event.

This module imports nothing from celery or the openrelik SDK so it can be unit
tested directly, the same reason `test_usn_journal_names.py` parses with `ast`.
"""

import csv
import os
import re
import sys

# The column `datetime` is derived from. Deliberately a single source with no
# fallback chain: a fallback would silently make `datetime` mean a different
# timestamp on different rows, which is exactly the kind of "looks fine, is
# wrong" behaviour this ticket exists to remove.
SOURCE_COLUMN = "Created0x10"

DATETIME_COLUMN = "datetime"

# MFTECmd emits naive UTC timestamps as `YYYY-MM-DD HH:MM:SS.fffffff` -- seven
# fractional digits, i.e. .NET tick precision. Verified against a live 10.5 GB
# `$MFT` CSV on case-0305: 200,000/200,000 sampled rows matched this shape, and
# every fractional part was exactly 7 digits.
_MFTECMD_TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?$"
)

# Python (and pandas) carry microseconds, not .NET ticks, so the 7-digit
# fraction is truncated to 6. Truncating rather than rounding keeps the value
# monotonic with the source and never invents a timestamp later than the one
# MFTECmd recorded.
_MICROSECOND_DIGITS = 6

# MFT records store FILETIME, which is UTC. MFTECmd does not apply an offset
# unless asked to, so the naive value is UTC and is stamped as such.
_UTC_SUFFIX = "+00:00"


def _raise_field_size_limit():
    """`ResidentDataBase64` can exceed csv's default 128 KB field cap.

    Hitting it raises `_csv.Error: field larger than field limit`, which would
    abort the rewrite partway. Walk the limit down from sys.maxsize because
    Python rejects values wider than a C long on some platforms.
    """
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return limit
        except OverflowError:
            limit //= 2


def needs_datetime_column(fieldnames):
    """True if TimeSketch would reject this CSV for lacking a usable time field.

    Mirrors the importer's own rule, so we rewrite only the files that would
    actually fail. Anything TimeSketch already accepts is left byte-identical
    -- notably `$UsnJrnl$J` via `UpdateTimestamp`.
    """
    if not fieldnames:
        return False
    headers = [(h or "").strip().lower() for h in fieldnames]
    if DATETIME_COLUMN in headers:
        return False
    if any("time" in h for h in headers):
        return False
    return True


def to_iso_utc(value):
    """`2026-08-22 11:23:45.1234567` -> `2026-08-22T11:23:45.123456+00:00`.

    Returns None if the value is empty or not a shape MFTECmd produces, so the
    caller can count it rather than emit a malformed timestamp.
    """
    if not value:
        return None
    match = _MFTECMD_TIMESTAMP.match(value.strip())
    if not match:
        return None
    date_part, time_part, fraction = match.groups()
    if fraction:
        fraction = (fraction + "0" * _MICROSECOND_DIGITS)[:_MICROSECOND_DIGITS]
        return f"{date_part}T{time_part}.{fraction}{_UTC_SUFFIX}"
    return f"{date_part}T{time_part}{_UTC_SUFFIX}"


def add_datetime_column(csv_path, source_column=SOURCE_COLUMN, logger=None):
    """Append a `datetime` column to an MFTECmd CSV, in place.

    Returns a (total_rows, unparsed_rows) tuple, or None when no rewrite was
    needed or possible -- a CSV TimeSketch already accepts, one with no
    `source_column`, or an empty/headerless file.

    Streams through the `csv` module rather than pandas: `$MFT` CSVs reach
    10.5 GB on a single host, and quoted fields can contain embedded commas and
    newlines, so naive line splitting would corrupt them.

    *Every input row is written out.* Rows whose source timestamp cannot be
    parsed get an empty `datetime` and are counted, never dropped -- the CSV is
    also a forensic artefact in its own right, and silently discarding records
    from it would be a worse failure than the one this fixes.
    """

    def _log(level, message):
        if logger is not None:
            getattr(logger, level)(message)

    _raise_field_size_limit()

    # `utf-8-sig` strips the BOM MFTECmd writes, which would otherwise make the
    # first header `﻿EntryNumber` and defeat the source-column lookup.
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        try:
            header = next(reader)
        except StopIteration:
            _log("info", f"MFTECmd CSV is empty, no datetime column added: {csv_path}")
            return None

        if not needs_datetime_column(header):
            return None

        stripped = [(h or "").strip() for h in header]
        if source_column not in stripped:
            _log(
                "warning",
                f"MFTECmd CSV has no '{source_column}' column, so TimeSketch will "
                f"reject it and no NTFS timeline will appear: {csv_path}",
            )
            return None

        source_index = stripped.index(source_column)
        temp_path = f"{csv_path}.kan1160.tmp"
        total = 0
        unparsed = 0

        try:
            with open(temp_path, "w", encoding="utf-8", newline="") as destination:
                writer = csv.writer(destination)
                writer.writerow(header + [DATETIME_COLUMN])
                for row in reader:
                    total += 1
                    # Short rows would otherwise raise IndexError and abort the
                    # whole file; treat them the same as an unparseable value.
                    raw = row[source_index] if source_index < len(row) else ""
                    converted = to_iso_utc(raw)
                    if converted is None:
                        unparsed += 1
                        converted = ""
                    writer.writerow(row + [converted])
        except Exception:
            # Never leave a truncated CSV where a complete one used to be.
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    # Outside the `with`: the source handle must be closed before the temp file
    # can take its place, or Windows raises PermissionError on os.replace.
    try:
        os.replace(temp_path, csv_path)
    except Exception:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise

    if unparsed:
        _log(
            "warning",
            f"MFTECmd CSV: {unparsed} of {total} rows had an unparseable "
            f"'{source_column}' and were written with an empty "
            f"'{DATETIME_COLUMN}': {csv_path}",
        )
    _log(
        "info",
        f"MFTECmd CSV: added '{DATETIME_COLUMN}' from '{source_column}' "
        f"for {total} rows ({unparsed} unparseable): {csv_path}",
    )
    return total, unparsed
