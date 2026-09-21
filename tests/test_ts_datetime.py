"""KAN-1160: MFTECmd's $MFT CSV had no column TimeSketch accepts as a time.

The TimeSketch import client takes a CSV only if it has a `datetime` column or
a column whose name contains "time". MFTECmd names its timestamp columns
`Created0x10`, `LastModified0x10`, `LastRecordChange0x10`, `LastAccess0x10`
(plus the `0x30` pair) -- none matches -- so every `$MFT` upload raised
ValueError and the NTFS timeline was always empty, while the task itself
reported success.

`ts_datetime` imports no celery / openrelik SDK, so unlike the worker module it
can be imported directly here.
"""

import csv
import io

import pytest

from src.ts_datetime import (
    DATETIME_COLUMN,
    SOURCE_COLUMN,
    add_datetime_column,
    needs_datetime_column,
    to_iso_utc,
)

# The real $MFT header, as MFTECmd writes it.
MFT_HEADER = [
    "EntryNumber", "SequenceNumber", "InUse", "ParentEntryNumber",
    "ParentSequenceNumber", "ParentPath", "FileName", "Extension", "FileSize",
    "ReferenceCount", "ReparseTarget", "IsDirectory", "HasAds", "IsAds",
    "SI<FN", "uSecZeros", "Copied", "SiFlags", "NameType",
    "Created0x10", "Created0x30", "LastModified0x10", "LastModified0x30",
    "LastRecordChange0x10", "LastRecordChange0x30", "LastAccess0x10",
    "LastAccess0x30", "UpdateSequenceNumber", "LogfileSequenceNumber",
    "SecurityId", "ObjectIdFileDroid", "LoggedUtilStream", "ZoneIdContents",
]

USN_HEADER = [
    "Name", "Extension", "EntryNumber", "SequenceNumber", "ParentEntryNumber",
    "ParentSequenceNumber", "ParentPath", "UpdateSequenceNumber",
    "UpdateTimestamp", "UpdateReasons", "FileAttributes", "OffsetToData",
    "SourceFile",
]


def _mft_row(created="2026-08-22 11:23:45.1234567"):
    row = [""] * len(MFT_HEADER)
    row[MFT_HEADER.index("Created0x10")] = created
    row[MFT_HEADER.index("LastModified0x10")] = "2026-08-22 12:00:00.0000000"
    return row


def _write_csv(path, header, rows, bom=False):
    encoding = "utf-8-sig" if bom else "utf-8"
    with open(path, "w", encoding=encoding, newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)


def _read_csv(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.reader(fh))


# --- the importer rule ------------------------------------------------------

def test_real_mft_header_is_rejected_by_timesketch():
    """The actual defect: not one $MFT column contains the substring 'time'."""
    assert needs_datetime_column(MFT_HEADER) is True


def test_real_usn_header_is_already_accepted():
    """`UpdateTimestamp` satisfies the importer, so $J must be left alone."""
    assert needs_datetime_column(USN_HEADER) is False


def test_existing_datetime_column_is_not_touched():
    assert needs_datetime_column(["a", "datetime", "b"]) is False


def test_header_matching_ignores_case_and_whitespace():
    assert needs_datetime_column([" DateTime "]) is False
    assert needs_datetime_column(["UpdateTIMEstamp"]) is False


def test_empty_header_needs_nothing():
    assert needs_datetime_column([]) is False
    assert needs_datetime_column(None) is False


# --- timestamp conversion ---------------------------------------------------

def test_converts_dotnet_tick_precision_to_iso_utc():
    """MFTECmd writes 7 fractional digits; ISO/pandas carry 6."""
    assert to_iso_utc("2026-08-22 11:23:45.1234567") == "2026-08-22T11:23:45.123456+00:00"


def test_fraction_is_truncated_not_rounded():
    """Rounding could invent a timestamp later than the one MFTECmd recorded."""
    assert to_iso_utc("2026-08-22 11:23:45.9999999") == "2026-08-22T11:23:45.999999+00:00"


def test_short_fraction_is_padded_to_microseconds():
    assert to_iso_utc("2026-08-22 11:23:45.12") == "2026-08-22T11:23:45.120000+00:00"


def test_value_without_fraction_still_converts():
    assert to_iso_utc("2026-08-22 11:23:45") == "2026-08-22T11:23:45+00:00"


def test_already_iso_separator_is_accepted():
    assert to_iso_utc("2026-08-22T11:23:45.1234567") == "2026-08-22T11:23:45.123456+00:00"


def test_result_matches_the_format_timesketch_asks_for():
    """TimeSketch documents %Y-%m-%dT%H:%M:%S%z -- it must parse round-trip."""
    from datetime import datetime, timezone

    parsed = datetime.fromisoformat(to_iso_utc("2026-08-22 11:23:45.1234567"))
    assert parsed.tzinfo is not None
    assert parsed.utcoffset().total_seconds() == 0
    assert parsed.astimezone(timezone.utc).year == 2026


@pytest.mark.parametrize("value", ["", None, "   ", "not a date", "22/08/2026 11:23", "2026-08-22"])
def test_unusable_values_return_none(value):
    assert to_iso_utc(value) is None


# --- the file rewrite -------------------------------------------------------

def test_datetime_column_is_appended_and_equals_created(tmp_path):
    path = tmp_path / "$MFT_MFTECmd_output.csv"
    _write_csv(path, MFT_HEADER, [_mft_row(), _mft_row("2020-01-02 03:04:05.6543210")])

    assert add_datetime_column(str(path)) == (2, 0)

    rows = _read_csv(path)
    assert rows[0][-1] == DATETIME_COLUMN
    assert rows[1][-1] == "2026-08-22T11:23:45.123456+00:00"
    assert rows[2][-1] == "2020-01-02T03:04:05.654321+00:00"


def test_all_eight_timestamp_columns_survive_unchanged(tmp_path):
    """Stu's design: `datetime` is added, nothing is replaced or dropped, and
    Created appears twice so the event is easy to analyse."""
    path = tmp_path / "mft.csv"
    _write_csv(path, MFT_HEADER, [_mft_row()])

    add_datetime_column(str(path))

    rows = _read_csv(path)
    header = rows[0]
    for column in (
        "Created0x10", "Created0x30", "LastModified0x10", "LastModified0x30",
        "LastRecordChange0x10", "LastRecordChange0x30", "LastAccess0x10",
        "LastAccess0x30",
    ):
        assert column in header, f"lost timestamp column {column}"
    assert header[:-1] == MFT_HEADER
    # Created is present twice: as itself and as the TimeSketch pivot.
    assert rows[1][header.index("Created0x10")] == "2026-08-22 11:23:45.1234567"
    assert rows[1][header.index(DATETIME_COLUMN)] == "2026-08-22T11:23:45.123456+00:00"


def test_one_event_per_record_no_row_explosion(tmp_path):
    path = tmp_path / "mft.csv"
    _write_csv(path, MFT_HEADER, [_mft_row() for _ in range(25)])

    total, _ = add_datetime_column(str(path))

    assert total == 25
    assert len(_read_csv(path)) == 26  # header + 25 rows, nothing multiplied


def test_usn_csv_is_left_byte_identical(tmp_path):
    """Scope guard: $J already imports, so it must not be rewritten at all."""
    path = tmp_path / "$UsnJrnl$J_MFTECmd_output.csv"
    _write_csv(path, USN_HEADER, [["x"] * len(USN_HEADER)])
    before = path.read_bytes()

    assert add_datetime_column(str(path)) is None
    assert path.read_bytes() == before


def test_rerun_is_idempotent(tmp_path):
    """A second pass must not append a second datetime column."""
    path = tmp_path / "mft.csv"
    _write_csv(path, MFT_HEADER, [_mft_row()])

    add_datetime_column(str(path))
    after_first = path.read_bytes()
    assert add_datetime_column(str(path)) is None
    assert path.read_bytes() == after_first


def test_unparseable_rows_are_kept_not_dropped(tmp_path):
    """The CSV is a forensic artefact too -- silently discarding records from it
    would be a worse failure than the one being fixed."""
    path = tmp_path / "mft.csv"
    _write_csv(path, MFT_HEADER, [_mft_row(), _mft_row(""), _mft_row("garbage")])

    total, unparsed = add_datetime_column(str(path))

    assert (total, unparsed) == (3, 2)
    rows = _read_csv(path)
    assert len(rows) == 4
    assert rows[1][-1] == "2026-08-22T11:23:45.123456+00:00"
    assert rows[2][-1] == ""
    assert rows[3][-1] == ""


def test_short_row_does_not_abort_the_file(tmp_path):
    """A ragged row would raise IndexError and lose the whole 10 GB rewrite."""
    path = tmp_path / "mft.csv"
    _write_csv(path, MFT_HEADER, [_mft_row(), ["1", "2"]])

    total, unparsed = add_datetime_column(str(path))

    assert (total, unparsed) == (2, 1)
    assert len(_read_csv(path)) == 3


def test_bom_does_not_hide_the_source_column(tmp_path):
    """MFTECmd writes a BOM; read as plain utf-8 the first header becomes
    '﻿EntryNumber' and a naive lookup misses the column."""
    path = tmp_path / "mft.csv"
    _write_csv(path, MFT_HEADER, [_mft_row()], bom=True)

    assert add_datetime_column(str(path)) == (1, 0)
    assert _read_csv(path)[1][-1] == "2026-08-22T11:23:45.123456+00:00"


def test_quoted_commas_and_newlines_survive(tmp_path):
    """Naive line splitting would corrupt these; the csv module must be used."""
    path = tmp_path / "mft.csv"
    row = _mft_row()
    row[MFT_HEADER.index("FileName")] = "in,voice\nQ3.txt"
    row[MFT_HEADER.index("ParentPath")] = '.\\Users\\"quoted"'
    _write_csv(path, MFT_HEADER, [row])

    add_datetime_column(str(path))

    rows = _read_csv(path)
    assert rows[1][MFT_HEADER.index("FileName")] == "in,voice\nQ3.txt"
    assert rows[1][MFT_HEADER.index("ParentPath")] == '.\\Users\\"quoted"'
    assert rows[1][-1] == "2026-08-22T11:23:45.123456+00:00"


def test_large_field_beyond_csv_default_limit(tmp_path):
    """ResidentDataBase64 can exceed csv's 128 KB default field cap, which
    raises _csv.Error and would abort the rewrite partway."""
    path = tmp_path / "mft.csv"
    row = _mft_row()
    row[MFT_HEADER.index("ResidentDataBase64") if "ResidentDataBase64" in MFT_HEADER
        else MFT_HEADER.index("ZoneIdContents")] = "A" * 200_000
    _write_csv(path, MFT_HEADER, [row])

    assert add_datetime_column(str(path)) == (1, 0)


def test_missing_source_column_leaves_file_alone(tmp_path):
    """No Created0x10 (e.g. a $Boot CSV) -- nothing to derive from, so don't
    pretend. The file is untouched and the caller can see None."""
    path = tmp_path / "boot.csv"
    _write_csv(path, ["EntryNumber", "VolumeSerialNumber"], [["1", "abc"]])
    before = path.read_bytes()

    assert add_datetime_column(str(path)) is None
    assert path.read_bytes() == before


def test_empty_file_is_handled(tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")
    assert add_datetime_column(str(path)) is None


def test_no_temp_file_is_left_behind(tmp_path):
    path = tmp_path / "mft.csv"
    _write_csv(path, MFT_HEADER, [_mft_row()])

    add_datetime_column(str(path))

    assert list(tmp_path.iterdir()) == [path]


def test_source_column_is_created_by_default():
    """Guard the design decision: datetime comes from Created, not modified."""
    assert SOURCE_COLUMN == "Created0x10"
