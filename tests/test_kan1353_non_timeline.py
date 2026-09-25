"""KAN-1353: $Boot / $Secure:$SDS outputs are typed as non-timeline reference data.

Neither MFTECmd output has a timestamp, so TimeSketch can never take them. Typed
`openrelik:mftecmd:mftecmd`, they were "skipped" by the upload this branch is
chained to, and every NTFS upload reported coverage_complete=false. They are
now `openrelik:mftecmd:reference`, which the TS worker counts as not-a-timeline.

AST-based like the other tests here: mftecmd.py imports celery /
openrelik_common at module load, absent in CI. The function and its constants
are lifted out of the source and executed in isolation.
"""
import ast
from pathlib import Path

import pytest

MODULE = Path(__file__).resolve().parent.parent / "src" / "mftecmd.py"
SOURCE = MODULE.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
NAMES = {
    "TIMELINE_DATA_TYPE", "REFERENCE_DATA_TYPE", "NON_TIMELINE_NAMES",
    "NON_TIMELINE_MIME_TYPES", "USN_JOURNAL_NAMES", "output_data_type",
}


def _load():
    body = [
        n for n in TREE.body
        if (isinstance(n, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in NAMES for t in n.targets))
        or (isinstance(n, ast.FunctionDef) and n.name in NAMES)
    ]
    ns = {}
    exec(compile(ast.Module(body=body, type_ignores=[]), str(MODULE), "exec"), ns)  # noqa: S102
    return ns


NS = _load()
output_data_type = NS["output_data_type"]
REFERENCE = "openrelik:mftecmd:reference"
TIMELINE = "openrelik:mftecmd:mftecmd"


@pytest.mark.parametrize("name", ["$Boot", "$Secure_$SDS", "$Secure%3A$SDS", "$Secure:$SDS"])
def test_non_temporal_artefacts_are_reference(name):
    assert output_data_type({"display_name": name}) == REFERENCE


def test_mangled_boot_typed_by_mime_is_reference():
    # KAN-1110: a $Boot whose filename was mangled still routes here by mime.
    f = {"display_name": "Boot-renamed", "mime_type": "application/x-ntfs-boot"}
    assert output_data_type(f) == REFERENCE


@pytest.mark.parametrize("name", ["$MFT", "$I30", "INDX", "$LogFile", *NS["USN_JOURNAL_NAMES"]])
def test_timeline_artefacts_stay_timeline(name):
    # $MFT must stay a timeline: a $MFT the importer rejects is a REAL coverage
    # gap (KAN-1160) and must keep reading as one.
    assert output_data_type({"display_name": name}) == TIMELINE


def test_mft_by_mime_stays_timeline():
    f = {"display_name": "mangled", "mime_type": "application/x-ntfs-mft"}
    assert output_data_type(f) == TIMELINE


def test_reference_type_keeps_the_cleanup_prefix():
    # The cleanup/janitor classifiers match data types on `openrelik:mftecmd:`.
    assert REFERENCE.startswith("openrelik:mftecmd:")


def test_output_file_uses_output_data_type():
    # The CSV's data_type must come from the helper, not a hardcoded literal.
    assert "data_type=output_data_type(file)" in SOURCE
    assert 'data_type="openrelik:mftecmd:mftecmd"' not in SOURCE
