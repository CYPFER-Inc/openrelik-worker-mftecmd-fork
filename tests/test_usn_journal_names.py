"""KAN-1109: MFTECmd never ran on the USN journal, on any case.

The extracted journal arrives as `$UsnJrnl$J`, but the worker carried the
accepted names as TWO separate literals -- one in COMPATIBLE_INPUTS (which
gates whether the file is processed at all) and one in the `-m $MFT`
enrichment check (which gates whether USN records get resolved to real
paths). Neither contained `$UsnJrnl$J`, so the journal was silently dropped.

These tests parse the module with `ast` rather than importing it: mftecmd.py
pulls in celery + openrelik_common + pathvalidate at module load, none of
which are installed in CI here.
"""

import ast
from pathlib import Path

MODULE = Path(__file__).resolve().parent.parent / "src" / "mftecmd.py"
TREE = ast.parse(MODULE.read_text(encoding="utf-8"))


def _assign(name):
    for node in ast.walk(TREE):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    return node.value
    raise AssertionError(f"{name} not found in mftecmd.py")


def test_journal_names_include_the_shape_we_actually_get():
    """`$UsnJrnl$J` is the name the extraction step really produces."""
    names = ast.literal_eval(_assign("USN_JOURNAL_NAMES"))
    assert "$UsnJrnl$J" in names


def test_journal_names_keep_the_previously_known_shapes():
    """Regression guard: the fix must ADD a shape, not swap one in."""
    names = ast.literal_eval(_assign("USN_JOURNAL_NAMES"))
    for legacy in ("$UsnJrnl%3A$J", "$J", "UsnJrnl-J"):
        assert legacy in names, f"dropped previously-accepted name {legacy}"


def test_compatible_inputs_uses_the_constant_not_a_literal():
    """COMPATIBLE_INPUTS must splat USN_JOURNAL_NAMES. A second literal list
    is what let the two copies drift in the first place."""
    src = MODULE.read_text(encoding="utf-8")
    assert "*USN_JOURNAL_NAMES," in src
    # the old duplicated literal must be gone from the filenames block
    assert '"$UsnJrnl%3A$J","$J","UsnJrnl-J",' not in src


def test_enrichment_check_uses_the_constant():
    """The `-m $MFT` path-resolution branch must key off the same constant,
    or a journal can be admitted and then processed WITHOUT path resolution."""
    src = MODULE.read_text(encoding="utf-8")
    assert "in USN_JOURNAL_NAMES:" in src
    assert 'in ["$UsnJrnl%3A$J","$J","UsnJrnl-J"]:' not in src


def test_mft_still_accepted():
    """$MFT admission is untouched -- it is also the -m source for the
    journal, so losing it would silently disable path resolution."""
    src = MODULE.read_text(encoding="utf-8")
    assert '"$MFT",' in src
