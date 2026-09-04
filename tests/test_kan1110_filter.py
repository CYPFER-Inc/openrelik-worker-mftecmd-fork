"""KAN-1110: MFTECmd input filter uses signature-typed mimes, not the
octet-stream catch-all.

The server now content-signature-types $MFT/$Boot (create_file_in_db), so they
arrive with a real mime. MFTECmd must match those mimes -- otherwise a $MFT
whose mime is no longer octet-stream and whose filename was mangled is stranded.
And with a comprehensive filename list, MFTECmd no longer needs octet-stream at
all, so it stops having every unrelated binary routed to it.

AST-based like the other tests here: mftecmd.py imports celery / openrelik_common
at module load, absent in CI.
"""
import ast
from pathlib import Path

MODULE = Path(__file__).resolve().parent.parent / "src" / "mftecmd.py"
TREE = ast.parse(MODULE.read_text(encoding="utf-8"))


def _compatible_inputs():
    for node in ast.walk(TREE):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == "COMPATIBLE_INPUTS":
                    # USN_JOURNAL_NAMES is splatted in, so eval with it bound.
                    usn = ast.literal_eval(_assign("USN_JOURNAL_NAMES"))
                    return eval(  # noqa: S307 -- our own module source
                        compile(ast.Expression(node.value), "<ci>", "eval"),
                        {"USN_JOURNAL_NAMES": usn},
                    )
    raise AssertionError("COMPATIBLE_INPUTS not found")


def _assign(name):
    for node in ast.walk(TREE):
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    return node.value
    raise AssertionError(f"{name} not found")


def test_signature_typed_mimes_are_accepted():
    """The mimes the server's KAN-1110 typing emits for $MFT/$Boot."""
    mimes = _compatible_inputs()["mime_types"]
    assert "application/x-ntfs-mft" in mimes
    assert "application/x-ntfs-boot" in mimes


def test_octet_stream_catch_all_is_dropped():
    """The whole point: MFTECmd must stop matching the non-discriminating
    octet-stream / text-plain fallback."""
    mimes = _compatible_inputs()["mime_types"]
    assert "application/octet-stream" not in mimes
    assert "text/plain" not in mimes


def test_every_parsed_artefact_still_covered_after_dropping_octet_stream():
    """Dropping octet-stream must not strand anything MFTECmd parses: each is
    matched by filename (or, for $MFT/$Boot, also by the new mime)."""
    ci = _compatible_inputs()
    filenames = set(ci["filenames"])
    for artefact in ("$MFT", "$Boot", "$LogFile", "$I30", "INDX",
                     "$Secure_$SDS", "$Secure%3A$SDS", ".openrelik-config"):
        assert artefact in filenames, f"{artefact} lost its routing key"
    # the USN journal names survive via the shared constant
    for usn in ("$UsnJrnl$J", "$J"):
        assert usn in filenames


def test_mft_matches_by_both_filename_and_signature_mime():
    """Belt-and-braces for the artefact that matters most: a $MFT reaches
    MFTECmd whether it kept its name or was typed by signature."""
    ci = _compatible_inputs()
    assert "$MFT" in ci["filenames"]
    assert "application/x-ntfs-mft" in ci["mime_types"]
