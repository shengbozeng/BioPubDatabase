# -*- coding: utf-8 -*-
"""MD5 checksum verification helper."""
import hashlib
import os
import re


_MD5_RE = re.compile(r"\b[a-fA-F0-9]{32}\b")


def _read_expected_md5(md5_file_path):
    if not os.path.isfile(md5_file_path):
        raise FileNotFoundError(md5_file_path)

    with open(md5_file_path, "r", encoding="utf-8") as f:
        content = f.read()

    match = _MD5_RE.search(content)
    if not match:
        raise ValueError("No MD5 hash found in md5 file.")
    return match.group(0).lower()


def _compute_md5(file_path, chunk_size=1024 * 1024):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(file_path)

    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()


def verify_md5(file_path, md5_file_path):
    """
    Verify file MD5 against the hash in md5_file_path.
    Returns True if match, False otherwise.
    """
    expected = _read_expected_md5(md5_file_path)
    actual = _compute_md5(file_path)
    return actual == expected
