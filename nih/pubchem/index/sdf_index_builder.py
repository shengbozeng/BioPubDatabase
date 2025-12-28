# -*- coding: utf-8 -*-
''' ************************************************************ 
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 20:58:50
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 21:04:51
### FilePath: //pubchem//nih//pubchem//index//sdf_index_builder.py
### Description: 
### 
### Copyright (c) 2025 by AI Lingues, All Rights Reserved. 
********************************************************** '''
from __future__ import annotations

import re
import time
import lmdb
from pathlib import Path
from typing import Optional, Dict, List, Union
from rich.progress import track
from nih.pubchem.index.record_locator import RecordLocator
from nih.pubchem.index.sdf_index import SDFIndex
from nih.pubchem.index.utils import (
    _determine_kind,
    _is_int_ascii,
    _iter_sdf_files,
    _make_alid,
    _norm_field_name,
    _uuid_to_keyprefix,
)


# -----------------------------
# Config & constants
# -----------------------------

DEFAULT_COMPOUND_PATTERNS = [
    r"compound",
    r"cmpd",
    r"compounds",
]
DEFAULT_CONFORMER_PATTERNS = [
    r"conformer",
    r"conf",
    r"conformers",
]

# Common field candidates inside SDF property blocks:
# > <FIELDNAME>
# value lines until blank line.
CID_FIELD_CANDIDATES = [
    "CID",
    "PUBCHEM_COMPOUND_CID",
    "PUBCHEM_CID",
    "COMPOUND_CID",
]
CONFORMER_ID_FIELD_CANDIDATES = [
    "CONFORMER_ID",
    "CONFID",
    "PUBCHEM_CONFORMER_ID",
    "CONFORMERID",
]
# Parent compound id field inside conformer records (best-effort)
PARENT_CID_FIELD_CANDIDATES = [
    "CID",
    "PUBCHEM_COMPOUND_CID",
    "PUBCHEM_CID",
    "COMPOUND_CID",
    "PARENT_CID",
]


# Posting list page size: number of ALIDs per page
PL_PAGE_SIZE = 4096
# -----------------------------
# Builder
# -----------------------------


class SDFIndexBuilder:
    """
    Build index for a directory of SDF files.

    You can tune:
    - filename patterns to classify compound vs conformer
    - field candidates for CID / conformer_id / parent CID
    - LMDB map_size
    """

    def __init__(
        self,
        root_dir: Union[str, Path],
        index_dir: Union[str, Path],
        map_size: int = 1 << 40,  # 1 TB default
        compound_name_patterns: Optional[List[str]] = None,
        conformer_name_patterns: Optional[List[str]] = None,
        cid_fields: Optional[List[str]] = None,
        conformer_id_fields: Optional[List[str]] = None,
        parent_cid_fields: Optional[List[str]] = None,
        verbose: bool = True,
    ):
        self.root_dir = Path(root_dir).resolve()
        self.index_dir = Path(index_dir).resolve()
        self.map_size = map_size
        self.verbose = verbose

        self.compound_patterns = [
            re.compile(p, re.I)
            for p in (compound_name_patterns or DEFAULT_COMPOUND_PATTERNS)
        ]
        self.conformer_patterns = [
            re.compile(p, re.I)
            for p in (conformer_name_patterns or DEFAULT_CONFORMER_PATTERNS)
        ]

        self.cid_fields = [
            _norm_field_name(x) for x in (cid_fields or CID_FIELD_CANDIDATES)
        ]
        self.confid_fields = [
            _norm_field_name(x)
            for x in (conformer_id_fields or CONFORMER_ID_FIELD_CANDIDATES)
        ]
        self.parent_cid_fields = [
            _norm_field_name(x)
            for x in (parent_cid_fields or PARENT_CID_FIELD_CANDIDATES)
        ]

        self.index_dir.mkdir(parents=True, exist_ok=True)

        # Open writable index
        self.idx = SDFIndex(self.index_dir, readonly=False, map_size=self.map_size)

    def build(self) -> Dict:
        """
        Full rebuild (safe and deterministic for ALID generation).
        If you need incremental updates, we can extend with per-file manifests.
        """
        # Write meta
        meta = {
            "schema_version": SDFIndex.SCHEMA_VERSION,
            "root_dir": str(self.root_dir),
            "built_at": int(time.time()),
            "pl_page_size": PL_PAGE_SIZE,
        }
        self.idx._set_meta(meta)

        sdf_files = sorted(_iter_sdf_files(self.root_dir))
        it = (
            track(sdf_files, description="Indexing SDF")
            if self.verbose
            else sdf_files
        )

        total_files = 0
        total_records = 0
        total_compounds = 0
        total_conformers = 0

        for fp in it:
            relpath = str(fp.relative_to(self.root_dir)).replace("\\", "/")
            kind = _determine_kind(fp, self.compound_patterns, self.conformer_patterns)
            stats = self._index_one_file(fp, relpath, kind)
            total_files += 1
            total_records += stats["records"]
            total_compounds += stats["compounds"]
            total_conformers += stats["conformers"]

        # Update meta with stats
        meta2 = self.idx.get_meta()
        meta2.update(
            {
                "total_files": total_files,
                "total_records": total_records,
                "total_compound_records": total_compounds,
                "total_conformer_records": total_conformers,
            }
        )
        self.idx._set_meta(meta2)

        return meta2

    def _index_one_file(self, fp: Path, relpath: str, kind: str) -> Dict[str, int]:
        """
        Stream scan SDF file in binary mode; parse record boundaries and needed fields.
        """
        records = 0
        compounds = 0
        conformers = 0

        # We'll commit per file in a single write txn for performance.
        # Note: if a file is extremely huge, you can commit per N records; this is already robust for GB.
        with self.idx.env.begin(write=True) as txn:
            file_id = self.idx._get_or_create_file_id(txn, relpath)

            with fp.open("rb") as f:
                # record state
                rec_start = f.tell()
                rec_no = 0

                title_line: Optional[bytes] = None
                in_prop = False
                cur_field: Optional[str] = None
                cur_val_lines: List[bytes] = []

                # extracted per record
                cid_val: Optional[int] = None
                conf_id_val: Optional[str] = None
                parent_cid_val: Optional[int] = None

                def finalize_field():
                    nonlocal cur_field, cur_val_lines, cid_val, conf_id_val, parent_cid_val
                    if not cur_field:
                        return
                    # join as SDF property value: take first non-empty line (common convention)
                    v = None
                    for line in cur_val_lines:
                        s = line.strip()
                        if s:
                            v = s
                            break

                    if v is not None:
                        fn = _norm_field_name(cur_field)
                        if fn in self.cid_fields and cid_val is None:
                            if _is_int_ascii(v):
                                cid_val = int(v.decode("ascii"))
                        if fn in self.parent_cid_fields and parent_cid_val is None:
                            if _is_int_ascii(v):
                                parent_cid_val = int(v.decode("ascii"))
                        if fn in self.confid_fields and conf_id_val is None:
                            # conformer_id may be numeric or string; store as string
                            conf_id_val = v.decode("utf-8", errors="replace")

                    cur_field = None
                    cur_val_lines = []

                while True:
                    line = f.readline()
                    if not line:
                        # EOF: if partial record exists without $$$$, ignore or handle
                        break

                    # First line of record (title)
                    if title_line is None:
                        title_line = line.rstrip(b"\r\n")
                        # For compound files, title line often is CID
                        if kind == "compound":
                            t = title_line.strip()
                            if _is_int_ascii(t):
                                cid_val = int(t.decode("ascii"))

                    # Property header line: > <FIELDNAME>
                    if line.startswith(b"> <") and line.rstrip().endswith(b">"):
                        finalize_field()
                        # Extract between "<" and ">"
                        try:
                            # line like: b"> <PUBCHEM_COMPOUND_CID>\n"
                            m = re.search(rb"> <([^>]+)>", line)
                            if m:
                                cur_field = m.group(1).decode("utf-8", errors="replace")
                                cur_val_lines = []
                                in_prop = True
                            else:
                                cur_field = None
                                in_prop = False
                        except Exception:
                            cur_field = None
                            in_prop = False
                        continue

                    # Property value ends at blank line (SDF convention)
                    if in_prop:
                        if line.strip() == b"":
                            finalize_field()
                            in_prop = False
                        else:
                            cur_val_lines.append(line)
                        # continue reading
                        pass

                    # Record terminator
                    if line.strip() == b"$$$$":
                        finalize_field()
                        rec_end = f.tell()
                        # Determine primary_id for ALID generation
                        if kind == "compound":
                            primary = str(cid_val) if cid_val is not None else ""
                            alid = _make_alid("compound", relpath, rec_no, primary)
                            is_conf = False
                            eff_cid = cid_val
                        else:
                            primary = conf_id_val or ""
                            alid = _make_alid("conformer", relpath, rec_no, primary)
                            is_conf = True
                            # Try to set CID for conformer from either cid_val or parent_cid_val
                            eff_cid = cid_val if cid_val is not None else parent_cid_val

                        # Store record locator
                        rec_key = _uuid_to_keyprefix(is_conf) + alid.bytes
                        loc = RecordLocator(
                            file_id=file_id,
                            start=rec_start,
                            end=rec_end,
                            is_conformer=is_conf,
                            cid=eff_cid,
                        )
                        txn.put(rec_key, loc.to_bytes(), db=self.idx.db_records)

                        # Secondary indexes
                        if not is_conf:
                            # CID -> compound (unique)
                            if cid_val is not None:
                                txn.put(
                                    str(int(cid_val)).encode("ascii"),
                                    rec_key,
                                    db=self.idx.db_cid_to_compound,
                                )
                            compounds += 1
                        else:
                            # conformer_id -> conformer (unique)
                            if conf_id_val is not None:
                                txn.put(
                                    conf_id_val.encode("utf-8"),
                                    rec_key,
                                    db=self.idx.db_confid_to_conf,
                                )
                            # CID -> conformers posting list (1..N)
                            if eff_cid is not None:
                                self._pl_append(txn, eff_cid, alid.bytes)
                            conformers += 1

                        records += 1
                        rec_no += 1

                        # reset record state
                        rec_start = f.tell()
                        title_line = None
                        in_prop = False
                        cur_field = None
                        cur_val_lines = []
                        cid_val = None
                        conf_id_val = None
                        parent_cid_val = None

        return {"records": records, "compounds": compounds, "conformers": conformers}

    def _pl_append(self, txn: lmdb.Transaction, cid: int, uuid16: bytes) -> None:
        """
        Append uuid16 to CID->conformer posting list (paged).
        """
        cid_k = str(int(cid)).encode("ascii")
        h = txn.get(cid_k, db=self.idx.db_cid2conf_h)
        page_count = int.from_bytes(h, "little") if h else 0

        if page_count == 0:
            # create first page
            page_no = 0
            pk = cid_k + b"|" + b"0"
            txn.put(pk, uuid16, db=self.idx.db_cid2conf_p)
            txn.put(cid_k, (1).to_bytes(4, "little"), db=self.idx.db_cid2conf_h)
            return

        # append to last page if space
        last_page_no = page_count - 1
        pk = cid_k + b"|" + str(last_page_no).encode("ascii")
        blob = txn.get(pk, db=self.idx.db_cid2conf_p) or b""
        n = len(blob) // 16

        if n < PL_PAGE_SIZE:
            txn.put(pk, blob + uuid16, db=self.idx.db_cid2conf_p)
            return

        # create new page
        new_page_no = page_count
        pk2 = cid_k + b"|" + str(new_page_no).encode("ascii")
        txn.put(pk2, uuid16, db=self.idx.db_cid2conf_p)
        txn.put(
            cid_k, (page_count + 1).to_bytes(4, "little"), db=self.idx.db_cid2conf_h
        )
