# -*- coding: utf-8 -*-
"""************************************************************
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 20:52:19
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 21:05:25
### FilePath: //pubchem//nih//pubchem//index//sdf_index.py
### Description:
###
### Copyright (c) 2025 by AI Lingues, All Rights Reserved.
**********************************************************"""
from __future__ import annotations

import json
import uuid
import lmdb
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional, Dict, Tuple, Union
from nih.pubchem.index.record_locator import RecordLocator
from nih.pubchem.index.utils_module import _chunked


@dataclass(frozen=True)
class IndexHit:
    """Resolved record including ALID."""

    alid: uuid.UUID
    locator: RecordLocator


# -----------------------------
# Core index class
# -----------------------------


class SDFIndex:
    """
    LMDB-backed index.

    Databases:
      - meta:              JSON metadata, schema version, root path, etc.
      - files:             file_id -> relative path (bytes)
      - files_rev:         relative path -> file_id
      - records:           key = b"C"+uuid16 or b"F"+uuid16 -> RecordLocator bytes
      - cid_to_compound:   key = cid (ascii bytes) -> record key (b"C"+uuid16)
      - confid_to_conf:    key = conformer_id (utf-8 bytes) -> record key (b"F"+uuid16)
      - cid_to_conformers_h:  header: key = cid -> page_count (uint32)
      - cid_to_conformers_p:  pages:  key = cid|page_no -> packed uuid16 list (len multiple of 16)
    """

    SCHEMA_VERSION = 1

    def __init__(
        self,
        index_dir: Union[str, Path],
        readonly: bool = True,
        map_size: int = 1 << 40,
    ):
        self.index_dir = Path(index_dir)
        self.readonly = readonly

        self.env = lmdb.open(
            str(self.index_dir),
            readonly=readonly,
            lock=not readonly,
            readahead=readonly,
            map_size=map_size,
            max_dbs=32,
            subdir=True,
            create=not readonly,
            metasync=not readonly,
            sync=not readonly,
        )

        self.db_meta = self.env.open_db(b"meta")
        self.db_files = self.env.open_db(b"files")
        self.db_files_rev = self.env.open_db(b"files_rev")
        self.db_records = self.env.open_db(b"records")
        self.db_cid_to_compound = self.env.open_db(b"cid_to_compound")
        self.db_confid_to_conf = self.env.open_db(b"confid_to_conf")
        self.db_cid2conf_h = self.env.open_db(b"cid_to_conformers_h")
        self.db_cid2conf_p = self.env.open_db(b"cid_to_conformers_p")

    # -------- metadata --------

    def get_meta(self) -> Dict:
        with self.env.begin(db=self.db_meta) as txn:
            b = txn.get(b"meta_json")
            if not b:
                return {}
            return json.loads(b.decode("utf-8"))

    def _set_meta(self, meta: Dict) -> None:
        if self.readonly:
            raise RuntimeError("Index is readonly")
        with self.env.begin(write=True, db=self.db_meta) as txn:
            txn.put(
                b"meta_json",
                json.dumps(meta, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            )

    # -------- file table --------

    def _get_or_create_file_id(self, txn: lmdb.Transaction, relpath: str) -> int:
        k = relpath.encode("utf-8")
        existing = txn.get(k, db=self.db_files_rev)
        if existing:
            return int.from_bytes(existing, "little", signed=False)

        # Create new file_id: use monotonic counter in meta db
        counter_b = txn.get(b"file_id_counter", db=self.db_meta)
        counter = int.from_bytes(counter_b, "little") if counter_b else 0
        file_id = counter + 1
        txn.put(b"file_id_counter", file_id.to_bytes(8, "little"), db=self.db_meta)
        txn.put(k, file_id.to_bytes(8, "little"), db=self.db_files_rev)
        txn.put(file_id.to_bytes(8, "little"), k, db=self.db_files)
        return file_id

    def resolve_file_path(self, file_id: int) -> Optional[str]:
        with self.env.begin(db=self.db_files) as txn:
            b = txn.get(int(file_id).to_bytes(8, "little"))
            return None if not b else b.decode("utf-8")

    # -------- record access --------

    def get_by_alid(
        self, alid: Union[str, uuid.UUID], is_conformer: Optional[bool] = None
    ) -> Optional[IndexHit]:
        """
        Fetch record by ALID.
        If is_conformer is None, tries compound then conformer.
        """
        if isinstance(alid, str):
            alid_u = uuid.UUID(alid)
        else:
            alid_u = alid

        key_c = b"C" + alid_u.bytes
        key_f = b"F" + alid_u.bytes

        with self.env.begin(db=self.db_records) as txn:
            if is_conformer is None:
                b = txn.get(key_c)
                if b:
                    return IndexHit(alid_u, RecordLocator.from_bytes(b))
                b = txn.get(key_f)
                if b:
                    return IndexHit(alid_u, RecordLocator.from_bytes(b))
                return None

            b = txn.get(key_f if is_conformer else key_c)
            if not b:
                return None
            return IndexHit(alid_u, RecordLocator.from_bytes(b))

    def get_compound_by_cid(self, cid: int) -> Optional[IndexHit]:
        k = str(int(cid)).encode("ascii")
        with self.env.begin() as txn:
            rec_key = txn.get(k, db=self.db_cid_to_compound)
            if not rec_key:
                return None
            b = txn.get(rec_key, db=self.db_records)
            if not b:
                return None
            alid = uuid.UUID(bytes=rec_key[1:17])
            return IndexHit(alid, RecordLocator.from_bytes(b))

    def get_conformer_by_conformer_id(self, conformer_id: str) -> Optional[IndexHit]:
        k = conformer_id.encode("utf-8")
        with self.env.begin() as txn:
            rec_key = txn.get(k, db=self.db_confid_to_conf)
            if not rec_key:
                return None
            b = txn.get(rec_key, db=self.db_records)
            if not b:
                return None
            alid = uuid.UUID(bytes=rec_key[1:17])
            return IndexHit(alid, RecordLocator.from_bytes(b))

    def iter_conformers_by_cid(self, cid: int) -> Iterator[IndexHit]:
        """
        Stream conformer records for a CID using posting list pages.
        Suitable for huge N (hundreds of thousands).
        """
        cid_k = str(int(cid)).encode("ascii")
        with self.env.begin() as txn:
            h = txn.get(cid_k, db=self.db_cid2conf_h)
            if not h:
                return
            page_count = int.from_bytes(h, "little", signed=False)
            for page_no in range(page_count):
                pk = cid_k + b"|" + str(page_no).encode("ascii")
                blob = txn.get(pk, db=self.db_cid2conf_p)
                if not blob:
                    continue
                # blob is concatenated uuid16 list
                for i in range(0, len(blob), 16):
                    ubytes = blob[i : i + 16]
                    if len(ubytes) < 16:
                        break
                    rec_key = b"F" + ubytes
                    rec_val = txn.get(rec_key, db=self.db_records)
                    if not rec_val:
                        continue
                    yield IndexHit(
                        uuid.UUID(bytes=ubytes), RecordLocator.from_bytes(rec_val)
                    )

    # -------- batch lookups (high throughput) --------

    def batch_get_compounds_by_cid(
        self,
        cids: Iterable[int],
        chunk_size: int = 50000,
    ) -> Iterator[Tuple[int, Optional[IndexHit]]]:
        """
        Stream results: (cid, hit_or_none)
        Designed for tens/hundreds of thousands keys.
        """
        with self.env.begin() as txn:
            for chunk in _chunked(cids, chunk_size):
                # local loop: per key approx constant cost
                for cid in chunk:
                    k = str(int(cid)).encode("ascii")
                    rec_key = txn.get(k, db=self.db_cid_to_compound)
                    if not rec_key:
                        yield int(cid), None
                        continue
                    rec_val = txn.get(rec_key, db=self.db_records)
                    if not rec_val:
                        yield int(cid), None
                        continue
                    alid = uuid.UUID(bytes=rec_key[1:17])
                    yield int(cid), IndexHit(alid, RecordLocator.from_bytes(rec_val))

    def batch_get_conformers_by_conformer_id(
        self,
        conformer_ids: Iterable[str],
        chunk_size: int = 50000,
    ) -> Iterator[Tuple[str, Optional[IndexHit]]]:
        with self.env.begin() as txn:
            for chunk in _chunked(conformer_ids, chunk_size):
                for confid in chunk:
                    k = confid.encode("utf-8")
                    rec_key = txn.get(k, db=self.db_confid_to_conf)
                    if not rec_key:
                        yield confid, None
                        continue
                    rec_val = txn.get(rec_key, db=self.db_records)
                    if not rec_val:
                        yield confid, None
                        continue
                    alid = uuid.UUID(bytes=rec_key[1:17])
                    yield confid, IndexHit(alid, RecordLocator.from_bytes(rec_val))

    # -------- read raw segment --------

    def read_segment(self, root_dir: Union[str, Path], locator: RecordLocator) -> bytes:
        """
        Read the raw SDF record text segment using locator offsets.
        """
        rel = self.resolve_file_path(locator.file_id)
        if not rel:
            raise KeyError(f"file_id={locator.file_id} not found in index")
        fp = Path(root_dir) / rel
        with fp.open("rb") as f:
            f.seek(locator.start)
            return f.read(locator.end - locator.start)
