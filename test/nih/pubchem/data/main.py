# -*- coding: utf-8 -*-
"""************************************************************
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 20:40:59
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 21:04:06
### FilePath: //pubchem//nih//pubchem//index//main.py
### Description:
###
### Copyright (c) 2025 by AI Lingues, All Rights Reserved.
**********************************************************"""
# sdf_index.py
# Production-ready SDF offset index for huge files (GB-scale) with LMDB.
# Features:
# - Index all .sdf files under a directory
# - Distinguish compound vs conformer by filename patterns (configurable)
# - Build byte-offset locator per record: file_id + start/end offsets
# - Generate deterministic ALID (UUIDv5) stored in index
# - Fast lookup:
#     CID -> compound record (0/1)
#     conformer_id -> conformer record (0/1)
#     CID -> conformer records (0..N) via paged posting lists
#     ALID -> record
# - Batch lookup for tens/hundreds of thousands keys via chunked streaming
#
# Install:
#   pip install lmdb
# Optional:
#   pip install tqdm

from __future__ import annotations

from argparse import Namespace
import json
import os
from os import write
from pathlib import Path
from queue import Queue
import threading
from typing import Dict, List, Union
from typing_extensions import Writer

from rdkit import Chem
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from nih.pubchem.index.sdf_index import SDFIndex
from nih.pubchem.index.sdf_index_builder import SDFIndexBuilder
from pybiotech.loaders.sdf_loader import SDFLoader
from rdkit.Chem import SDWriter, Mol

from utils.compound_conf_merge import merge_compound_to_conformer

# -----------------------------
# CLI helpers (optional)
# -----------------------------

DEFAULT_IO_WORKERS = min(32, max(4, (os.cpu_count() or 1) * 2))


def _fetch_single_compound(
    index_dir: Union[str, Path], cid: int, root_dir: str
) -> tuple[int, str | None]:
    local_idx = SDFIndex(index_dir, readonly=True)
    try:
        hit = local_idx.get_compound_by_cid(cid)
        if not hit:
            return cid, None
        seg = local_idx.read_segment(root_dir, hit.locator)
        return cid, seg.decode("utf-8", errors="replace")
    finally:
        local_idx.env.close()


def _compound_worker(
    index_dir: Union[str, Path],
    root_dir: str,
    cid_queue: Queue,
    result_queue: Queue,
) -> None:
    local_idx = SDFIndex(index_dir, readonly=True)
    try:
        while True:
            cid = cid_queue.get()
            if cid is None:
                cid_queue.task_done()
                break

            try:
                hit = local_idx.get_compound_by_cid(cid)
                if not hit:
                    result_queue.put((cid, None))
                else:
                    seg = local_idx.read_segment(root_dir, hit.locator)
                    result_queue.put((cid, seg.decode("utf-8", errors="replace")))
            finally:
                cid_queue.task_done()
    finally:
        local_idx.env.close()


def _list_single_conformer(
    index_dir: Union[str, Path], cid: int, root_dir: str, limit: int
) -> tuple[int, str | None, int]:
    local_idx = SDFIndex(index_dir, readonly=True)
    try:
        conformer_data: str | None = None
        count = 0
        for hit in local_idx.iter_conformers_by_cid(cid):
            seg = local_idx.read_segment(root_dir, hit.locator)
            conformer_data = seg.decode("utf-8", errors="replace")
            count += 1
            if count >= limit:
                break
        return cid, conformer_data, count
    finally:
        local_idx.env.close()


def _conformer_worker(
    index_dir: Union[str, Path],
    root_dir: str,
    limit: int,
    cid_queue: Queue,
    result_queue: Queue,
) -> None:
    local_idx = SDFIndex(index_dir, readonly=True)
    try:
        while True:
            cid = cid_queue.get()
            if cid is None:
                cid_queue.task_done()
                break

            try:
                conformer_data: str | None = None
                count = 0
                for hit in local_idx.iter_conformers_by_cid(cid):
                    seg = local_idx.read_segment(root_dir, hit.locator)
                    conformer_data = seg.decode("utf-8", errors="replace")
                    count += 1
                    if count >= limit:
                        break
                result_queue.put((cid, conformer_data, count))
            finally:
                cid_queue.task_done()
    finally:
        local_idx.env.close()


def build_index(
    root_dir: Union[str, Path],
    index_dir: Union[str, Path],
    map_size: int = 1 << 40,
    verbose: bool = True,
) -> Dict:
    builder = SDFIndexBuilder(
        root_dir=root_dir,
        index_dir=index_dir,
        map_size=map_size,
        verbose=verbose,
    )
    return builder.build()


def get_compound(idx: SDFIndex, cid: int, root_dir: str):
    hit = idx.get_compound_by_cid(cid=cid)
    if not hit:
        print("NOT FOUND")
    else:
        seg = idx.read_segment(root_dir, hit.locator)
        print(f"ALID={hit.alid}")
        print(
            f"file_id={hit.locator.file_id} start={hit.locator.start} end={hit.locator.end} cid={hit.locator.cid}"
        )
        print(seg.decode("utf-8", errors="replace"))


def batch_get_compounds(
    idx: SDFIndex,
    cids: list[int],
    root_dir: str,
    max_workers: int = DEFAULT_IO_WORKERS,
) -> Dict[int, str] | None:
    if not cids:
        return None

    found = 0
    compounds_data: Dict[int, str] = {}
    worker_count = min(max_workers, len(cids))
    cid_queue: Queue = Queue()
    result_queue: Queue = Queue()
    workers: list[threading.Thread] = []

    for cid in cids:
        cid_queue.put(cid)
    for _ in range(worker_count):
        cid_queue.put(None)

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_id = progress.add_task("Fetching compounds", total=len(cids))
        for _ in range(worker_count):
            worker = threading.Thread(
                target=_compound_worker,
                args=(idx.index_dir, root_dir, cid_queue, result_queue),
                daemon=True,
            )
            worker.start()
            workers.append(worker)

        processed = 0
        while processed < len(cids):
            cid, compound_raw = result_queue.get()
            if compound_raw is not None:
                compounds_data[cid] = compound_raw
                found += 1
            processed += 1
            progress.advance(task_id)

    cid_queue.join()
    for worker in workers:
        worker.join()

    if found == 0:
        print("NOT FOUND")
    return compounds_data


def get_conformer(idx: SDFIndex, conformer_id: str, root_dir: str):
    hit = idx.get_conformer_by_conformer_id(conformer_id)
    if not hit:
        print("NOT FOUND")
    else:
        seg = idx.read_segment(root_dir, hit.locator)
        print(f"ALID={hit.alid}")
        print(
            f"file_id={hit.locator.file_id} start={hit.locator.start} end={hit.locator.end} cid={hit.locator.cid}"
        )
        print(seg.decode("utf-8", errors="replace"))


def batch_get_conformers(idx: SDFIndex, confids: list[str], root_dir: str):
    found = 0
    for confid, hit in idx.batch_get_conformers_by_conformer_id(confids):
        if not hit:
            print(f"Conformer ID={confid} NOT FOUND")
            continue

        seg = idx.read_segment(root_dir, hit.locator)
        print(f"Conformer ID={confid} ALID={hit.alid}")
        print(
            f"file_id={hit.locator.file_id} start={hit.locator.start} end={hit.locator.end} cid={hit.locator.cid}"
        )
        print(seg.decode("utf-8", errors="replace"))
        found += 1

    if found == 0:
        print("NOT FOUND")


def list_conformers(
    idx: SDFIndex,
    cids: list[int],
    root_dir: str,
    limit: int = 10,
    max_workers: int = DEFAULT_IO_WORKERS,
) -> Dict[int, str] | None:
    if not cids:
        return None

    conformers_data: Dict[int, str] = {}
    counts_by_cid: Dict[int, int] = {}
    worker_count = min(max_workers, len(cids))
    cid_queue: Queue = Queue()
    result_queue: Queue = Queue()
    workers: list[threading.Thread] = []

    for cid in cids:
        cid_queue.put(cid)
    for _ in range(worker_count):
        cid_queue.put(None)

    with Progress(
        TextColumn("[bold green]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_id = progress.add_task("Listing conformers", total=len(cids))
        for _ in range(worker_count):
            worker = threading.Thread(
                target=_conformer_worker,
                args=(idx.index_dir, root_dir, limit, cid_queue, result_queue),
                daemon=True,
            )
            worker.start()
            workers.append(worker)

        processed = 0
        while processed < len(cids):
            cid, conformer_raw, count = result_queue.get()
            counts_by_cid[cid] = count
            if conformer_raw is not None:
                conformers_data[cid] = conformer_raw
            processed += 1
            progress.advance(task_id)

    cid_queue.join()
    for worker in workers:
        worker.join()

    for cid in cids:
        print(f"cid:\t{cid} shown {counts_by_cid.get(cid, 0)}")
    return conformers_data if conformers_data else None


def batch_query_data(
    comcound_idx: SDFIndex,
    conformer_idx: SDFIndex,
    cids: list[int],
    compound_root_dir: str,
    conformer_root_dir: str,
    conformer_limit: int = 10,
):
    # Example: batch query compounds and conformers for CID range
    compounds_data: Dict[int, str] | None = batch_get_compounds(
        comcound_idx, cids, compound_root_dir
    )
    conformers_data: Dict[int, str] | None = list_conformers(
        conformer_idx, cids, conformer_root_dir, limit=conformer_limit
    )

    if compounds_data is None:
        print("No compounds found.")
        return

    merged_mols: List[Mol] = []
    with Progress(
        TextColumn("[bold magenta]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task_id = progress.add_task(
            "Merging compound/conformer data", total=len(compounds_data)
        )
        for key, compound_raw in compounds_data.items():
            if compound_raw is None:
                progress.advance(task_id)
                continue

            print(f"Compound CID={key} \n")
            compound_mols, _, _ = SDFLoader.readDataFromText(compound_raw)
            compound_mol = compound_mols[0] if compound_mols else None
            if not compound_mol:
                print(f"No valid compound found for CID={key}.")
                progress.advance(task_id)
                continue
            conf_raw = (
                conformers_data[key]
                if conformers_data and key in conformers_data
                else None
            )
            if conf_raw:
                conformer_mols, _, _ = SDFLoader.readDataFromText(conf_raw)
                conformer_mol = conformer_mols[0] if conformer_mols else None
                if conformer_mol:
                    merged_mol = merge_compound_to_conformer(
                        compound_mol, conformer_mol
                    )
                    merged_mols.append(merged_mol)
                    # print(f"Merged Mol for CID={key}:\n{Chem.MolToMolBlock(merged_mol)}")
                else:
                    print(
                        f"No valid conformer found for CID={key}, using compound only."
                    )
            else:
                merged_mols.append(compound_mol)
            progress.advance(task_id)
    return merged_mols


# -----------------------------
# Example usage
# -----------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build and query SDF offset index (LMDB)."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_build = sub.add_parser("build", help="Build index for a directory.")
    p_build.add_argument(
        "--root", required=True, help="Root directory containing .sdf files."
    )
    p_build.add_argument(
        "--index", required=True, help="Index directory (LMDB environment)."
    )
    p_build.add_argument(
        "--map-size",
        type=int,
        default=(1 << 40),
        help="LMDB map size in bytes (default 1TB).",
    )
    p_build.add_argument(
        "--quiet", action="store_true", help="Disable progress output."
    )

    p_q1 = sub.add_parser("get-compound", help="Get compound record by CID.")
    p_q1.add_argument("--root", required=True)
    p_q1.add_argument("--index", required=True)
    p_q1.add_argument("--cid", type=int, required=True)

    p_q1b = sub.add_parser(
        "batch-get-compound", help="Batch get compound records by CIDs."
    )
    p_q1b.add_argument("--root", required=True)
    p_q1b.add_argument("--index", required=True)
    p_q1b.add_argument("--cid_start", type=int, required=True)
    p_q1b.add_argument("--cid_end", type=int, required=True)

    p_q2 = sub.add_parser("get-conformer", help="Get conformer record by conformer_id.")
    p_q2.add_argument("--root", required=True)
    p_q2.add_argument("--index", required=True)
    p_q2.add_argument("--confid", required=True)

    p_q3 = sub.add_parser(
        "list-conformers", help="List conformer records by CID (stream)."
    )
    p_q3.add_argument("--root", required=True)
    p_q3.add_argument("--index", required=True)
    p_q3.add_argument("--cid_start", type=int, required=True)
    p_q3.add_argument("--cid_end", type=int, required=True)
    p_q3.add_argument("--limit", type=int, default=10)

    p_q9 = sub.add_parser(
        "batch-query-data", help="List compound and conformer records by CID (stream)."
    )
    p_q9.add_argument("--compound_root", required=True)
    p_q9.add_argument("--compound_index", required=True)
    p_q9.add_argument("--conformer_root", required=True)
    p_q9.add_argument("--conformer_index", required=True)
    p_q9.add_argument("--cid_start", type=int, required=True)
    p_q9.add_argument("--cid_end", type=int, required=True)
    p_q9.add_argument("--conformer_limit", type=int, default=10)

    args: Namespace = parser.parse_args()

    match args.cmd:
        case "build":
            meta = build_index(
                args.root, args.index, map_size=args.map_size, verbose=not args.quiet
            )
            print(json.dumps(meta, ensure_ascii=False, indent=2))

        case "get-compound":
            idx = SDFIndex(args.index, readonly=True)
            get_compound(idx, args.cid, args.root)

        case "batch-get-compound":
            idx = SDFIndex(args.index, readonly=True)
            cids = list(range(args.cid_start, args.cid_end + 1))
            batch_get_compounds(idx, cids, args.root)

        case "get-conformer":
            idx = SDFIndex(args.index, readonly=True)
            get_conformer(idx, args.confid, args.root)

        case "batch-get-conformer":
            idx = SDFIndex(args.index, readonly=True)
            confids = [f"{args.cid}_{i}" for i in range(1, 11)]  # example confids
            batch_get_conformers(idx, confids, args.root)

        case "list-conformers":
            idx = SDFIndex(args.index, readonly=True)
            cids = list(range(args.cid_start, args.cid_end + 1))
            list_conformers(idx, cids, args.root, limit=args.limit)

        case "batch-query-data":
            # Example: batch query compounds and conformers for CID range
            comcound_idx = SDFIndex(args.compound_index, readonly=True)
            conformer_idx = SDFIndex(args.conformer_index, readonly=True)

            cids = list(range(args.cid_start, args.cid_end + 1))
            mols: None | List[Mol] = batch_query_data(
                comcound_idx,
                conformer_idx,
                cids,
                args.compound_root,
                args.conformer_root,
                conformer_limit=args.conformer_limit,
            )

            if mols:
                sdf_output_doc = Path(
                    f"{args.cid_start}_{args.cid_end}_merged_mols.sdf"
                )
                writer = Chem.SDWriter(str(sdf_output_doc))
                for mol in mols:
                    writer.write(mol=mol)
                writer.flush()
                writer.close()
                print(f"{len(mols)} Merged molecules written to {sdf_output_doc}")
            else:
                print("No data found for the given CID range.")

        case _:
            print(f"Unknown command: {args.cmd}")
