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
#   pip install rich

from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path
from typing import Dict, List, Union
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from nih.pubchem.index.sdf_index import SDFIndex
from nih.pubchem.index.sdf_index_builder import SDFIndexBuilder
from pybiotech.loaders.sdf_loader import SDFLoader
from rdkit.Chem import SDWriter, Mol
from  testcase.nih.pubchem.data.conformer import batch_get_conformers, get_conformer, list_conformers

from testcase.nih.pubchem.data.compound import batch_get_compounds, get_compound
from utils.compound_conf_merge import merge_compound_to_conformer

# -----------------------------
# CLI helpers (optional)
# -----------------------------



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



def batch_query_data(
    comcound_idx: SDFIndex,
    conformer_idx: SDFIndex,
    cids: list[int],
    compound_root_dir: str,
    conformer_root_dir: str,
    conformer_limit: int = 10,
) -> None | List[Mol]:
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
        task_id: TaskID = progress.add_task(
            "Merging compound/conformer data", total=len(compounds_data)
        )
        for key, compound_raw in compounds_data.items():
            if compound_raw is None:
                progress.advance(task_id)
                continue

            # print(f"Compound CID={key} \n")
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
                    f"0{args.cid_start}_0{args.cid_end}_merged_mols.sdf"
                )
                writer = SDWriter(str(sdf_output_doc))
                for mol in mols:
                    writer.write(mol=mol)
                writer.flush()
                writer.close()
                print(f"{len(mols)} Merged molecules written to {sdf_output_doc}")
            else:
                print("No data found for the given CID range.")

        case _:
            print(f"Unknown command: {args.cmd}")
