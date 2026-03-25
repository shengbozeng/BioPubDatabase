# -*- coding: utf-8 -*-
''' ************************************************************ 
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 20:40:59
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 21:04:06
### FilePath: //pubchem//nih//pubchem//index//main.py
### Description: 
### 
### Copyright (c) 2025 by AI Lingues, All Rights Reserved. 
********************************************************** '''
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
from pathlib import Path
from typing import Dict, Union
from nih.pubchem.index.sdf_index import SDFIndex
from nih.pubchem.index.sdf_index_builder import SDFIndexBuilder

# -----------------------------
# CLI helpers (optional)
# -----------------------------


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
    
    p_q1b = sub.add_parser("batch-get-compound", help="Batch get compound records by CIDs.")
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
    p_q3.add_argument("--cid", type=int, required=True)
    p_q3.add_argument("--limit", type=int, default=10)

    args: Namespace = parser.parse_args()

    match args.cmd:
        case "build":                
            meta = build_index(
                args.root, args.index, map_size=args.map_size, verbose=not args.quiet
            )
            print(json.dumps(meta, ensure_ascii=False, indent=2))

        case "get-compound":
            idx = SDFIndex(args.index, readonly=True)
            hit = idx.get_compound_by_cid(args.cid)
            if not hit:
                print("NOT FOUND")
            else:
                seg = idx.read_segment(args.root, hit.locator)
                print(f"ALID={hit.alid}")
                print(
                    f"file_id={hit.locator.file_id} start={hit.locator.start} end={hit.locator.end} cid={hit.locator.cid}"
                )
                print(seg.decode("utf-8", errors="replace"))

        case "batch-get-compound":
            idx = SDFIndex(args.index, readonly=True)
            cids=list(range(args.cid_start, args.cid_end+1))
            hit = idx.batch_get_compounds_by_cid(cids)
            if not hit:
                print("NOT FOUND")
            else:
                seg = idx.read_segment(args.root, hit.locator)
                print(f"ALID={hit.alid}")
                print(
                    f"file_id={hit.locator.file_id} start={hit.locator.start} end={hit.locator.end} cid={hit.locator.cid}"
                )
                print(seg.decode("utf-8", errors="replace"))

        case "get-conformer":
            idx = SDFIndex(args.index, readonly=True)
            hit = idx.get_conformer_by_conformer_id(args.confid)
            if not hit:
                print("NOT FOUND")
            else:
                seg = idx.read_segment(args.root, hit.locator)
                print(f"ALID={hit.alid}")
                print(
                    f"file_id={hit.locator.file_id} start={hit.locator.start} end={hit.locator.end} cid={hit.locator.cid}"
                )
                print(seg.decode("utf-8", errors="replace"))

        case "list-conformers":
            idx = SDFIndex(args.index, readonly=True)
            count = 0
            for hit in idx.iter_conformers_by_cid(args.cid):
                print(
                    f"[{count}] ALID={hit.alid} file_id={hit.locator.file_id} start={hit.locator.start} end={hit.locator.end} cid={hit.locator.cid}"
                )
                count += 1
                if count >= args.limit:
                    break
            print(f"shown {count}")

        case _:
            print(f"Unknown command: {args.cmd}")