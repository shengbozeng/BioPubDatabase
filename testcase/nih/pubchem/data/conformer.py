from __future__ import annotations
from pathlib import Path
from queue import Queue
import threading
from typing import Dict, Union
from nih.pubchem.index.sdf_index import SDFIndex

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from testcase.nih.pubchem.data.compound import DEFAULT_IO_WORKERS


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

    # for cid in cids:
    #     print(f"cid:\t{cid} shown {counts_by_cid.get(cid, 0)}")
    return conformers_data if conformers_data else None

