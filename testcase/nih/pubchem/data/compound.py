from __future__ import annotations
import os
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

DEFAULT_IO_WORKERS = min(32, max(4, (os.cpu_count() or 1) * 2))

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
