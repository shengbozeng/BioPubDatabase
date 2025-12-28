# -*- coding: utf-8 -*-
''' ************************************************************ 
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 16:06:59
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 19:31:26
### FilePath: //pubchem//nih//pubchem//download//main.py
### Description: 
### 
### Copyright (c) 2025 by AI Lingues, All Rights Reserved. 
********************************************************** '''
import enum
import os
from pathlib import Path
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List

# 引入 rich 库用于进度显示
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
)


from nih.pubchem.download import fetch_html_source, parse_html_content,download_worker
from nih.pubchem.types import FileNode
from nih.pubchem.utils import verify_md5


# --- 配置区域 ---
MAX_WORKERS = 8  # 最大并行进程数
ERROR_LOG_FILE = "download_errors.txt"  # 错误记录文件


# 5. [新增] 主控制逻辑
def main():
    target_url = (
        "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound_3D/01_conf_per_cmpd/SDF/"
        # "https://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/"
    )
    # save_path = "/data/pubchem_origin_data/compound_current-full_sdf"
    save_path = "/data/pubchem_origin_data/compound_3d_01_conf_per_cmpd_sdf"

    # 1. 获取并解析列表
    print("[*] Fetching file list...")
    html_source = fetch_html_source(target_url)
    if not html_source:
        print("[!] Failed to fetch HTML source.")
        return

    total_file_list = parse_html_content(html_source)
    total_files: int = len(total_file_list)
    print(f"[*] Found {total_files} files on site.")

    target_dir = "/data/pubchem_origin_data/compound_3d_01_conf_per_cmpd_sdf"
    # 找到哪些文件已经下载过，排除这些文件
    downloaded_files = os.listdir(target_dir)

    should_file_list = []
    for i, node in enumerate(total_file_list):
        if str(node.file_name.split(".")[0]) in downloaded_files:
            continue
        should_file_list.append(node)

    total_should_download_files = len(should_file_list)

    print(f"[*] Found {total_should_download_files} files ready to download.")
    
    # 准备记录失败的任务
    failed_tasks = []

    # 2. 设置 Rich 进度条布局
    progress_columns = [
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("({task.completed}/{task.total})"),
        TimeRemainingColumn(),
    ]

    # 3. 开始多进程下载
    # 使用 'spawn' 或 'fork' 取决于系统，ProcessPoolExecutor 会自动处理

    download_success_files:List[(FileNode,str)] = []
    with Progress(*progress_columns) as progress:
        task_id = progress.add_task(
            "[green]Downloading...", total=total_should_download_files
        )

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # 提交所有任务
            # future_to_file 映射用于追踪哪个 future 对应哪个文件（如果需要的话，这里主要靠返回值）
            futures = [
                executor.submit(download_worker, node, target_url, save_path,'500K')
                for node in should_file_list
            ]

            # as_completed 会在任意一个子进程结束时 yield 结果
            for future in as_completed(futures):
                success, msg,url, fnode = future.result()
                download_success_files.append((fnode,url))
                # 更新进度条 (前进 1)ll
                progress.update(task_id, advance=1)

                # 收集失败任务
                if not success:
                    print(f"failed:\t{fnode.file_name} | {url} | Reason: {msg}")
                    failed_tasks.append(f"{fnode.file_name} | {url} | Reason: {msg}")
    
    # 4. 处理失败记录
    print("\n" + "=" * 40)
    if failed_tasks:
        print(f"[!] {len(failed_tasks)} files failed to download.")
        print(f"[*] Writing error log to {ERROR_LOG_FILE}...")

        with open(ERROR_LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"Failed Downloads Report - {time.ctime()}\n")
            f.write("=" * 50 + "\n")
            for line in failed_tasks:
                f.write(line + "\n")
        print(f"[*] Error log saved.")
    else:
        print("[*] All files downloaded successfully! No errors.")
    print("=" * 40)

    # 5. 校验成功下载文件的md5,校验失败的文件名写入错误记录
    failed_verify_files:List[str]=[]
    print("\n" + "=" * 40)
    print(f'[*] Starting verify {len(download_success_files)} downloaded files...')
    for i, (fnode,url) in enumerate(download_success_files):            
        gz_file = Path(target_dir) / fnode.file_name
        md5_file =Path(f"{str(gz_file)}.md5")
        # 如果md文件不存在则不进行校验
        if not md5_file.exists():
            continue
        result=verify_md5(gz_file,md5_file)
        if not result:
            failed_verify_files.append(fnode.file_name)
        print(f"{i}:\t{result}{'✅' if result else '❌'}:\t {gz_file.name}")

    # 校验失败的文件名写入错误记录
    if len(failed_verify_files)>0:
        with open('./failed_verify_files.txt','w',encoding='utf-8') as f:
            f.write('\n'.join(failed_verify_files))

    print(f'[*] Verify success files {len(download_success_files)-len(failed_verify_files)}, faild {len(failed_verify_files)}, finished.')




if __name__ == "__main__":
    main()
