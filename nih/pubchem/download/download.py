# -*- coding: utf-8 -*-
"""************************************************************
### Author: Zeng Shengbo shengbo.zeng@ailingues.com
### Date: 12/27/2025 16:07:55
### LastEditors: Zeng Shengbo shengbo.zeng@ailingues.com
### LastEditTime: 12/27/2025 19:31:33
### FilePath: //pubchem//nih//pubchem//download//download.py
### Description:
###
### Copyright (c) 2025 by AI Lingues, All Rights Reserved.
**********************************************************"""
import re
import os
import time
import subprocess
from typing import List
import urllib.request

from nih.pubchem.types import FileNode


MAX_RETRIES = 2  # 最大重试次数 (不含首次)
RETRY_DELAY = 5  # 重试间隔 (秒)


# 2. 获取 HTML 源码 (保持不变)
def fetch_html_source(target_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        req = urllib.request.Request(target_url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as response:
            return response.read().decode("utf-8")
    except Exception as e:
        print(f"Error fetching source: {e}")
        return None


# 3. 解析函数 (保持不变)
def parse_html_content(html_text) -> List[FileNode]:
    results = []
    pattern = re.compile(
        r'<a href="([^"]+)">([^<]+)</a>\s+(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+([0-9\.]+[KMGTP]?)'
    )
    if not html_text:
        return results

    for line in html_text.split("\n"):
        if "Parent Directory" in line:
            continue
        match = pattern.search(line)
        if match:
            href, name, last_mod, size = match.groups()
            results.append(
                FileNode(name.strip(), href.strip(), last_mod.strip(), size.strip())
            )
    return results


# 4. [修改] 单个文件的下载逻辑 (Worker 函数)
def download_worker(
    file_node: FileNode, base_url, output_dir, limit_rate: str | None = None,tries:int=3
):
    """
    运行在子进程中的下载函数
    返回: (Success: bool, Message: str, URL: str, FileName: str)
    """
    full_url = base_url + file_node.href

    # 确保目录存在 (多进程中通常由主进程创建更好，但也为了健壮性保留)
    os.makedirs(output_dir, exist_ok=True)

    # 构造命令:
    # -q: 安静模式 (必须加，否则会打乱 rich 的进度条)
    # -c: 断点续传
    # -N: 只有比本地新才下载
    # -P: 指定目录
    # --limit-rate: 限速
    # --tries:重试次数
    cmd = ["wget", "-q", "-c", "-N", "-P",f"--tries={tries}", output_dir, full_url]
    if limit_rate:
        cmd.append(f"--limit-rate={limit_rate}")

    attempts = 0
    # 总尝试次数 = 1次首次 + MAX_RETRIES次重试
    total_attempts = 1 + MAX_RETRIES

    while attempts < total_attempts:
        try:
            # 运行命令
            subprocess.run(cmd, check=True)

            return True, "Success", full_url, file_node

        except subprocess.CalledProcessError:
            attempts += 1
            if attempts < total_attempts:
                # 等待后重试
                time.sleep(RETRY_DELAY)
            else:
                return False, "Failed after retries", full_url, file_node
        except Exception as e:
            return False, str(e), full_url, file_node
