# BioPubDatabase

BioPubDatabase 是一套面向 [PubChem](https://pubchem.ncbi.nlm.nih.gov/) SDF 数据的本地处理工具。项目不把完整分子记录复制进数据库，而是扫描原始 SDF 文件，将每条记录的文件编号和字节区间写入 LMDB；查询时再按偏移量读取原始记录。该方式适合体积较大、以 CID 或 Conformer ID 精确检索为主的数据集。

目前项目覆盖以下处理环节：

- 解析 PubChem FTP/HTTP 目录页，并通过外部 `wget` 下载文件；
- 校验下载文件的 MD5，解压归档文件；
- 为 Compound 与 Conformer SDF 文件建立 LMDB 偏移索引；
- 按 CID、Conformer ID 或内部 ALID 定位原始 SDF 记录；
- 批量查询 Compound/Conformer，并用 Compound 属性补充带 3D 坐标的 Conformer；
- 以分页 posting list 保存 `CID -> 多个 Conformer` 的一对多关系。

> 当前仓库更接近数据处理工具集和集成示例，而不是可直接部署的服务：没有 HTTP API、图形界面或已打包的命令行程序。

## 工作流程

```text
PubChem 目录
    │
    ├─ 下载 .sdf.gz / .md5
    ├─ MD5 校验与解压
    ▼
原始 Compound / Conformer .sdf
    │
    ├─ 流式扫描记录边界与关键字段
    ▼
LMDB 偏移索引
    │
    ├─ CID -> Compound
    ├─ Conformer ID -> Conformer
    ├─ CID -> Conformer 列表
    └─ ALID -> (file_id, start, end)
    ▼
按需读取原始 SDF / 合并 2D 属性与 3D 坐标
```

## 项目结构

```text
BioPubDatabase/
├─ nih/pubchem/
│  ├─ download/               # 目录页解析与 wget 下载基础函数
│  ├─ index/
│  │  ├─ sdf_index_builder.py # SDF 流式扫描和 LMDB 索引构建
│  │  ├─ sdf_index.py         # 单条、批量及一对多查询
│  │  ├─ record_locator.py    # 32 字节记录定位结构
│  │  └─ SDF_INDEX_DESIGN.md  # 索引设计说明
│  └─ types/                  # 下载文件节点等数据类型
├─ utils/
│  ├─ md5_check.py            # MD5 校验
│  ├─ files.py                # 递归文件检索
│  └─ compound_conf_merge.py  # Compound/Conformer RDKit Mol 合并
├─ testcase/
│  └─ nih/pubchem/data/       # 可运行的索引与查询示例 CLI
├─ requirements.txt
└─ README.md
```

## 环境要求

- Python 3.10 或更高版本；
- 使用下载功能时，需要系统已安装 `wget` 并可从 `PATH` 调用；
- 建立索引至少需要 `lmdb` 和 `rich`；
- 合并与导出分子还需要 `rdkit` 和 `pybiotech`；
- 解压辅助脚本使用 `ailingues-core`。

建议在项目根目录创建虚拟环境。PowerShell 示例：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

仓库的 `requirements.txt` 是开发环境的完整锁定依赖，包含构建、发布和辅助工具，数量多于核心索引功能的最小依赖。

## 数据准备

索引器只扫描指定根目录及其子目录中的 `*.sdf` 文件，不直接读取压缩包。建议将不同数据类型放在独立目录中，例如：

```text
D:\data\pubchem\
├─ source\
│  ├─ compound\   # 解压后的 Compound SDF
│  └─ conformer\  # 解压后的 Conformer SDF
└─ index\
   ├─ compound\   # Compound LMDB
   └─ conformer\  # Conformer LMDB
```

Compound 文件需要能从记录标题行或下列属性之一取得 CID：

- `CID`
- `PUBCHEM_COMPOUND_CID`
- `PUBCHEM_CID`
- `COMPOUND_CID`

Conformer 文件需要包含 Conformer ID 属性，例如 `CONFORMER_ID`、`CONFID`、`PUBCHEM_CONFORMER_ID` 或 `CONFORMERID`。父 CID 会从常见 CID 字段或 `PARENT_CID` 中提取。

## 快速开始

示例 CLI 位于 `testcase.nih.pubchem.data.main`。以下命令均应在项目根目录执行。

### 1. 建立索引

分别为 Compound 与 Conformer 数据建立索引：

```powershell
python -m testcase.nih.pubchem.data.main build `
  --root D:\data\pubchem\source\compound `
  --index D:\data\pubchem\index\compound

python -m testcase.nih.pubchem.data.main build `
  --root D:\data\pubchem\source\conformer `
  --index D:\data\pubchem\index\conformer
```

默认 LMDB `map_size` 为 1 TiB。它是地址空间上限，不代表会立即占用同等磁盘空间；也可以显式设置，例如 256 GiB：

```powershell
python -m testcase.nih.pubchem.data.main build `
  --root D:\data\pubchem\source\compound `
  --index D:\data\pubchem\index\compound `
  --map-size 274877906944
```

构建结束后会输出索引元数据，包括文件数、总记录数、Compound 数和 Conformer 数。

> 请为每次完整构建使用新的空索引目录。当前实现没有增量更新、旧记录清理或重复构建保护。

### 2. 按 CID 查询 Compound

```powershell
python -m testcase.nih.pubchem.data.main get-compound `
  --root D:\data\pubchem\source\compound `
  --index D:\data\pubchem\index\compound `
  --cid 361276
```

命令会输出 ALID、文件定位信息和原始 SDF 记录。

### 3. 按 Conformer ID 查询

```powershell
python -m testcase.nih.pubchem.data.main get-conformer `
  --root D:\data\pubchem\source\conformer `
  --index D:\data\pubchem\index\conformer `
  --confid 361276
```

`--confid` 必须与 SDF 记录中的 Conformer ID 完全一致。

### 4. 按 CID 范围读取 Conformer

```powershell
python -m testcase.nih.pubchem.data.main list-conformers `
  --root D:\data\pubchem\source\conformer `
  --index D:\data\pubchem\index\conformer `
  --cid_start 1 `
  --cid_end 1000 `
  --limit 1
```

当前示例会并行读取数据并显示进度，但不会把结果写入文件；若需要消费查询结果，建议直接调用 Python API。

### 5. 合并 Compound 与 Conformer

```powershell
python -m testcase.nih.pubchem.data.main batch-query-data `
  --compound_root D:\data\pubchem\source\compound `
  --compound_index D:\data\pubchem\index\compound `
  --conformer_root D:\data\pubchem\source\conformer `
  --conformer_index D:\data\pubchem\index\conformer `
  --cid_start 1 `
  --cid_end 1000 `
  --conformer_limit 1
```

合并时以 Conformer 的 3D 结构为基础，并复制 Compound 的属性。输出文件名形如 `01_01000_merged_mols.sdf`，生成在当前工作目录。当前批处理实现每个 CID 最终只保留一个 Conformer 用于合并，因此建议保持 `--conformer_limit 1`。

## Python API

### 构建索引

```python
from nih.pubchem.index.sdf_index_builder import SDFIndexBuilder

builder = SDFIndexBuilder(
    root_dir=r"D:\data\pubchem\source\compound",
    index_dir=r"D:\data\pubchem\index\compound",
)
metadata = builder.build()
print(metadata)
```

### 查询并读取原始 SDF

```python
from nih.pubchem.index.sdf_index import SDFIndex

root_dir = r"D:\data\pubchem\source\compound"

index = SDFIndex(r"D:\data\pubchem\index\compound", readonly=True)
try:
    hit = index.get_compound_by_cid(361276)
    if hit is not None:
        raw_record = index.read_segment(root_dir, hit.locator)
        print(raw_record.decode("utf-8", errors="replace"))
finally:
    index.env.close()
```

主要查询接口：

| 接口 | 用途 |
| --- | --- |
| `get_by_alid(...)` | 按确定性 UUID（ALID）查询记录 |
| `get_compound_by_cid(cid)` | 按 CID 查询一条 Compound |
| `get_conformer_by_conformer_id(id)` | 按 Conformer ID 查询一条 Conformer |
| `iter_conformers_by_cid(cid)` | 流式遍历某 CID 下的全部 Conformer |
| `batch_get_compounds_by_cid(cids)` | 分块批量查询 Compound |
| `batch_get_conformers_by_conformer_id(ids)` | 分块批量查询 Conformer |
| `read_segment(root_dir, locator)` | 按文件偏移读取原始 SDF 字节段 |

## 下载与校验

`nih.pubchem.download` 提供三个基础函数：

- `fetch_html_source(url)`：获取目录页 HTML；
- `parse_html_content(html)`：解析文件名、链接、更新时间和大小；
- `download_worker(...)`：封装系统 `wget`，设计参数包括断点续传、更新时间判断、限速和重试。

MD5 校验可独立使用：

```python
from utils.md5_check import verify_md5

ok = verify_md5(
    r"D:\data\pubchem\example.sdf.gz",
    r"D:\data\pubchem\example.sdf.gz.md5",
)
print(ok)
```

`testcase/nih/pubchem/download/test_download.py` 是面向特定服务器目录的实验脚本，包含 Linux 数据路径和固定下载参数，且其工具模块导入路径尚未与当前目录结构同步。`download_worker` 当前生成的 `wget -P/--tries` 参数顺序也需要修正后再用于正式下载，因此这部分不应视作开箱即用的下载 CLI。

## 索引实现

每条记录使用固定 32 字节的 `RecordLocator` 保存：

```text
file_id:uint32 + start:uint64 + end:uint64 + flags:uint16 + cid:int64 + reserved:uint16
```

索引包含以下 LMDB 子库：

- 文件 ID 与相对路径的双向映射；
- ALID 到 `RecordLocator` 的主记录表；
- CID 到 Compound 的唯一索引；
- Conformer ID 到 Conformer 的唯一索引；
- CID 到 Conformer ALID 的分页 posting list，每页最多 4096 条。

ALID 使用 UUIDv5 确定性生成，输入由数据类型、相对文件路径、文件内记录序号和主标识组成。只要这些输入不变，同一条记录会得到相同 ALID。更完整的设计说明见 [`nih/pubchem/index/SDF_INDEX_DESIGN.md`](nih/pubchem/index/SDF_INDEX_DESIGN.md)。

## CLI 命令概览

| 命令 | 作用 | 结果 |
| --- | --- | --- |
| `build` | 扫描 SDF 并建立 LMDB 索引 | 输出 JSON 元数据 |
| `get-compound` | 按 CID 查询 Compound | 输出定位信息和原始 SDF |
| `batch-get-compound` | 按闭区间批量读取 Compound | 显示进度，示例不落盘 |
| `get-conformer` | 按 Conformer ID 查询 | 输出定位信息和原始 SDF |
| `list-conformers` | 按 CID 闭区间读取 Conformer | 显示进度，示例不落盘 |
| `batch-query-data` | 批量合并 Compound/Conformer | 在当前目录生成 SDF |

查看完整参数：

```powershell
python -m testcase.nih.pubchem.data.main --help
```

## 当前限制

- Compound 与 Conformer 的类型目前主要根据文件名判断：文件名包含 `conformer` 或 `conf` 时视为 Conformer，否则通常视为 Compound。若原始 Conformer 文件名不含这些关键字，需要重命名文件，或在 Python 中为 `SDFIndexBuilder` 传入自定义 `conformer_name_patterns`。
- 索引只处理以 `$$$$` 正常结束的完整 SDF 记录；文件末尾未闭合的记录会被忽略。
- 同一 CID 的 Compound、同一 Conformer ID 出现多次时，唯一索引保留最后写入的记录。
- 索引保存的是原始文件相对路径和字节偏移。建好索引后不要修改、重新格式化或移动根目录内的 SDF 文件；若数据变化，应使用新的空目录重建索引。
- 示例 CLI 用 `cid_start..cid_end` 生成完整整数列表，大范围查询会占用相应内存。
- `testcase` 中的内容主要依赖外部 PubChem 数据进行集成演示，目前没有可脱离数据集运行的自动化测试套件。

## 开发说明

在提交代码前，至少可以进行语法检查：

```powershell
python -m compileall nih utils testcase
```

若要运行 CLI 或集成示例，请先安装 `requirements.txt` 并准备对应的 PubChem SDF 数据。索引设计取舍及 LMDB/SQLite 对比可参考：

- [`nih/pubchem/index/SDF_INDEX_DESIGN.md`](nih/pubchem/index/SDF_INDEX_DESIGN.md)
- [`nih/pubchem/index/comment.md`](nih/pubchem/index/comment.md)
