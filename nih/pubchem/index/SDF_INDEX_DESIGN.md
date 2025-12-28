# PubChem 原始数据索引方案设计文档

---

## SDF Offset Index 设计方案（Compound + Conformer）

### 背景与目标

项目存在大量 `.sdf` 文件（单文件可达 GB 级），包含海量分子记录。目标是在不解析全文件的情况下，实现**快速定位与检索**：

* 一次性为指定目录下所有 SDF 文件建立索引；
* 能快速定位到：

  * 哪个 SDF 文件；
  * 该文件中哪一段文本（记录的 start/end 字节偏移）；
* 支持高吞吐查询：

  * `CID` 查 compound；
  * `CID` 查 conformers（0..N）；
  * `conformer_id` 查 conformer；
  * `ALID`（项目生成的唯一编号）查任意记录；
* 支持大规模批量查找（数万到数十万 key），并要求每个 key 的查询成本近似常数，整体吞吐线性增长。

> 注：本方案取消对 SMILES 的检索支持（按需求变更）。

---

### 数据模型

系统中存在两类 SDF：

1. **Compound SDF**

   * 一条 SDF 记录对应一个化合物实体（通常可解析出 CID）
   * 关系：compound 与 conformer 为 1:N

2. **Conformer SDF**

   * 一条 SDF 记录对应一个构象（conformer）
   * 包含 `conformer_id`，并可（尽量）解析出其所属 `CID`（parent CID）

---

### 核心思想：外部索引 + 字节偏移定位（Offset Index）

SDF 是文本格式，每条记录以分隔符 `$$$$` 结束。
索引构建过程对每条记录仅做：

* 记录该记录的 **start_offset**（开始字节位置）
* 记录该记录的 **end_offset**（结束字节位置）
* 提取少量关键字段（CID / conformer_id / parent CID）
* 生成项目唯一编号 **ALID**（确定性 UUIDv5）

读取记录时无需扫描文件：
只需 `seek(start_offset)` 并读取 `end_offset - start_offset` 字节即可得到完整记录文本。

---

### 存储选型：LMDB（Key-Value）

为满足“批量几十万 key”的吞吐需求，选用 LMDB 作为索引存储：

* 适合高频点查（key -> value）
* 读路径开销极低（mmap + 最少抽象）
* 批量查询可实现每个 key 均摊成本近似常数
* 支持只读并发访问（索引构建后通常读多写少）

SQLite 未被选用的原因：

* 面对几十万级 `IN (...)` 或批量点查时，SQL 层解析/绑定/执行开销明显；
* 参数数量与 SQL 编译成本会成为工程瓶颈；
* 虽然可行，但在该场景下不如 KV 直接。

---

### 索引结构

索引由一个 LMDB 环境组成，包含多个子 DB：

#### 1) 文件表（file registry）

* `files`: `file_id -> relative_path`
* `files_rev`: `relative_path -> file_id`

用于在索引中用整数 `file_id` 代替冗长路径。

#### 2) 记录定位表（records）

* `records`:

  * key: `b"C" + ALID.bytes`（compound）或 `b"F" + ALID.bytes`（conformer）
  * value: 固定长度二进制结构，包含：
    `file_id, start, end, flags(is_conformer), cid(optional)`

该表是最终的“定位真相源”，所有查询最终都要回到这里拿 `(file_id, start, end)`。

#### 3) 唯一键索引

* `cid_to_compound`: `cid -> record_key(C + alid16)`
  用于 `CID -> compound`（预期 0/1）
* `confid_to_conf`: `conformer_id -> record_key(F + alid16)`
  用于 `conformer_id -> conformer`（预期 0/1）

#### 4) 一对多索引：CID -> conformers（posting list 分页）

当一个 CID 对应很多 conformers 时，不能把所有 conformer ALID 塞进一个 value。

方案采用分页 posting list：

* header：

  * `cid_to_conformers_h`: `cid -> page_count(uint32)`
* pages：

  * `cid_to_conformers_p`: `(cid|page_no) -> [ALID16, ALID16, ...]`（拼接的 16 字节序列）

每页默认 4096 个 ALID（约 64KB），支持极大 N（数十万级）。

查询时可流式迭代每一页，避免内存爆炸。

---

### ALID 生成策略（确定性）

ALID 为项目内唯一编号，要求索引可重建且编号稳定。

采用 UUIDv5（确定性哈希 UUID）：

* Compound record：

  * `ALID = uuid5(namespace, "compound|relpath|rec_no|cid")`
* Conformer record：

  * `ALID = uuid5(namespace, "conformer|relpath|rec_no|conformer_id")`

其中：

* `relpath` 为相对路径（避免机器路径变化导致 ALID 变化）
* `rec_no` 为该文件内记录序号
* `cid / conformer_id` 用于增强稳定性与可读性

---

### 索引构建流程

对目录内每个 `.sdf` 文件：

1. 判断文件类型（compound / conformer）

   * 依据文件名 pattern（可配置）
2. 二进制方式流式读取
3. 记录每条记录的 start/end offset
4. 解析需要字段（只解析 `> <FIELD>` 块）

   * compound：优先使用 title line 的 CID（若为纯数字）
   * conformer：提取 conformer_id；并尽量提取 parent CID
5. 生成 ALID
6. 写入：

   * `records`
   * `cid_to_compound` 或 `confid_to_conf`
   * conformer 额外写入 `cid_to_conformers_*` posting list

最终生成 meta 信息（schema_version, build_time, counts 等）。

---

### 查询与批量查询策略

#### 单次查询

* CID -> compound：

  1. `cid_to_compound.get(cid)` 得到 record_key
  2. `records.get(record_key)` 得到 locator

* conformer_id -> conformer：

  1. `confid_to_conf.get(confid)` -> record_key
  2. `records.get(record_key)` -> locator

* CID -> conformers：

  1. `cid_to_conformers_h.get(cid)` -> page_count
  2. 对每页：读取 page blob，拆分为 ALID16
  3. 对每个 ALID16：回 `records` 取 locator（流式 yield）

#### 大批量查询（数万～数十万 key）

* 采用 chunk（默认 50k）分块处理；
* 每个 key 做一次 `txn.get()`，均摊成本近似常数；
* 结果流式输出（generator），避免一次性堆内存。

---

### 根据 locator 读取原始 SDF 片段

给定 `RecordLocator(file_id, start, end)`：

1. `file_path = root_dir / resolve(file_id)`
2. `seek(start)`
3. `read(end - start)` 得到记录文本（bytes）

---

### 可靠性与生产建议

* 所有 offset 使用字节偏移（binary mode），避免 UTF-8 多字节导致错位
* posting list 分页避免 value 过大
* 建议索引构建后只读使用（readonly=True），可提升并发读性能
* 可增加 manifest（文件 size/mtime/hash）实现增量更新与过期检测（后续扩展）

---

### 方案边界

* 不支持 SMILES 相关检索（按需求取消）
* 未实现增量更新（当前为全量构建；可基于 manifest 扩展）
* conformer 归属 CID 依赖源数据字段质量（尽可能提取多候选字段）

---

## 结束语（现实一点的结论）

SQLite **当然能做**，甚至做得很优雅；但在“几十万 key 批量点查 + offset 定位”的场景里，SQLite 的优势没怎么发挥，反而会被 SQL 层开销和参数/查询构造限制拖慢。LMDB 更像“在一个磁盘上的超级 dict”，直球命中需求。

如果后续要加“复杂查询、统计、分析”，可以再加一个 SQLite/Parquet 的“分析索引层”，与 LMDB 并存：
LMDB 负责**定位**，SQLite 负责**分析** —— 这是很多高性能系统的经典分层。
