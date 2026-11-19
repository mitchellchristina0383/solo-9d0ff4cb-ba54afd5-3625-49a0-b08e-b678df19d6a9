# 奇偶校验文件卷（Parity File Volume）

用 Python + FastAPI + SQLite 实现的三盘条带化文件卷：每个文件按固定条带
（默认 512 KiB）切成 **两份数据块 + 一份 XOR 异或校验块**，分散存放在三个
相互独立的持久目录中。任一单盘故障，已确认上传的文件仍可下载；换上空盘后
后台自动重建冗余，重建期间允许继续上传。

## 目录布局

```
parity-volume/
├── app/
│   ├── coding.py        # XOR 编码 / 任意两块重建第三块
│   ├── storage.py       # 块文件原子写、换盘归档
│   ├── db.py            # SQLite 元数据（WAL，文件/条带/块/磁盘/重建任务）
│   ├── volume.py        # 上传、下载恢复、下线、换盘、重建、崩溃恢复
│   ├── main.py          # FastAPI 路由与异常映射
│   └── web/index.html   # 单文件操作网页（无构建步骤）
├── tests/selftest.py    # 端到端自测（真实 uvicorn + HTTP）
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

三个“盘”是三个持久目录（生产环境应分别挂载到不同物理盘/独立卷），
SQLite 元数据放在第四个独立目录 `data/meta/`，单盘故障不影响元数据。

## 快速启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

无 Docker 时本地运行：

```bash
pip install -r requirements.txt
PV_DATA_DIR=./data PV_DB_PATH=./data/meta/volume.db \
  uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 网页操作

* **上传**：点击或拖拽文件。只有三块块全部落盘并提交元数据后，文件才会
  出现在“已确认文件”列表中。
* **下线**：把某盘标记为故障（模拟拔盘）。
* **换空盘并重建**：旧盘目录归档为 `diskN.failed-<时间戳>-…`，原位建立空
  目录，后台线程按条带用其余两块盘重建；页面每 2 秒刷新进度。
* **下载/删除**：对已确认文件操作；下载时若有盘下线，自动用剩余两块在线
  重建数据。

## 关键正确性设计

| 需求 | 实现方式 |
| --- | --- |
| 单盘故障可读 | 每条带 3 块等长、`d0 XOR d1 XOR p = 0`，任两块可重建第三块；下载逐带恢复并按记录长度裁掉零填充 |
| 重建期间可上传 | 新条带直接三盘写入（含重建中的新盘）；重建收尾再全盘扫描，确认新条带在新盘上不缺失才置 `online` |
| 残缺条带不会标健康 | 条带先以 `healthy=0` 入库，三个块原子写（tmp+fsync+rename）完成后才置 `healthy=1`；重建只扫描健康条带 |
| 上传中断/重启 | 未确认文件（`status='uploading'`）启动时连同块文件一并清除，永不可见；已确认文件不受影响 |
| 重建中断/重启 | 重建任务持久化在 `rebuild_jobs`，重启后自动续跑；块写入幂等，重复重建同一条带结果一致 |
| 超一盘失效 | 可读块不足两块时返回 HTTP 503 `{"error":"unrecoverable"}`，明确报不可恢复 |
| 降级拒绝残缺写入 | 任一盘 `offline` 时上传直接 503 拒绝，不会写出只有两块的新条带 |
| 物理丢盘识别 | 运行时对账：在线盘目录消失自动转 `offline`；已有部署启动时先检测后建目录，不会掩盖真实丢盘 |

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/status` | 磁盘状态、重建进度 |
| GET | `/api/files` | 已确认文件列表 |
| POST | `/api/files` | multipart 上传 |
| GET | `/api/files/{id}` | 下载（必要时自动重建） |
| DELETE | `/api/files/{id}` | 删除文件及其全部块 |
| POST | `/api/disks/{0..2}/offline` | 磁盘下线 |
| POST | `/api/disks/{0..2}/replace` | 换空盘并启动重建 |

## 自测

```bash
# 需要 fastapi/uvicorn/httpx，可通过 PV_EXTRA_SYS_PATH 指定依赖目录
python -I tests/selftest.py
```

覆盖：逐盘下线下载、双盘失效 503、换盘重建后三盘齐备、重建与 4 个上传
交错、重建中上传后重启续建、上传中断清理、服务重启恢复、物理目录消失、
降级态拒绝上传等 80 项断言。

## 已知边界

* 条带布局为固定的“2 数据块 + 1 校验块”，只容忍单盘故障（题目要求范围）。
* 块级静默腐化（bit rot）仅在三盘齐全时靠下载时的奇偶校验发现，没有后台
  定期巡检任务。
* 上传为单请求流式处理，未实现断点续传；大文件占用条带数 × 512 KiB 的
  三盘空间（含零填充的末条带）。
