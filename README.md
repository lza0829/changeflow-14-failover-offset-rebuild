# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见题单）

当前仓库已实现「连续序幂等应用」主链。缺口等待/跳过、乱序缓冲、事务整批、双 sink 部分失败、毒丸选择性跳过等均在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。

## 已实现切口：故障后位点重建（failover_offset_rebuild）

`apply_cursor` 丢失或被改坏时**不清零瞎跑**：以 Sink（`sink_records.last_seq`）与已应用
账本（`applied_seqs` + `change_ledger`，隔离区 `quarantine` 作旁证）为证据重建位点，
人工确认后才切换。

闭环（页面「故障位点重建」，API 前缀 `/api/failover-rebuild`）：

1. **扫证据 → 候选**：候选位点 = `applied_seqs` 从 1 起的最长连续前缀；任务记录扫描规模、
   证据摘要与逐条计算依据。此步只读，**不改游标**。
2. **门禁**：G1 证据非空；G2 Sink 不得超出账本；G3 Sink 不得越过连续前缀；
   G4 隔离区与已应用集合互斥；G5 账本两来源一致。任一不过即拒绝切换，任务落
   `rejected`，不允许标记成功。
3. **确认才切换**：确认前重新扫库复核，通过后在同一事务写新位点并留审计
   （`cursor_before → cursor_after`）；人工拒绝则游标不动。任务持久化于
   `failover_rebuild_tasks`，可对照账本、Sink、游标页核验。

边界：不是回拨重放（不重放事件、不改 Sink/账本），也不是行级校验/漂移门禁
（不逐行比对 payload）。页面内置演练工具：改坏游标、注入 Sink/账本矛盾、清理注入数据。
