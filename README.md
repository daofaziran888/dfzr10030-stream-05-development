# 管道工坊 · 实时数仓与流式管道平台

面向实时数据接入、流式转换、变更捕获、窗口聚合、状态管理、断流恢复与多维数据集下发的实时数仓与流式管道开发平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 源端绑定 | `source_binding` | 数据源端 | 源端编号、源端类型、连接地址 |
| 变更捕获 | `cdc_task` | CDC任务 | 任务编号、源数据库、监控表 |
| 流式转换 | `stream_transform` | 转换算子 / 订单转换批次 | 算子编号、输入流、输出流；批次号、源订单编号、发布状态 |
| 窗口聚合 | `window_agg` | 时间窗口 / 窗口入账 | 窗口编号、聚合指标、窗口类型；已发布笔数、未到账范围 |
| 状态管理 | `state_store` | 算子状态 | 状态编号、关联算子、存储后端 |
| 规则引擎 | `rule_engine` | 业务规则 | 规则编号、规则条件、触发动作 |
| 水位管理 | `watermark_mgr` | 事件水位 | 水位编号、关联流、水位类型 |
| 数据落盘 | `sink_writer` | 输出槽位 | 槽位编号、输出类型、目标地址 |
| 背压控制 | `backpressure` | 背压策略 | 策略编号、关联算子、队列上限 |
| 断流恢复 | `checkpoint` | 检查点 | 检查点编号、关联流水线、位点时间 |
| 流式质检 | `data_quality` | 质检规则 | 规则编号、关联流、检查项 |
| Schema演进 | `schema_evolve` | 结构变更 | 变更编号、关联流、变更类型 |
| 管道编排 | `pipeline_orch` | 管道DAG | 管道编号、管道名称、节点列表 |
| 去重管理 | `dedup_mgr` | 去重策略 | 策略编号、关联流、去重键 |
| 指标计算 | `metric_calc` | 实时指标 | 指标编号、指标名称、计算公式 |
| 事件路由 | `event_router` | 路由规则 | 规则编号、事件类型、匹配条件 |
| 历史回放 | `replay_batch` | 回放任务 | 任务编号、数据源、起始时间 |
| 分组分区 | `grp_partition` | 分组策略 | 策略编号、关联流、分区键 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 订单转换批次与窗口入账

窗口聚合和指标计算不再直接读固定样本，而是从持久化的订单转换批次取数：

- `POST /api/order_transform/batches`（流式转换页「发布订单批次」）接收一批新订单。
  每行先以原始值落进 `order_transform_record`，再按金额、事件时间、用户字段校验。
  金额非法、缺失或不大于 0 的行不会被当作 0 元；时间缺时区或无法解析的行不会被当作
  当前时间，这两类行统一进入隔离区并保留原始值、源订单编号和批次号。
- `POST /api/order_transform/batches/{batch_no}/retry` 用于落库失败后的续跑：从转换批
  找回坏行原始值，只确认仍待处理的行，已发布订单不会重复。
- `POST /api/order_transform/batches/{batch_no}/records/{source_order_id}` 修正隔离行，
  修正值与原始值分开保存；重新提交同一源订单时若原记录尚未发布，会就地重新校验。
- `GET /api/order_transform/windows/{window_no}` 只对已发布记录汇总，并返回未到账笔数、
  订单范围和已发布未入账数量；`POST .../settle` 表示窗口实际入账，回写核对清单。
- `GET /api/order_transform/orders/{source_order_id}` 按源订单编号回查，刷新或重新进入
  窗口后仍返回已提交批次中的同一条记录。

领域：数据工程
子方向：数仓与数据湖
