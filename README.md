# 并发写入观测台（隔离级别沙箱）

给数据库工程师的一个小工具：在改隔离级别之前，先看看**同一组并发写入**在不同隔离级别下会留下什么数据。

另有一个**设备台账维护**入口（首页顶部链接，或直接访问 `/maintenance`）：在已有数据的
PostgreSQL 样例台账上，演练用 `CREATE UNIQUE INDEX CONCURRENTLY` 给设备编号在线补建唯一索引
的完整运维流程，包括重号失败、无效索引清理、重试与服务重启恢复。

两个会话（A、B）按可编辑的脚本交错执行 SQL。同一组脚本、同一份初始数据，分别在
**READ COMMITTED** 和 **SERIALIZABLE** 下各跑一轮，页面上并排展示：

- 每一步的执行时间线（完成 / 出错 / 被阻塞 / 解除阻塞）；
- 每个事务的结局（已提交 / 已回滚 / 40001 中止）；
- 两轮事务都结束后，核对查询查到的最终数据。

每步有两种推进方式：

| 模式 | 含义 |
| --- | --- |
| `complete`（默认） | 等这条语句执行完成再推进下一步 |
| `blocked` | 语句在后台执行，等数据库确认它**被另一会话阻塞**（`pg_blocking_pids()`）就推进下一步；语句仍在飞，持锁会话可以继续走完自己的 `COMMIT` |

每轮有超时（默认 5 秒）：超时立即结束本轮，取消卡住的语句并**回滚未提交的事务**。
重跑总是从“初始数据 SQL”重新开始，两轮之间、两次运行之间互不影响。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8080
```

`web` 依赖 `db` 的健康检查，数据库就绪后页面即可用。端口默认 `127.0.0.1:8080`
（本工具不做任何鉴权，会原样执行你贴的 SQL，请勿暴露到公共网络）。

## 页面与脚本语法

页面上三段可编辑内容：

1. **初始数据 SQL**：每轮开始前在控制连接上执行（可多条语句），负责把库重置到相同起点；
2. **交错脚本**：见下；
3. **核对查询**：两轮事务都结束后，在新连接上执行并展示结果。

交错脚本由步骤组成，每个步骤一行头部 + 一段 SQL：

```sql
@A complete
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

@B blocked
UPDATE accounts SET balance = balance - 200 WHERE id = 1;

@A complete
COMMIT;
```

- 头部：`@A` 或 `@B`，模式可写 `complete` / `blocked`（也接受 `完成` / `阻塞`），省略时是 `complete`；
- 事务由引擎按本轮隔离级别自动 `BEGIN ISOLATION LEVEL ...`，**脚本里不要写 `BEGIN`**，
  用 `COMMIT` / `ROLLBACK` 结束事务；脚本结束仍未提交的事务会被引擎回滚并记在时间线里；
- 一个步骤里可以写多条语句（结果只展示最后一条的），但建议一步一条，时间线更清晰。

## 内置样例

页面顶部可一键载入：

1. **两人同时撤班（写偏斜）**：`alice`、`bob` 两人在岗，规则是“至少留一人在岗”。
   两人同时撤班：各自确认在岗人数 ≥ 2 才撤。READ COMMITTED 下两笔都成功，
   核对查询显示 `on_duty_count = 0`——**无人值守**；SERIALIZABLE 下后提交者收到
   `40001 could not serialize access`，事务中止，仍有一人在岗。
2. **行锁等待（blocked 模式）**：两个会话更新同一行。B 用 `blocked` 模式确认被 A
   阻塞即推进，让 A 先提交。READ COMMITTED 下 B 在 A 的结果上继续（余额 700）；
   SERIALIZABLE 下 B 解除阻塞时收到 40001（余额 900）。
3. **超时回滚（演示）**：B 用 `complete` 模式等 A 的行锁，而 A 的 `COMMIT` 排在 B 之后
   永远轮不到 → 本轮超时，引擎取消卡住的语句并回滚两个事务，余额保持 1000。
   超时后立刻重跑，仍从初始数据开始。

## 设备台账维护（唯一索引在线补建）

首页顶部链接进入 `/maintenance`。场景：`devices` 台账已有约 40 万行数据，值班 DBA 要给
`device_no` 补建唯一索引，且日常读写不能停。

### 页面能做什么

- **重置为演示台账**：灌入 40 万台设备（自测可用 12 万行的小数据量），其中刻意留一对重号
  `DEV-DUP-1`；
- **录入设备 / 更正编号**：普通的日常写入入口，构建期间也照常可用；
- **重号清单**：列出所有重复编号及涉及的行，可直接在页面上把某一台改成新编号；
- **补建 / 重试唯一索引**：后台线程执行
  `CREATE UNIQUE INDEX CONCURRENTLY devices_device_no_uidx ON devices (device_no)`，
  页面按库内实况（`pg_index` / `pg_stat_activity`）自动轮询，显示
  **正在构建 / 无效（INVALID）/ 已生效**；
- 建成后录入重复编号会被数据库以 `23505` 拒绝，页面如实展示数据库错误。

### 运维语义（与需求逐条对应）

1. **在线构建、读写照常**：CONCURRENTLY 不拿 `SHARE` 锁，构建期间普通 INSERT/UPDATE 不阻塞。
2. **状态只信库内实况**：页面的阶段标签完全由 `pg_index.indisvalid/indisready/indislive`
   与 `pg_stat_activity` 中是否存在以 `CREATE ... INDEX` 开头、且文本含目标索引名的在飞会话推导；
   进程内只保存最近一次尝试的日志，重启后状态仍能正确识别。
3. **重试前先确认上次构建已结束**：重试时先等待针对本索引名的在飞会话消失
   （上限 60 秒，超时拒绝并提示人工确认），不会并发重复构建。
4. **只清理本项补建留下的无效索引**：索引名固定为 `devices_device_no_uidx`，清理只对这个名字的
   INVALID 索引执行 `DROP INDEX IF EXISTS`，不触碰任何其他对象；若存在同名非唯一索引则拒绝自动处理。
5. **服务重启恢复**：
   - 构建途中重启 → 库内残留 INVALID 索引，新实例重试时等待在飞结束、清理该 INVALID 后重建；
   - 索引生效后、HTTP 结果返回前重启 → 新实例发现有效唯一索引已存在，**直接沿用、不重复构建**
     （索引 oid 不变），并在任务记录里注明“沿用”。
6. **建成后由数据库兜底**：唯一约束在数据库侧生效，任何重复 `device_no` 写入都收到 23505。

### HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/maintenance/status` | 库内实况 + 最近一次补建任务状态 |
| POST | `/api/maintenance/reset` | 重置演示台账（body 传 `{"small": true}` 用小数据量） |
| GET | `/api/maintenance/devices?q=` | 最近 20 台设备，可按编号/名称过滤 |
| GET | `/api/maintenance/duplicates` | 重号清单 |
| POST | `/api/maintenance/devices` | 录入设备（重号在索引生效后返回 409/23505） |
| POST | `/api/maintenance/devices/correct` | 更正指定 id 的编号 |
| POST | `/api/maintenance/index/build` | 启动/重试在线补建（异步，轮询 status 看进度） |

## 自测

栈起来之后，在容器里跑端到端自测（覆盖：写偏斜两种结局、锁等待的阻塞确认与解除、
超时回滚、超时后重跑、blocked 模式未阻塞分支、脚本解析器）：

```bash
docker compose up -d --build
docker compose exec web python tests/selftest.py
```

台账维护功能另有一套端到端自测（构建期间写入、重号失败与重试、只清理本补建的无效索引、
构建中断重启恢复、生效后重启沿用、建成后拒绝重号、在飞并发拒绝）：

```bash
docker compose exec web python tests/selftest_maintenance.py
```

也可以对着任意可达的 PostgreSQL 跑：

```bash
pip install -r app/requirements.txt
PGHOST=... PGDATABASE=... PGUSER=... PGPASSWORD=... python tests/selftest.py
PGHOST=... PGDATABASE=... PGUSER=... PGPASSWORD=... python tests/selftest_maintenance.py
```

## 实现要点

- `app/engine.py`：执行引擎。每个会话一条连接（`autocommit=True`，事务边界完全由
  SQL 语句控制），语句在工作线程里执行；`blocked` 模式通过控制连接轮询
  `pg_blocking_pids()` 确认阻塞；超时用 `pg_cancel` 取消在飞语句；收尾先回滚
  “没有在飞语句”的事务（可能借此解除另一会话的阻塞），再等在飞语句结束。
- 事务结局以服务器命令标签为准：`COMMIT` 一个已失败的事务时 PostgreSQL 实际返回
  `ROLLBACK`，页面会如实显示为回滚；`40001` / `40P01` 记为“已中止”。
- `app/script_parser.py`：`@A complete` / `@B blocked` 头部解析，带行号报错。
- `app/main.py`：Flask 页面与 `/api/run`。所有请求共用一个沙箱库，同一时刻只允许
  一轮实验在跑（其余请求收到 409）。
- `app/maintenance.py`：设备台账与唯一索引在线补建。所有连接 `autocommit=True`
  （CONCURRENTLY 不能在事务块中运行）；状态判断只查 `pg_index` /
  `pg_stat_activity`，进程内的 `IndexBuildManager` 只做任务编排，重启后以库内实况为准。
- `app/templates/maintenance.html`：维护入口页面，构建中按 800ms 轮询库内实况。
- `tests/selftest.py`：并发沙箱的端到端自测。
- `tests/selftest_maintenance.py`：台账维护功能的端到端自测。

## 限制与说明

- 只支持两个会话；`blocked` 确认是 50ms 轮询，时间线精度有限；
- 引擎**不会**自动重试 40001（重试策略属于应用层，可在脚本里自行表达）；
- 单步可含多条语句，但只展示最后一条的结果；
- 初始数据 SQL 在 `postgres` 超级用户下执行，能力不设限——这是本地沙箱，不是多租户服务；
- 台账维护的表名/索引名固定（`devices` / `devices_device_no_uidx`），重试清理只针对该固定
  命名的 INVALID 索引；演示台账重置会 DROP 这两个本功能对象，不影响其他表；
- 数据不持久化：每轮重置，容器删除即清空。
