"""设备台账维护：给已有数据的设备编号补建唯一索引（在线方式）。

业务背景：值班 DBA 要在不锁表、日常读写照常的前提下，给设备台账
``devices.device_no`` 补建唯一索引。补建采用
``CREATE UNIQUE INDEX CONCURRENTLY``：

- 构建期间不阻塞普通读写（不拿 SHARE 锁，只与同时刻的 DDL 冲突）；
- 若表里存在重号，语句失败并留下一个 ``INVALID`` 索引，需要 DBA 先更正
  数据，再 *只清理本项补建留下的无效索引* 后重试；
- 服务若在索引生效后、HTTP 结果返回前重启，库里的有效索引仍然存在，
  重试时直接沿用，不会重复构建。

本模块的所有状态判断都以数据库实况为准（``pg_index`` /
``pg_stat_activity``），进程内只记录最近一次构建尝试的日志与错误，
进程重启后状态依然可被正确识别。

固定对象命名，保证“只清理这项补建留下的索引”，绝不误删其他索引：
  表：devices            索引：devices_device_no_uidx
"""
from __future__ import annotations

import threading
import time

import psycopg2
from psycopg2 import errors, sql

TABLE = "devices"
INDEX = "devices_device_no_uidx"
COLUMN = "device_no"

# 重试前等待“可能仍在飞的构建”结束的上限与轮询间隔
INFLIGHT_WAIT_TIMEOUT = 60.0
INFLIGHT_POLL = 0.2

# 演示数据规模：reset 后灌入的设备行数（重号两行除外均不重复）
DEMO_ROWS = 400_000
# 自测用的较小规模，缩短构建耗时
SELFTEST_ROWS = 120_000

DUPLICATE_PGCODES = {"23505", "23P01"}  # unique_violation / exclusion_violation


class MaintenanceError(Exception):
    """维护操作的可预期错误，携带 HTTP 状态码与提示。"""

    def __init__(self, message, status=400, pgcode=None):
        super().__init__(message)
        self.message = message
        self.status = status
        self.pgcode = pgcode


def connect(ci):
    """开一条 autocommit 连接——CONCURRENTLY 不能在事务块里运行。"""
    conn = psycopg2.connect(**ci)
    conn.autocommit = True
    return conn


def _err_text(exc):
    diag = getattr(exc, "diag", None)
    msg = getattr(diag, "message_primary", None) if diag else None
    return msg or str(exc).strip()


# ---------------------------------------------------------------------------
# 库内实况查询
# ---------------------------------------------------------------------------

def _index_row(conn):
    """返回目标索引在 pg_index 中的实况行，不存在返回 None。"""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.oid,
               i.indisvalid,
               i.indisready,
               i.indislive,
               NOT i.indisunique AS not_unique
          FROM pg_class c
          JOIN pg_index i ON i.indexrelid = c.oid
         WHERE c.relname = %s
           AND c.relnamespace = (SELECT oid FROM pg_namespace
                                 WHERE nspname = current_schema())
        """,
        (INDEX,),
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return {"oid": row[0], "indisvalid": row[1], "indisready": row[2],
            "indislive": row[3], "not_unique": row[4]}


def _inflight_build_pids(conn):
    """查当前库内是否还有 CREATE INDEX（含 CONCURRENTLY）在飞。

    只认命令文本里带目标索引名的会话，避免把别的索引构建误判为本任务。
    """
    cur = conn.cursor()
    # 用“语句以 CREATE [UNIQUE] INDEX 开头”识别真实构建语句：
    # 监控查询自身的文本里虽含索引名字面量，但它以 SELECT 开头，不会被误匹配。
    cur.execute(
        """
        SELECT pid,
               left(query, 200) AS query,
               state,
               wait_event_type,
               wait_event
          FROM pg_stat_activity
         WHERE datname = current_database()
           AND pid <> pg_backend_pid()
           AND query ~* %s
           AND query ILIKE %s
           AND state <> 'idle'
         ORDER BY pid
        """,
        (r"^\s*CREATE\s+(UNIQUE\s+)?INDEX", f"%{INDEX}%"),
    )
    rows = cur.fetchall()
    cur.close()
    return [{"pid": r[0], "query": r[1], "state": r[2],
             "wait_event_type": r[3], "wait_event": r[4]} for r in rows]


def _table_exists(conn):
    cur = conn.cursor()
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (TABLE,))
    exists = cur.fetchone()[0]
    cur.close()
    return exists


def _duplicate_count(conn):
    """统计 device_no 重号组数与涉及行数；表不存在时返回 (0, 0)。"""
    if not _table_exists(conn):
        return 0, 0
    cur = conn.cursor()
    cur.execute(
        sql.SQL(
            """
            SELECT count(*), coalesce(sum(c), 0)
              FROM (SELECT {col}, count(*) AS c
                      FROM {tbl} GROUP BY {col} HAVING count(*) > 1) d
            """
        ).format(col=sql.Identifier(COLUMN), tbl=sql.Identifier(TABLE))
    )
    groups, rows = cur.fetchone()
    cur.close()
    return int(groups or 0), int(rows or 0)


def _index_phase(idx, inflight):
    """根据库内实况推导面向页面的阶段标签。"""
    if inflight:
        if idx is None:
            return "building", "索引正在构建（CONCURRENTLY 分阶段扫描，读写不阻塞）"
        if not idx["indisvalid"]:
            return "building", "索引正在构建（pg_index 中尚为 invalid，构建会话仍在飞）"
        return "building", "构建会话仍在飞，索引已可见，等待收尾"
    if idx is None:
        return "absent", "尚无补建索引"
    if not idx["indisvalid"]:
        return "invalid", "索引无效：上次构建失败或被中断，需更正重号后清理重试"
    if idx["not_unique"]:
        return "non_unique", "存在同名索引但不是唯一索引（非本补建产物，请人工核对）"
    return "valid", "唯一索引已生效，重复编号将被数据库拒绝"


def get_snapshot(ci, job):
    """汇总一次库内实况，供页面与 API 返回。"""
    conn = connect(ci)
    try:
        table_exists = _table_exists(conn)
        idx = _index_row(conn)
        inflight = _inflight_build_pids(conn)
        dup_groups, dup_rows = _duplicate_count(conn)
        device_count = None
        if table_exists:
            cur = conn.cursor()
            cur.execute(sql.SQL("SELECT count(*) FROM {}").format(
                sql.Identifier(TABLE)))
            device_count = cur.fetchone()[0]
            cur.close()
        phase, phase_detail = _index_phase(idx, inflight)
        return {
            "table": TABLE,
            "index": INDEX,
            "column": COLUMN,
            "table_exists": table_exists,
            "device_count": device_count,
            "index_present": idx is not None,
            "indisvalid": None if idx is None else idx["indisvalid"],
            "indisready": None if idx is None else idx["indisready"],
            "indislive": None if idx is None else idx["indislive"],
            "phase": phase,
            "phase_detail": phase_detail,
            "inflight": inflight,
            "duplicate_groups": dup_groups,
            "duplicate_rows": dup_rows,
            "job": _public_job(job),
        }
    finally:
        conn.close()


def _public_job(job):
    if not job:
        return None
    return {k: job.get(k) for k in (
        "state", "started_at", "finished_at", "error", "pgcode",
        "attempt", "duration_s", "note")}


# ---------------------------------------------------------------------------
# 台账数据
# --------------------------------------------------------------------------

def ensure_table(ci):
    conn = connect(ci)
    try:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    id         bigserial PRIMARY KEY,
                    device_no  text NOT NULL,
                    name       text NOT NULL,
                    location   text NOT NULL DEFAULT '',
                    created_at timestamptz NOT NULL DEFAULT now()
                )
                """
            ).format(sql.Identifier(TABLE))
        )
        cur.close()
    finally:
        conn.close()


def seed_demo(ci, rows=DEMO_ROWS):
    """灌入演示台账：rows 台编号唯一的设备，外加一对重号 DEV-DUP。

    重号是刻意留下的：第一次补建唯一索引必然失败并留下 INVALID 索引，
    供值班 DBA 演练“更正编号 → 清理 → 重试”的完整流程。
    """
    ensure_table(ci)
    conn = connect(ci)
    try:
        cur = conn.cursor()
        cur.execute(sql.SQL("TRUNCATE {} RESTART IDENTITY").format(
            sql.Identifier(TABLE)))
        # 用 generate_series 批量造数；编号 DEV-000001 ... 唯一
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {tbl} (device_no, name, location)
                SELECT 'DEV-' || lpad(g::text, 6, '0'),
                       '设备-' || g,
                       (ARRAY['一号机房', '二号机房', '三号机房', '仓库'])[mod(g, 4) + 1]
                  FROM generate_series(1, %s) AS g
                """
            ).format(tbl=sql.Identifier(TABLE)),
            (rows,),
        )
        # 一对重号：与主数据不冲突的编号，但两行互相重复
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {tbl} (device_no, name, location) VALUES
                    ('DEV-DUP-1', '待整改设备-A', '临时存放区'),
                    ('DEV-DUP-1', '待整改设备-B', '临时存放区')
                """
            ).format(tbl=sql.Identifier(TABLE)),
        )
        cur.close()
    finally:
        conn.close()


def list_devices(ci, limit=20, q=None):
    ensure_table(ci)
    conn = connect(ci)
    try:
        cur = conn.cursor()
        if q:
            like = f"%{q.strip()}%"
            cur.execute(
                sql.SQL(
                    """
                    SELECT id, device_no, name, location, created_at
                      FROM {tbl}
                     WHERE device_no ILIKE %s OR name ILIKE %s
                     ORDER BY id DESC LIMIT %s
                    """
                ).format(tbl=sql.Identifier(TABLE)),
                (like, like, limit),
            )
        else:
            cur.execute(
                sql.SQL(
                    """
                    SELECT id, device_no, name, location, created_at
                      FROM {tbl} ORDER BY id DESC LIMIT %s
                    """
                ).format(tbl=sql.Identifier(TABLE)),
                (limit,),
            )
        rows = [
            {"id": r[0], "device_no": r[1], "name": r[2],
             "location": r[3], "created_at": r[4].isoformat() if r[4] else None}
            for r in cur.fetchall()
        ]
        cur.close()
        return rows
    finally:
        conn.close()


def list_duplicates(ci):
    ensure_table(ci)
    conn = connect(ci)
    try:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                """
                SELECT {col}, count(*) AS c,
                       array_agg(id ORDER BY id) AS ids
                  FROM {tbl}
                 GROUP BY {col}
                HAVING count(*) > 1
                 ORDER BY {col}
                """
            ).format(col=sql.Identifier(COLUMN), tbl=sql.Identifier(TABLE))
        )
        out = [{"device_no": r[0], "count": r[1], "ids": list(r[2])}
               for r in cur.fetchall()]
        cur.close()
        return out
    finally:
        conn.close()


def insert_device(ci, device_no, name, location):
    """录入一台设备。索引生效后重号由数据库拒绝（23505）。"""
    device_no = (device_no or "").strip()
    name = (name or "").strip()
    if not device_no:
        raise MaintenanceError("设备编号不能为空")
    if not name:
        raise MaintenanceError("设备名称不能为空")
    ensure_table(ci)
    conn = connect(ci)
    try:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                "INSERT INTO {tbl} (device_no, name, location) "
                "VALUES (%s, %s, %s) RETURNING id"
            ).format(tbl=sql.Identifier(TABLE)),
            (device_no, name, (location or "").strip()),
        )
        new_id = cur.fetchone()[0]
        cur.close()
        return new_id
    except errors.UniqueViolation as exc:
        raise MaintenanceError(
            f"数据库拒绝写入：设备编号 {device_no} 已存在（唯一索引已生效，23505）",
            status=409, pgcode="23505") from exc
    finally:
        conn.close()


def correct_device_no(ci, device_id, new_no):
    """更正指定设备的编号（用于消除重号）。"""
    new_no = (new_no or "").strip()
    if not new_no:
        raise MaintenanceError("新编号不能为空")
    ensure_table(ci)
    conn = connect(ci)
    try:
        cur = conn.cursor()
        cur.execute(
            sql.SQL(
                "UPDATE {tbl} SET {col} = %s WHERE id = %s RETURNING id"
            ).format(tbl=sql.Identifier(TABLE), col=sql.Identifier(COLUMN)),
            (new_no, device_id),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            raise MaintenanceError(f"id={device_id} 的设备不存在", status=404)
    except errors.UniqueViolation as exc:
        raise MaintenanceError(
            f"数据库拒绝更正：编号 {new_no} 已被占用（23505）",
            status=409, pgcode="23505") from exc
    finally:
        conn.close()


def reset_demo(ci):
    """删掉本维护功能的表与固定命名索引（仅限本功能对象）。

    若库内还有针对本索引的构建在飞，直接拒绝：DDL 与在飞构建抢锁会让请求
    长时间挂起，也违背“先确认上次构建已结束再动手”的运维约定。
    """
    conn = connect(ci)
    try:
        inflight = _inflight_build_pids(conn)
        if inflight:
            raise MaintenanceError(
                "有针对 " + INDEX + " 的构建在飞"
                "（pid=" + ",".join(str(p["pid"]) for p in inflight)
                + "），请先确认其结束后再重置台账", status=409)
        cur = conn.cursor()
        cur.execute(
            sql.SQL("DROP INDEX IF EXISTS {}").format(sql.Identifier(INDEX)))
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
            sql.Identifier(TABLE)))
        cur.close()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 唯一索引补建（核心流程）
# ---------------------------------------------------------------------------

class IndexBuildManager:
    """进程内的补建任务管理；库内实况始终是最终事实来源。"""

    def __init__(self):
        self._lock = threading.Lock()
        self.job = None
        self.thread = None

    def start(self, ci):
        """启动一次补建；已在飞 / 已生效时返回当前实况，不重复开工。"""
        with self._lock:
            if self.thread is not None and self.thread.is_alive():
                raise MaintenanceError(
                    "本进程已有一次补建在执行，请等其结束后再试", status=409)

            # 先看库内实况：可能本进程重启过，库里有别人/上一世代留下的构建
            probe = connect(ci)
            try:
                idx = _index_row(probe)
                inflight = _inflight_build_pids(probe)
            finally:
                probe.close()

            if inflight:
                raise MaintenanceError(
                    "数据库内已有针对 " + INDEX + " 的构建在飞"
                    "（pid=" + ",".join(str(p["pid"]) for p in inflight)
                    + "），请先确认其结束，不要并发重复构建", status=409)
            if idx is not None and idx["indisvalid"] and not idx["not_unique"]:
                # 服务重启后的关键分支：索引已生效，直接沿用
                self.job = {
                    "state": "valid",
                    "started_at": None,
                    "finished_at": time.time(),
                    "error": None,
                    "pgcode": None,
                    "attempt": (self.job or {}).get("attempt", 0),
                    "duration_s": None,
                    "note": "库内唯一索引已生效，直接沿用，未重复构建",
                }
                return self.job

            attempt = (self.job or {}).get("attempt", 0) + 1
            self.job = {
                "state": "starting",
                "started_at": time.time(),
                "finished_at": None,
                "error": None,
                "pgcode": None,
                "attempt": attempt,
                "duration_s": None,
                "note": None,
            }
            self.thread = threading.Thread(
                target=self._run, args=(ci,), daemon=True,
                name=f"uidx-build-{attempt}")
            self.thread.start()
            return self.job

    def _run(self, ci):
        job = self.job
        t0 = time.monotonic()
        conn = None
        try:
            conn = connect(ci)
            job["state"] = "precheck"

            # 1) 确认没有在飞的构建（本进程看不到的、跨重启的场景也要防）
            self._wait_no_inflight(conn)

            # 2) 只清理“这项补建”留下的 INVALID 索引；其他对象一律不碰
            idx = _index_row(conn)
            if idx is not None and not idx["indisvalid"]:
                job["state"] = "cleanup_invalid"
                job["note"] = f"清理上次补建留下的无效索引 {INDEX}"
                cur = conn.cursor()
                cur.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(
                    sql.Identifier(INDEX)))
                cur.close()
            elif idx is not None and idx["not_unique"]:
                raise MaintenanceError(
                    f"存在同名非唯一索引 {INDEX}，非本补建产物，拒绝自动处理",
                    status=409)

            # 3) 在线补建唯一索引
            job["state"] = "building"
            job["note"] = "CREATE UNIQUE INDEX CONCURRENTLY 执行中，日常读写不阻塞"
            cur = conn.cursor()
            cur.execute(
                sql.SQL(
                    "CREATE UNIQUE INDEX CONCURRENTLY {} ON {} ({})"
                ).format(sql.Identifier(INDEX),
                         sql.Identifier(TABLE),
                         sql.Identifier(COLUMN)))
            cur.close()

            # 4) 以库内实况确认生效，而不是只相信语句返回
            idx2 = _index_row(conn)
            inflight = _inflight_build_pids(conn)
            phase, detail = _index_phase(idx2, inflight)
            if phase != "valid":
                raise MaintenanceError(
                    f"构建语句已返回但索引状态为 {phase}：{detail}", status=500)
            job["state"] = "valid"
            job["note"] = "唯一索引已生效，重复编号今后由数据库拒绝"
        except MaintenanceError as exc:
            job["state"] = "failed"
            job["error"] = exc.message
            job["pgcode"] = exc.pgcode
            self._rollback_idle(conn)
        except psycopg2.Error as exc:
            job["state"] = "failed"
            pgcode = getattr(exc, "pgcode", None)
            job["pgcode"] = pgcode
            if pgcode in DUPLICATE_PGCODES:
                job["error"] = ("存在重复设备编号，唯一索引构建失败；"
                                "数据库已留下 INVALID 索引，请先在台账中更正重号，"
                                "再点“重试补建”（系统只清理本补建的无效索引）。"
                                f"数据库原文：{_err_text(exc)}")
            else:
                job["error"] = f"构建失败（{pgcode}）：{_err_text(exc)}"
            self._rollback_idle(conn)
        except Exception as exc:  # 进程被 kill 等极端情况
            job["state"] = "failed"
            job["error"] = f"补建线程异常：{exc}"
        finally:
            job["finished_at"] = time.time()
            job["duration_s"] = round(time.monotonic() - t0, 3)
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
            with self._lock:
                self.thread = None

    @staticmethod
    def _rollback_idle(conn):
        """autocommit 下失败语句通常已自动回滚；防御性地把连接拉回空闲。"""
        if conn is None:
            return
        try:
            if conn.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                cur = conn.cursor()
                cur.execute("ROLLBACK")
                cur.close()
        except Exception:
            pass

    def _wait_no_inflight(self, conn):
        """重试前先确认上次构建已结束（只等待针对本索引名的会话）。"""
        deadline = time.monotonic() + INFLIGHT_WAIT_TIMEOUT
        inflight = _inflight_build_pids(conn)
        if not inflight:
            return
        self.job["note"] = ("等待上次构建结束后再重试：pid="
                            + ",".join(str(p["pid"]) for p in inflight))
        while time.monotonic() < deadline:
            time.sleep(INFLIGHT_POLL)
            inflight = _inflight_build_pids(conn)
            if not inflight:
                return
        raise MaintenanceError(
            "等待在飞构建结束超时（%gs），请人工确认后再重试"
            % INFLIGHT_WAIT_TIMEOUT, status=409)
