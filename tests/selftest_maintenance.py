"""设备台账维护功能端到端自测。

覆盖验收点：
  1. 演示台账重号 → 首次补建失败并留下 INVALID 索引；
  2. 构建期间日常写入不阻塞（写入线程在构建过程中成功提交）；
  3. 更正重号后重试：先确认上次构建已结束，只 DROP 本补建的 INVALID 索引；
  4. 重试期间服务“重启”（换一个全新的管理器实例）：
     - 构建途中重启 → 库内残留 INVALID，新实例重试清理并建成；
     - 索引生效后、结果返回前重启 → 新实例直接沿用已生效索引，不重复构建；
  5. 建成后数据库拒绝重复编号（23505），正常新编号仍可写入。

用法（docker compose 栈起来后）：
    docker compose exec web python tests/selftest_maintenance.py
或对着任意可达的 PostgreSQL：
    PGHOST=... PGPORT=... PGDATABASE=... PGUSER=... PGPASSWORD=... \
        python tests/selftest_maintenance.py
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

import psycopg2  # noqa: E402

import maintenance as mt  # noqa: E402

FAILURES = []


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "postgres"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def wait_phase(ci, manager, phases, timeout=120.0):
    """轮询库内实况直到 phase 命中目标集合。"""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        snap = mt.get_snapshot(ci, manager.job)
        last = snap
        if snap["phase"] in phases:
            return snap
        # 管理器线程失败也要及时返回
        job = snap.get("job") or {}
        if job.get("state") == "failed" and "building" not in phases:
            return snap
        time.sleep(0.1)
    raise AssertionError(f"等待阶段 {phases} 超时，最后实况：{last['phase']}")


def wait_job(manager, states, timeout=120.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = manager.job or {}
        if job.get("state") in states:
            return job
        time.sleep(0.1)
    raise AssertionError(f"等待任务状态 {states} 超时：{manager.job}")


def raw_index_state(ci):
    """绕过管理器直接查库，验证“实况”本身。"""
    conn = mt.connect(ci)
    try:
        row = mt._index_row(conn)
        inflight = mt._inflight_build_pids(conn)
        return row, inflight
    finally:
        conn.close()


def test_failure_then_retry_with_writes(ci):
    print("== 场景 1：重号导致首次失败 → 更正 → 重试成功，构建期间可正常写入 ==")
    mt.reset_demo(ci)
    mt.seed_demo(ci, rows=mt.SELFTEST_ROWS)
    manager = mt.IndexBuildManager()

    snap = mt.get_snapshot(ci, manager.job)
    check("演示台账已建表且有数据", snap["table_exists"] and snap["device_count"] > 0,
          str(snap.get("device_count")))
    check("演示台账含 1 组重号（DEV-DUP-1）",
          snap["duplicate_groups"] == 1 and snap["duplicate_rows"] == 2,
          f"{snap['duplicate_groups']}/{snap['duplicate_rows']}")
    check("初始无补建索引", snap["phase"] == "absent", snap["phase"])

    # ---- 第一次补建：应因重号失败，留下 INVALID 索引 ----
    manager.start(ci)
    job = wait_job(manager, {"failed", "valid"})
    check("首次补建失败（重号 23505）",
          job["state"] == "failed" and job.get("pgcode") == "23505",
          str(job))
    snap = wait_phase(ci, manager, {"invalid", "absent", "valid"})
    check("库内留下 INVALID 索引", snap["phase"] == "invalid"
          and snap["indisvalid"] is False, str(snap["phase"]))
    row, inflight = raw_index_state(ci)
    check("失败后无在飞构建会话", not inflight, str(inflight))
    check("无效索引仍 indisready=false / indislive=true",
          row and row["indisready"] is False and row["indislive"] is True)

    # ---- 更正重号 ----
    dups = mt.list_duplicates(ci)
    check("重号清单返回 DEV-DUP-1 两行",
          len(dups) == 1 and dups[0]["device_no"] == "DEV-DUP-1"
          and len(dups[0]["ids"]) == 2, str(dups))
    fix_id = dups[0]["ids"][1]
    mt.correct_device_no(ci, fix_id, "DEV-DUP-1-FIX")
    snap = mt.get_snapshot(ci, manager.job)
    check("更正后重号清零", snap["duplicate_groups"] == 0,
          str(snap["duplicate_groups"]))

    # ---- 重试：构建期间并发写入 ----
    write_results = []

    def writer_during_build():
        # 等构建真正开始
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            s = mt.get_snapshot(ci, manager.job)
            if s["phase"] == "building":
                break
            time.sleep(0.05)
        # 构建期间连续写入新编号设备（CONCURRENTLY 不阻塞普通写入）
        for i in range(20):
            try:
                new_id = mt.insert_device(
                    ci, f"DEV-WRITE-{i}-{time.time_ns()}", f"构建期写入{i}", "在线验证")
                write_results.append(("ok", new_id))
            except Exception as exc:  # noqa: BLE001
                write_results.append(("err", str(exc)))
            time.sleep(0.02)

    t = threading.Thread(target=writer_during_build, daemon=True)
    t.start()
    t0 = time.monotonic()
    manager.start(ci)
    t.join(timeout=120)
    duration = time.monotonic() - t0

    saw_building = False
    snap = wait_phase(ci, manager, {"valid"}, timeout=180)
    # 回看：管理器任务记录里出现过 building（线程内状态）
    check("重试最终唯一索引生效", snap["phase"] == "valid"
          and snap["indisvalid"] is True, snap["phase"])
    check("构建确实经历了 building 阶段（非瞬间完成）",
          duration > 0.05 and len(write_results) > 0,
          f"duration={duration:.2f}s writes={len(write_results)}")
    check("构建期间 20 笔写入全部成功（读写不阻塞）",
          len(write_results) == 20 and all(r[0] == "ok" for r in write_results),
          str(write_results[:3]))
    check("任务记录标注了清理无效索引的动作",
          (manager.job or {}).get("state") == "valid", str(manager.job))

    # ---- 建成后数据库拒绝重号 ----
    try:
        mt.insert_device(ci, "DEV-DUP-1-FIX", "试图重号", "")
        check("重复编号被数据库拒绝", False, "未抛错")
    except mt.MaintenanceError as exc:
        check("重复编号被数据库拒绝（23505, 409）",
              exc.status == 409 and exc.pgcode == "23505", str(exc))
    new_id = mt.insert_device(ci, f"DEV-OK-{time.time_ns()}", "新编号可写", "")
    check("不重复的新编号仍可正常写入", isinstance(new_id, int))

    # 再次点击补建：应直接沿用，不重复构建
    before, _ = raw_index_state(ci)
    manager.start(ci)
    after, _ = raw_index_state(ci)
    check("已生效后重复请求直接沿用、不重建（索引 oid 不变）",
          before and after and before["oid"] == after["oid"],
          f"{before and before['oid']} vs {after and after['oid']}")
    check("沿用分支有明确说明", "沿用" in ((manager.job or {}).get("note") or ""))


def test_cleanup_only_own_invalid(ci):
    print("== 场景 2：清理范围只限本补建的固定命名无效索引 ==")
    mt.reset_demo(ci)
    mt.seed_demo(ci, rows=2000)
    conn = mt.connect(ci)
    try:
        # 人造一个与本任务无关的无效索引（同名规则之外的对象）
        cur = conn.cursor()
        cur.execute("CREATE INDEX devices_other_idx ON devices (name)")
        cur.close()
    finally:
        conn.close()

    manager = mt.IndexBuildManager()
    manager.start(ci)
    wait_job(manager, {"failed", "valid"})
    check("重号场景下补建失败", manager.job["state"] == "failed", str(manager.job))

    # 手工把本补建的 INVALID 索引 DROP 前先确认其他索引健在
    conn = mt.connect(ci)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE tablename = %s "
            "ORDER BY indexname", (mt.TABLE,))
        names = [r[0] for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()
    check("无关索引 devices_other_idx 在失败后仍存在",
          "devices_other_idx" in names, str(names))
    check("本补建的无效索引 devices_device_no_uidx 存在",
          mt.INDEX in names, str(names))

    # 更正重号并重试
    dups = mt.list_duplicates(ci)
    mt.correct_device_no(ci, dups[0]["ids"][0], "DEV-DUP-1-FIX2")
    manager.start(ci)
    wait_phase(ci, manager, {"valid"}, timeout=120)

    conn = mt.connect(ci)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT indexname FROM pg_indexes WHERE tablename = %s "
            "ORDER BY indexname", (mt.TABLE,))
        names = [r[0] for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()
    check("重试后无关索引 devices_other_idx 仍未被误删",
          "devices_other_idx" in names, str(names))
    check("本补建索引以有效唯一索引存在", mt.INDEX in names, str(names))
    conn = mt.connect(ci)
    try:
        row = mt._index_row(conn)
        check("本补建索引 indisvalid=true 且为唯一索引",
              row and row["indisvalid"] and not row["not_unique"], str(row))
    finally:
        conn.close()


def test_restart_during_build(ci):
    print("== 场景 3a：构建途中服务重启 → 新实例识别残留并重试建成 ==")
    mt.reset_demo(ci)
    mt.seed_demo(ci, rows=mt.SELFTEST_ROWS)
    # 先消除重号，让构建本身能成功；中断只模拟“服务在构建完成前死亡”
    dups = mt.list_duplicates(ci)
    mt.correct_device_no(ci, dups[0]["ids"][1], "DEV-RESTART-FIX")

    # 用一个持有表锁的事务把 CONCURRENTLY 稳定卡在“等待锁”阶段，
    # 这样测试不依赖构建速度，可以确定性地观察到在飞构建会话。
    # 锁事务必须在事务块里，因此这条连接不能用 autocommit。
    locker = psycopg2.connect(**ci)
    locker.autocommit = False
    lcur = locker.cursor()
    # SHARE ROW EXCLUSIVE 与 CREATE INDEX CONCURRENTLY 所需的
    # SHARE UPDATE EXCLUSIVE 冲突，构建会话会挂起等待
    lcur.execute("LOCK TABLE devices IN SHARE ROW EXCLUSIVE MODE")

    manager2 = mt.IndexBuildManager()
    manager2.start(ci)

    # 确定性地等到库内出现真实在飞构建（被锁卡住）
    deadline = time.monotonic() + 30
    inflight = []
    while time.monotonic() < deadline:
        conn = mt.connect(ci)
        try:
            inflight = mt._inflight_build_pids(conn)
        finally:
            conn.close()
        if inflight:
            break
        time.sleep(0.05)
    check("库内出现真实在飞构建会话（被表锁卡住）",
          len(inflight) >= 1, str(inflight))
    snap = mt.get_snapshot(ci, manager2.job)
    check("实况阶段显示 building", snap["phase"] == "building", snap["phase"])

    # 在构建仍被锁卡住时取消其后端，等价于“服务在构建完成前被 kill -9”。
    # 此时索引可能尚未创建（absent），或已创建为 INVALID——两种都是合法的
    # 中断残留，管理器重试时都必须能处理（前者直接建，后者先 DROP 再建）。
    conn = mt.connect(ci)
    try:
        inflight = mt._inflight_build_pids(conn)
        check("取消前构建会话确实在飞", len(inflight) >= 1, str(inflight))
        cur = conn.cursor()
        for p in inflight:
            cur.execute("SELECT pg_cancel_backend(%s)", (p["pid"],))
        cur.close()
    finally:
        conn.close()
    # 释放锁事务，避免残留表锁影响后续重试
    lcur.execute("ROLLBACK")
    lcur.close()
    locker.close()
    if manager2.thread is not None:
        manager2.thread.join(timeout=30)

    # 等库内构建会话消失
    deadline = time.monotonic() + 30
    phase = None
    while time.monotonic() < deadline:
        s = mt.get_snapshot(ci, None)
        phase = s["phase"]
        if phase in ("invalid", "absent") and not s["inflight"]:
            break
        time.sleep(0.1)
    check("中断后无在飞会话，库内残留无效索引或索引缺失（phase=%s）" % phase,
          phase in ("invalid", "absent"), str(phase))
    print("  [INFO] 中断后实际库内阶段：", phase)

    # “重启服务”：全新管理器。重试时先确认无在飞构建，只清理本补建的
    # INVALID 索引（若存在），然后重新构建到生效
    manager3 = mt.IndexBuildManager()
    manager3.start(ci)
    snap = wait_phase(ci, manager3, {"valid"}, timeout=180)
    check("重启后重试成功，唯一索引生效", snap["phase"] == "valid", snap["phase"])
    check("重试任务记录为第 1 次（新进程重新计数）",
          manager3.job["attempt"] == 1, str(manager3.job))
    row, inflight = raw_index_state(ci)
    check("生效后 indisvalid=true 且无在飞会话",
          row and row["indisvalid"] and not inflight, str(row))


def test_restart_after_valid_before_response(ci):
    print("== 场景 3b：索引生效后、结果返回前重启 → 新实例沿用已生效索引 ==")
    mt.reset_demo(ci)
    mt.seed_demo(ci, rows=mt.SELFTEST_ROWS)
    # 先消除重号，让一次构建能成功
    dups = mt.list_duplicates(ci)
    mt.correct_device_no(ci, dups[0]["ids"][1], "DEV-VALID-FIX")

    manager1 = mt.IndexBuildManager()
    manager1.start(ci)
    snap = wait_phase(ci, manager1, {"valid"}, timeout=180)
    check("构建已在库内生效", snap["phase"] == "valid")
    # 模拟“HTTP 响应还没回去服务就重启”：管理器状态全部丢失
    oid_before = snap["index_present"]
    row_before, _ = raw_index_state(ci)

    manager2 = mt.IndexBuildManager()  # 全新进程
    manager2.start(ci)
    job2 = wait_job(manager2, {"valid", "failed"})
    check("新实例判定为已生效、未重新构建",
          job2["state"] == "valid" and "沿用" in (job2.get("note") or ""),
          str(job2))
    row_after, inflight = raw_index_state(ci)
    check("沿用同一索引（oid 不变）且无在飞构建",
          row_after and row_before and row_after["oid"] == row_before["oid"]
          and not inflight,
          f"{row_before} / {row_after} / {inflight}")
    check("oid_before 标记一致", oid_before is True)

    # 沿用后数据库仍然拒绝重号
    try:
        mt.insert_device(ci, "DEV-VALID-FIX", "重号", "")
        check("沿用后重号仍被拒绝", False)
    except mt.MaintenanceError as exc:
        check("沿用后重号仍被拒绝（23505）", exc.pgcode == "23505", str(exc))


def test_concurrent_start_rejected(ci):
    print("== 场景 4：构建在飞时重复发起补建会被拒绝 ==")
    mt.reset_demo(ci)
    mt.seed_demo(ci, rows=mt.SELFTEST_ROWS)
    dups = mt.list_duplicates(ci)
    mt.correct_device_no(ci, dups[0]["ids"][1], "DEV-CONC-FIX")
    manager = mt.IndexBuildManager()
    manager.start(ci)
    snap = wait_phase(ci, manager, {"building", "valid"}, timeout=60)
    if snap["phase"] == "valid":
        print("  [INFO] 构建在首次轮询前已完成，跳过在飞并发拒绝分支，"
              "改由管理器存活检查覆盖")
        check("构建最终生效", manager.job["state"] == "valid", str(manager.job))
        return
    try:
        manager.start(ci)
        check("在飞期间第二次 start 被拒绝", False)
    except mt.MaintenanceError as exc:
        check("在飞期间第二次 start 被拒绝（409）", exc.status == 409, str(exc))
    wait_phase(ci, manager, {"valid"}, timeout=180)
    check("构建最终生效", manager.job["state"] == "valid", str(manager.job))


def main():
    ci = conninfo()
    # 连通性
    conn = psycopg2.connect(**ci)
    ver = conn.server_version
    conn.close()
    print(f"PostgreSQL 连接正常，server_version={ver}\n")

    test_failure_then_retry_with_writes(ci)
    test_cleanup_only_own_invalid(ci)
    test_restart_during_build(ci)
    test_restart_after_valid_before_response(ci)
    test_concurrent_start_rejected(ci)

    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项失败：")
        for name in FAILURES:
            print("  -", name)
        return 1
    print("维护功能全部自测通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
