"""Flask 入口：页面 + 运行接口。

环境变量：PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD / PORT
"""
import os
import threading

from flask import Flask, jsonify, render_template, request

import maintenance
from engine import run_experiment
from presets import PRESETS
from script_parser import ScriptError, parse_script

app = Flask(__name__)

# 所有请求共用一个沙箱库，同一时刻只允许跑一轮实验，避免互相重置数据
RUN_LOCK = threading.Lock()

# 设备台账补建唯一索引的任务管理（状态以数据库实况为准，管理器只负责任务编排）
BUILD_MANAGER = maintenance.IndexBuildManager()

DEFAULT_TIMEOUT = 5.0
MAX_TIMEOUT = 60.0


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "sandbox"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/maintenance")
def maintenance_page():
    return render_template("maintenance.html")


@app.get("/api/presets")
def get_presets():
    return jsonify(PRESETS)


@app.post("/api/run")
def run():
    body = request.get_json(force=True, silent=True) or {}
    init_sql = (body.get("init_sql") or "").strip()
    script = (body.get("script") or "").strip()
    check_sql = (body.get("check_sql") or "").strip()
    try:
        timeout_s = float(body.get("timeout_s") or DEFAULT_TIMEOUT)
    except (TypeError, ValueError):
        timeout_s = DEFAULT_TIMEOUT
    timeout_s = min(max(timeout_s, 1.0), MAX_TIMEOUT)

    if not init_sql:
        return jsonify({"error": "初始数据 SQL 不能为空"}), 400
    if not check_sql:
        return jsonify({"error": "核对查询不能为空"}), 400
    try:
        steps = parse_script(script)
    except ScriptError as exc:
        return jsonify({"error": f"脚本解析失败：{exc}"}), 400

    if not RUN_LOCK.acquire(blocking=False):
        return jsonify({"error": "已有实验正在运行，请稍候（所有会话共用一个沙箱库）"}), 409
    try:
        rounds = run_experiment(conninfo(), init_sql, steps, check_sql, timeout_s)
    finally:
        RUN_LOCK.release()
    return jsonify({"rounds": rounds, "timeout_s": timeout_s})


# ---------------------------------------------------------------------------
# 设备台账维护：在线补建设备编号唯一索引
# ---------------------------------------------------------------------------

def _mt_error(exc):
    return jsonify({"error": exc.message, "pgcode": exc.pgcode}), exc.status


@app.get("/api/maintenance/status")
def mt_status():
    snap = maintenance.get_snapshot(conninfo(), BUILD_MANAGER.job)
    return jsonify(snap)


@app.post("/api/maintenance/reset")
def mt_reset():
    """重置成演示台账（含一对重号）。仅删除本功能的固定命名对象。"""
    body = request.get_json(force=True, silent=True) or {}
    rows = maintenance.DEMO_ROWS
    if body.get("small"):
        rows = maintenance.SELFTEST_ROWS
    try:
        maintenance.reset_demo(conninfo())
        maintenance.seed_demo(conninfo(), rows=rows)
    except maintenance.MaintenanceError as exc:
        return _mt_error(exc)
    return jsonify(maintenance.get_snapshot(conninfo(), BUILD_MANAGER.job))


@app.get("/api/maintenance/devices")
def mt_devices():
    q = request.args.get("q") or None
    return jsonify(maintenance.list_devices(conninfo(), q=q))


@app.get("/api/maintenance/duplicates")
def mt_duplicates():
    return jsonify(maintenance.list_duplicates(conninfo()))


@app.post("/api/maintenance/devices")
def mt_insert_device():
    body = request.get_json(force=True, silent=True) or {}
    try:
        new_id = maintenance.insert_device(
            conninfo(), body.get("device_no"), body.get("name"),
            body.get("location"))
    except maintenance.MaintenanceError as exc:
        return _mt_error(exc)
    return jsonify({"id": new_id})


@app.post("/api/maintenance/devices/correct")
def mt_correct_device():
    body = request.get_json(force=True, silent=True) or {}
    try:
        maintenance.correct_device_no(
            conninfo(), body.get("id"), body.get("device_no"))
    except maintenance.MaintenanceError as exc:
        return _mt_error(exc)
    return jsonify({"ok": True})


@app.post("/api/maintenance/index/build")
def mt_build_index():
    """启动（或重试）在线唯一索引补建；异步执行，靠 status 接口轮询实况。"""
    try:
        BUILD_MANAGER.start(conninfo())
    except maintenance.MaintenanceError as exc:
        return _mt_error(exc)
    return jsonify(maintenance.get_snapshot(conninfo(), BUILD_MANAGER.job))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), threaded=True)
