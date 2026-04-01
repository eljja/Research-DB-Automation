import atexit
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request

import services
from database import DAY_KEYS, get_db, init_db, is_debug_enabled, log_message, set_debug_enabled

KST = ZoneInfo("Asia/Seoul")

app = Flask(__name__, static_folder="static", static_url_path="")
scheduler = BackgroundScheduler(timezone=KST)


def _run_async(target, *args):
    thread = threading.Thread(target=target, args=args, daemon=True)
    thread.start()
    return thread


def _serialize_topic(row):
    topic = dict(row)
    topic["schedule"] = {
        day: {
            "enabled": bool(topic.get(f"{day}_enabled")),
            "time": topic.get(f"{day}_time"),
        }
        for day in DAY_KEYS
    }
    return topic


def scheduled_sensing_task():
    conn = get_db()
    cursor = conn.cursor()
    try:
        now = datetime.now(KST)
        day_key = now.strftime("%a").lower()[:3]
        current_time = now.strftime("%H:%M")

        cursor.execute("SELECT * FROM topics")
        topics = cursor.fetchall()
        for topic in topics:
            enabled = bool(topic[f"{day_key}_enabled"])
            scheduled_time = topic[f"{day_key}_time"]
            if enabled and scheduled_time == current_time:
                log_message(
                    "INFO",
                    f"Scheduled sensing triggered for topic '{topic['name']}' at {now.strftime('%Y-%m-%d %H:%M')} KST.",
                )
                _run_async(services.sense_scholar, topic["id"], 20, 0)
    except Exception as exc:
        log_message("ERROR", f"Scheduler tick failed: {exc}")
    finally:
        conn.close()


def _dashboard_counts(cursor):
    cursor.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS new_count,
            SUM(CASE WHEN status = 'abstract_fetched' THEN 1 ELSE 0 END) AS abstract_count,
            SUM(CASE WHEN status = 'llm_processed' THEN 1 ELSE 0 END) AS llm_count,
            SUM(CASE WHEN status LIKE '%error%' THEN 1 ELSE 0 END) AS error_count
        FROM papers
        """
    )
    row = cursor.fetchone()
    return {
        "total": row["total"] or 0,
        "new": row["new_count"] or 0,
        "abstract_fetched": row["abstract_count"] or 0,
        "llm_processed": row["llm_count"] or 0,
        "error": row["error_count"] or 0,
    }


init_db()

if not scheduler.running:
    scheduler.add_job(scheduled_sensing_task, trigger="cron", minute="*")
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown(wait=False))


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify(
        {
            "status": "ok",
            "time_kst": datetime.now(KST).isoformat(timespec="seconds"),
            "debug_enabled": is_debug_enabled(),
        }
    )


@app.route("/api/dashboard", methods=["GET"])
def dashboard():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) AS count FROM topics")
    topic_count = cursor.fetchone()["count"]
    data = {
        "topic_count": topic_count,
        "debug_enabled": is_debug_enabled(),
        "counts": _dashboard_counts(cursor),
        "time_kst": datetime.now(KST).isoformat(timespec="seconds"),
    }
    conn.close()
    return jsonify(data)


@app.route("/api/topics", methods=["GET"])
def get_topics():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM topics ORDER BY CASE WHEN name = 'NVM' THEN 0 ELSE 1 END, name")
    topics = [_serialize_topic(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(topics)


@app.route("/api/topics/<int:topic_id>", methods=["POST"])
def update_topic(topic_id):
    payload = request.get_json(force=True)
    values = [payload.get("query", "").strip()]
    set_clauses = ["query = ?", "updated_at = datetime('now', 'localtime')"]

    for day in DAY_KEYS:
        set_clauses.append(f"{day}_enabled = ?")
        set_clauses.append(f"{day}_time = ?")
        values.append(1 if payload.get(f"{day}_enabled") else 0)
        values.append((payload.get(f"{day}_time") or "").strip() or None)

    values.append(topic_id)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(f"UPDATE topics SET {', '.join(set_clauses)} WHERE id = ?", values)
    conn.commit()
    conn.close()

    log_message("INFO", f"Topic configuration saved for topic_id={topic_id}.")
    return jsonify({"status": "success"})


@app.route("/api/settings/debug", methods=["GET", "POST"])
def debug_settings():
    if request.method == "POST":
        payload = request.get_json(force=True)
        enabled = bool(payload.get("enabled"))
        set_debug_enabled(enabled)
        log_message("INFO", f"Debug logging {'enabled' if enabled else 'disabled' }.", force=True)
        return jsonify({"status": "success", "debug_enabled": enabled})

    return jsonify({"debug_enabled": is_debug_enabled()})


@app.route("/api/papers", methods=["GET"])
def get_papers():
    page = max(int(request.args.get("page", 1)), 1)
    per_page = 20
    offset = (page - 1) * per_page
    topic_id = request.args.get("topic_id")

    conn = get_db()
    cursor = conn.cursor()

    if topic_id:
        cursor.execute("SELECT COUNT(*) AS total FROM papers WHERE topic_id = ?", (topic_id,))
        total = cursor.fetchone()["total"]
        cursor.execute(
            """
            SELECT *
            FROM papers
            WHERE topic_id = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            """,
            (topic_id, per_page, offset),
        )
    else:
        cursor.execute("SELECT COUNT(*) AS total FROM papers")
        total = cursor.fetchone()["total"]
        cursor.execute(
            "SELECT * FROM papers ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (per_page, offset),
        )

    papers = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(
        {
            "papers": papers,
            "total": total,
            "page": page,
            "pages": max((total + per_page - 1) // per_page, 1),
        }
    )


@app.route("/api/papers/all", methods=["GET"])
def get_papers_for_graph():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT
            result_id, title, category, year, year_month, mechanism,
            architecture, stack, key_film, tr_structure,
            memory_window, memory_window_voltage, memory_window_ratio,
            voltage, voltage_value,
            speed, speed_seconds,
            retention, retention_year1,
            endurance, endurance_cycles,
            fetch_attempts, llm_attempts
        FROM papers
        WHERE status = 'llm_processed'
          AND COALESCE(excluded, 0) = 0
        ORDER BY created_at DESC
        """
    )
    papers = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(
        {
            "columns": [
                {"key": "year", "label": "Year"},
                {"key": "memory_window_voltage", "label": "Memory Window (V)"},
                {"key": "memory_window_ratio", "label": "Memory Window Ratio"},
                {"key": "voltage_value", "label": "Voltage"},
                {"key": "speed_seconds", "label": "Speed (s)"},
                {"key": "retention_year1", "label": "Retention @1Y (%)"},
                {"key": "endurance_cycles", "label": "Endurance (cycles)"},
                {"key": "fetch_attempts", "label": "Fetch Attempts"},
                {"key": "llm_attempts", "label": "LLM Attempts"},
            ],
            "papers": papers,
        }
    )


@app.route("/api/papers/<result_id>/reset_fetch_state", methods=["POST"])
def reset_fetch_state(result_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE papers
        SET status = 'new',
            abstract = '',
            full_text = '',
            llm_summary = '',
            mechanism = '',
            architecture = '',
            stack = '',
            key_film = '',
            tr_structure = '',
            memory_window = '',
            memory_window_voltage = NULL,
            memory_window_ratio = NULL,
            voltage = '',
            voltage_value = NULL,
            speed = '',
            speed_seconds = NULL,
            retention = '',
            retention_year1 = NULL,
            endurance = '',
            endurance_cycles = NULL,
            other_features = '',
            uniqueness = '',
            category = '',
            comparison_notes = '',
            excluded = 0,
            updated_at = datetime('now', 'localtime')
        WHERE result_id = ?
          AND (status IN ('abstract_error', 'abstract_fetched', 'llm_processed') OR COALESCE(excluded, 0) = 1)
        """,
        (result_id,),
    )
    changed = cursor.rowcount
    conn.commit()
    conn.close()

    if changed:
        log_message("INFO", f"Fetch state reset to new for result_id={result_id}.")
    return jsonify({"status": "success", "changed": changed})


@app.route("/api/papers/<result_id>/exclude", methods=["POST"])
def exclude_paper(result_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE papers
        SET excluded = 1,
            updated_at = datetime('now', 'localtime')
        WHERE result_id = ?
        """,
        (result_id,),
    )
    changed = cursor.rowcount
    conn.commit()
    conn.close()
    if changed:
        log_message("INFO", f"Paper excluded from fetch/llm pipeline: result_id={result_id}.")
    return jsonify({"status": "success", "changed": changed})


@app.route("/api/papers/<result_id>", methods=["DELETE"])
def delete_paper(result_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM papers WHERE result_id = ?", (result_id,))
    changed = cursor.rowcount
    conn.commit()
    conn.close()
    if changed:
        log_message("INFO", f"Paper deleted from DB: result_id={result_id}.")
    return jsonify({"status": "success", "changed": changed})


@app.route("/api/papers/<result_id>/manual_abstract", methods=["POST"])
def manual_abstract(result_id):
    payload = request.get_json(force=True)
    abstract = (payload.get("abstract") or "").strip()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE papers
        SET abstract = ?,
            status = CASE WHEN ? != '' THEN 'abstract_fetched' ELSE status END,
            updated_at = datetime('now', 'localtime')
        WHERE result_id = ?
        """,
        (abstract, abstract, result_id),
    )
    changed = cursor.rowcount
    conn.commit()
    conn.close()
    if changed:
        log_message("INFO", f"Manual abstract saved for result_id={result_id}.")
    return jsonify({"status": "success", "changed": changed})


@app.route("/api/logs", methods=["GET"])
def get_logs():
    limit = min(max(int(request.args.get("limit", 30)), 1), 200)
    debug_only = request.args.get("debug") == "true"
    since_id = int(request.args.get("since_id", 0))

    conn = get_db()
    cursor = conn.cursor()
    if debug_only:
        cursor.execute(
            """
            SELECT *
            FROM logs
            WHERE level = 'DEBUG' AND id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (since_id, limit),
        )
    else:
        cursor.execute(
            """
            SELECT *
            FROM logs
            WHERE level != 'DEBUG'
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )

    logs = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return jsonify(logs)


@app.route("/api/actions/sense", methods=["POST"])
def action_sense():
    payload = request.get_json(force=True) if request.data else {}
    topic_id = int(payload.get("topic_id", 1))
    start = max(int(payload.get("start", 0) or 0), 0)
    log_message("INFO", f"Manual sensing queued for topic_id={topic_id}, start={start}.")
    _run_async(services.sense_scholar, topic_id, 20, start)
    return jsonify({"status": "started"})


@app.route("/api/actions/fetch_abstracts", methods=["POST"])
def action_fetch_abstracts():
    payload = request.get_json(force=True) if request.data else {}
    limit = max(int(payload.get("limit", 10) or 10), 1)
    log_message("INFO", f"Manual abstract fetch queued for {limit} items.")
    _run_async(services.fetch_abstracts, limit)
    return jsonify({"status": "started"})


@app.route("/api/actions/process_llm", methods=["POST"])
def action_process_llm():
    payload = request.get_json(force=True) if request.data else {}
    limit = max(int(payload.get("limit", 10) or 10), 1)
    log_message("INFO", f"Manual LLM processing queued for {limit} items.")
    _run_async(services.process_llm, limit)
    return jsonify({"status": "started"})


if __name__ == "__main__":
    print("Starting Flask server on http://0.0.0.0:5000")
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
