import sqlite3

DB_PATH = "research.db"
DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def _table_columns(cursor, table_name):
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cursor.fetchall()}


def _ensure_columns(cursor, table_name, definitions):
    existing = _table_columns(cursor, table_name)
    for column_name, column_definition in definitions.items():
        if column_name not in existing:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}")


def _ensure_indexes(cursor):
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_topic_id ON papers(topic_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_status ON papers(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_papers_created_at ON papers(created_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_logs_level_id ON logs(level, id DESC)")


def _migrate_topics(cursor):
    topic_columns = _table_columns(cursor, "topics")
    legacy_schedule_columns = {f"schedule_{day}" for day in DAY_KEYS}

    if not topic_columns:
        cursor.execute(
            """
            CREATE TABLE topics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                query TEXT NOT NULL,
                mon_enabled INTEGER DEFAULT 0,
                mon_time TEXT,
                tue_enabled INTEGER DEFAULT 0,
                tue_time TEXT,
                wed_enabled INTEGER DEFAULT 0,
                wed_time TEXT,
                thu_enabled INTEGER DEFAULT 0,
                thu_time TEXT,
                fri_enabled INTEGER DEFAULT 0,
                fri_time TEXT,
                sat_enabled INTEGER DEFAULT 0,
                sat_time TEXT,
                sun_enabled INTEGER DEFAULT 0,
                sun_time TEXT,
                created_at DATETIME DEFAULT (datetime('now', 'localtime')),
                updated_at DATETIME DEFAULT (datetime('now', 'localtime'))
            )
            """
        )
        topic_columns = _table_columns(cursor, "topics")

    _ensure_columns(
        cursor,
        "topics",
        {
            "created_at": "DATETIME",
            "updated_at": "DATETIME",
            **{f"{day}_enabled": "INTEGER DEFAULT 0" for day in DAY_KEYS},
            **{f"{day}_time": "TEXT" for day in DAY_KEYS},
        },
    )

    topic_columns = _table_columns(cursor, "topics")
    if legacy_schedule_columns.intersection(topic_columns):
        for day in DAY_KEYS:
            legacy_column = f"schedule_{day}"
            if legacy_column in topic_columns:
                cursor.execute(
                    f"""
                    UPDATE topics
                    SET {day}_time = COALESCE({day}_time, {legacy_column}),
                        {day}_enabled = CASE
                            WHEN {day}_enabled IS NULL OR {day}_enabled = 0
                            THEN CASE WHEN {legacy_column} IS NOT NULL AND TRIM({legacy_column}) != '' THEN 1 ELSE 0 END
                            ELSE {day}_enabled
                        END
                    WHERE {legacy_column} IS NOT NULL AND TRIM({legacy_column}) != ''
                    """
                )


def _migrate_papers(cursor):
    paper_columns = _table_columns(cursor, "papers")
    if not paper_columns:
        cursor.execute(
            """
            CREATE TABLE papers (
                result_id TEXT PRIMARY KEY,
                topic_id INTEGER,
                title TEXT,
                link TEXT,
                snippet TEXT,
                pub_info TEXT,
                publication_summary TEXT,
                abstract TEXT,
                full_text TEXT,
                llm_summary TEXT,
                mechanism TEXT,
                architecture TEXT,
                stack TEXT,
                key_film TEXT,
                tr_structure TEXT,
                year REAL,
                year_month TEXT,
                memory_window TEXT,
                memory_window_voltage REAL,
                memory_window_ratio REAL,
                voltage TEXT,
                voltage_value REAL,
                speed TEXT,
                speed_seconds REAL,
                retention TEXT,
                retention_year1 REAL,
                endurance TEXT,
                endurance_cycles REAL,
                other_features TEXT,
                uniqueness TEXT,
                category TEXT,
                comparison_notes TEXT,
                excluded INTEGER DEFAULT 0,
                fetch_attempts INTEGER DEFAULT 0,
                llm_attempts INTEGER DEFAULT 0,
                status TEXT DEFAULT 'new',
                created_at DATETIME DEFAULT (datetime('now', 'localtime')),
                updated_at DATETIME DEFAULT (datetime('now', 'localtime')),
                FOREIGN KEY(topic_id) REFERENCES topics(id)
            )
            """
        )

    _ensure_columns(
        cursor,
        "papers",
        {
            "publication_summary": "TEXT",
            "llm_summary": "TEXT",
            "year_month": "TEXT",
            "category": "TEXT",
            "comparison_notes": "TEXT",
            "stack": "TEXT",
            "key_film": "TEXT",
            "memory_window_voltage": "REAL",
            "memory_window_ratio": "REAL",
            "voltage_value": "REAL",
            "speed_seconds": "REAL",
            "retention_year1": "REAL",
            "endurance_cycles": "REAL",
            "excluded": "INTEGER DEFAULT 0",
            "fetch_attempts": "INTEGER DEFAULT 0",
            "llm_attempts": "INTEGER DEFAULT 0",
            "created_at": "DATETIME",
            "updated_at": "DATETIME",
        },
    )

    paper_columns = _table_columns(cursor, "papers")
    if "comparison_dram_flash_logic" in paper_columns and "comparison_notes" in paper_columns:
        cursor.execute(
            """
            UPDATE papers
            SET comparison_notes = COALESCE(comparison_notes, comparison_dram_flash_logic)
            WHERE comparison_dram_flash_logic IS NOT NULL
              AND TRIM(comparison_dram_flash_logic) != ''
            """
        )


def _migrate_logs(cursor):
    log_columns = _table_columns(cursor, "logs")
    if not log_columns:
        cursor.execute(
            """
            CREATE TABLE logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT (datetime('now', 'localtime')),
                level TEXT,
                message TEXT,
                raw_data TEXT
            )
            """
        )

    _ensure_columns(cursor, "logs", {"raw_data": "TEXT"})


def _ensure_settings_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at DATETIME DEFAULT (datetime('now', 'localtime'))
        )
        """
    )


def _ensure_default_topic(cursor):
    cursor.execute('SELECT id FROM topics WHERE name = "NVM"')
    if cursor.fetchone():
        return

    cursor.execute(
        """
        INSERT INTO topics (
            name, query,
            mon_enabled, mon_time,
            wed_enabled, wed_time
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "NVM",
            '("charge trap" OR "electrochemical" OR "ferro" OR "resistive" OR "phase change" OR "floating gate" OR "nano cluster" OR "nano crystal" OR "selector only" OR "chalcogenide") AND ("memory" OR "NVM" OR "non-volatile" OR "non volatile" OR "SSD" OR "Flash" OR "NAND" OR "VNAND" OR "NOR")',
            1,
            "07:00",
            1,
            "13:00",
        ),
    )


def get_setting(key, default=None):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key, value):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES (?, ?, datetime('now', 'localtime'))
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        """,
        (key, str(value)),
    )
    conn.commit()
    conn.close()


def is_debug_enabled():
    return get_setting("debug_enabled", "0") == "1"


def set_debug_enabled(enabled):
    set_setting("debug_enabled", "1" if enabled else "0")


def init_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")

    _migrate_topics(cursor)
    _migrate_papers(cursor)
    _migrate_logs(cursor)
    _ensure_settings_table(cursor)
    _ensure_indexes(cursor)
    _ensure_default_topic(cursor)

    cursor.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES ('debug_enabled', '0', datetime('now', 'localtime'))
        ON CONFLICT(key) DO NOTHING
        """
    )

    cursor.execute(
        """
        UPDATE topics
        SET created_at = COALESCE(created_at, datetime('now', 'localtime')),
            updated_at = COALESCE(updated_at, datetime('now', 'localtime'))
        """
    )
    cursor.execute(
        """
        UPDATE papers
        SET created_at = COALESCE(created_at, datetime('now', 'localtime')),
            updated_at = COALESCE(updated_at, datetime('now', 'localtime'))
        """
    )

    conn.commit()
    conn.close()


def log_message(level, message, raw_data=None, force=False):
    if level == "DEBUG" and not force and not is_debug_enabled():
        return

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO logs (level, message, raw_data) VALUES (?, ?, ?)",
        (level, message, raw_data),
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    init_db()
    print("Database initialized.")
