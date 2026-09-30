from concurrent.futures import ThreadPoolExecutor

from persistence.database import Database


def test_shared_sqlite_connection_serializes_concurrent_trigger_writes(tmp_path):
    path = tmp_path / "concurrent.db"
    databases = [Database(path) for _ in range(8)]
    try:
        databases[0].execute(
            "CREATE TABLE IF NOT EXISTS trigger_write_probe "
            "(writer INTEGER NOT NULL, sequence INTEGER NOT NULL, "
            "PRIMARY KEY(writer, sequence))")

        def write_batch(writer):
            db = databases[writer]
            for sequence in range(75):
                db.execute(
                    "INSERT INTO trigger_write_probe(writer, sequence) VALUES (?, ?)",
                    (writer, sequence),
                )

        with ThreadPoolExecutor(max_workers=len(databases)) as pool:
            list(pool.map(write_batch, range(len(databases))))

        assert databases[0].scalar(
            "SELECT COUNT(*) FROM trigger_write_probe") == 8 * 75
        assert databases[0].integrity_check() == ["ok"]
    finally:
        for db in databases:
            db.close()
