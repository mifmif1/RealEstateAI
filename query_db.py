r"""
Simple script to query the RealEstateAI Postgres/PostGIS database.
Run from project root (so the database package is on the path):

  .venv\Scripts\python.exe query_db.py          # Windows, venv
  python query_db.py                             # if venv is activated
  docker compose exec api python query_db.py    # inside API container
"""
import sys


def main():
    try:
        from database.connection import get_db_connection
    except Exception as e:
        print("Could not import database module. Run from project root.", file=sys.stderr)
        print(e, file=sys.stderr)
        sys.exit(1)

    db = get_db_connection()

    queries = [
        (
            "Table row counts",
            "SELECT 'potential_assets' AS table_name, COUNT(*) AS n FROM potential_assets "
            "UNION ALL SELECT 'comparison_assets', COUNT(*) FROM comparison_assets "
            "UNION ALL SELECT 'potential_comparison_summary', COUNT(*) FROM potential_comparison_summary"
        ),
        ("PostGIS version", "SELECT PostGIS_Version() AS postgis_version"),
    ]

    for title, sql in queries:
        print(f"\n--- {title} ---")
        try:
            rows = db.execute_query(sql)
            for row in rows:
                print(dict(row))
        except Exception as e:
            print(f"Error: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
