"""
Batch cost estimation runner.
Processes wells without estimated_cost_p50 and writes P10/P25/P50/P75/P90.

Usage:
    python3 batch_runner.py [--limit N] [--source DATA_SOURCE]
"""

import sys, argparse, psycopg2, psycopg2.extras
from datetime import datetime

sys.path.insert(0, '/home/openclaw/.openclaw/workspace/varro')
from cost_model.estimator import estimate_well_cost, MODEL_VERSION

DB = 'postgresql://postgres:$DB_PASSWORD@db.temtptsfiksixxhbigkg.supabase.co:5432/postgres'

def log(msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)

def run_batch(limit=None, source_filter=None, batch_size=5000):
    conn = psycopg2.connect(DB, connect_timeout=15, options='-c statement_timeout=60s')
    conn.autocommit = False
    cur = conn.cursor()

    # Count how many need estimates
    where = "estimated_cost_p50 IS NULL"
    if source_filter:
        where += f" AND data_source = '{source_filter}'"
    cur.execute(f"SELECT COUNT(*) FROM wells WHERE {where}")
    total_need = cur.fetchone()[0]
    log(f"Wells needing estimates: {total_need:,}")

    total_done = 0
    while True:
        lim = min(batch_size, limit - total_done) if limit else batch_size

        cur.execute(f"""
            SELECT id, state, well_class, well_type, total_depth_ft,
                   water_depth_ft, vintage_year, status
            FROM wells
            WHERE {where}
            LIMIT {lim}
        """)
        rows = cur.fetchall()
        if not rows: break

        updates = []
        for row in rows:
            well_id, state, well_class, well_type, depth, water_depth, vintage, status = row
            est = estimate_well_cost(
                state=state, well_class=well_class, well_type=well_type,
                total_depth_ft=depth, water_depth_ft=water_depth,
                vintage_year=vintage, status=status,
            )
            updates.append((
                est['p10_usd'], est['p50_usd'], est['p90_usd'],
                MODEL_VERSION, datetime.utcnow(),
                est['p50_usd'], # aro_variance placeholder (balance_sheet - estimate)
                well_id
            ))

        psycopg2.extras.execute_values(cur, """
            UPDATE wells SET
                estimated_cost_p10 = data.p10,
                estimated_cost_p50 = data.p50,
                estimated_cost_p90 = data.p90,
                cost_model_version = data.mv,
                cost_estimated_at  = data.eat
            FROM (VALUES %s) AS data(p10, p50, p90, mv, eat, aro_v, id)
            WHERE wells.id = data.id::uuid
        """, updates, page_size=1000)

        n = len(updates)
        conn.commit()
        total_done += n
        log(f"Batch +{n:,} | total={total_done:,}")

        if limit and total_done >= limit:
            break

    log(f"DONE: {total_done:,} wells estimated (model v{MODEL_VERSION})")
    conn.close()

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--source', type=str, default=None)
    ap.add_argument('--batch', type=int, default=5000)
    args = ap.parse_args()
    run_batch(limit=args.limit, source_filter=args.source, batch_size=args.batch)
