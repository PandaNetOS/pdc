import sqlite3, time

conn = sqlite3.connect(r'file:D:/test/pdc/data/pdc.db?mode=ro', uri=True)
cur = conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_dht_nodes_ip_port_expr'")
print('IDX:', cur.fetchone())
keys = [r[0] for r in conn.execute("SELECT (ip || ':' || port) FROM dht_nodes LIMIT 500").fetchall()]
ph = ','.join(['?'] * len(keys))
sql_in = "SELECT count(*) FROM dht_nodes WHERE (ip || ':' || port) IN (" + ph + ") AND deleted_at IS NULL"
cur = conn.execute('EXPLAIN QUERY PLAN ' + sql_in, keys)
for row in cur.fetchall():
    print('PLAN:', row)
t0 = time.time()
n = conn.execute(sql_in, keys).fetchone()[0]
print('IN-500 rows:', n, 'elapsed:', round(time.time() - t0, 2), 's')
t0 = time.time()
cnt = 0
for r in conn.execute("SELECT (ip || ':' || port) FROM dht_nodes LIMIT 500").fetchall():
    ip, port = r[0].rsplit(':', 1)
    row = conn.execute('SELECT 1 FROM dht_nodes WHERE ip=? AND port=? AND deleted_at IS NULL', (ip, int(port))).fetchone()
    if row:
        cnt += 1
print('PK-500 rows:', cnt, 'elapsed:', round(time.time() - t0, 3), 's')
