import sqlite3, sys

path = sys.argv[1]
out = []
c = sqlite3.connect(path)
try:
    out.append("chunk_digests total rows: %d" % c.execute("SELECT COUNT(*) FROM chunk_digests").fetchone()[0])
    for r in c.execute("SELECT repo, COUNT(*), SUM(rows), SUM(CASE WHEN dirty THEN 1 ELSE 0 END) FROM chunk_digests GROUP BY repo").fetchall():
        out.append("  repo=%s blocks=%s rows=%s dirty=%s" % r)
except Exception as e:
    out.append("chunk_digests error: %s" % e)
out.append("counts (nodes, trackers, ih): %s" % str(c.execute("SELECT (SELECT COUNT(*) FROM dht_nodes WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM trackers WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM infohashes WHERE deleted_at IS NULL)").fetchone()))
out.append("peer counts (peers, archive): %s" % str(c.execute("SELECT (SELECT COUNT(*) FROM peers WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM peers_archive)").fetchone()))
c.close()
with open(r"D:\PNOS\pdc\chunk_check_out.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out))
print("done")
