import sqlite3, sys

path = sys.argv[1]
c = sqlite3.connect(path)
print("chunk_digests total rows:", c.execute("SELECT COUNT(*) FROM chunk_digests").fetchone()[0])
print("by repo (repo, blocks, rows_sum, dirty_blocks):")
for r in c.execute("SELECT repo, COUNT(*), SUM(rows), SUM(CASE WHEN dirty THEN 1 ELSE 0 END) FROM chunk_digests GROUP BY repo").fetchall():
    print("  ", r)
print("table counts (nodes, trackers, ih):",
      c.execute("SELECT (SELECT COUNT(*) FROM dht_nodes WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM trackers WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM infohashes WHERE deleted_at IS NULL)").fetchone())
print("peer counts (peers, archive):",
      c.execute("SELECT (SELECT COUNT(*) FROM peers WHERE deleted_at IS NULL),(SELECT COUNT(*) FROM peers_archive)").fetchone())
c.close()
