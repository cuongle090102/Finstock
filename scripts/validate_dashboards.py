#!/usr/bin/env python3
"""Validate all Grafana dashboard JSON files."""
import json
import re
import sys
import os

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), '..', 'config', 'dashboards')
DS_UID = 'P40AE60E18F02DE32'
VALID_TABLES = {
    'paper_trades', 'daily_pnl', 'backtest_results', 'optimized_parameters',
    'market_data_hot', 'market_data_1hour',
    # CTE names used in queries
    'bounds', 'buckets', 'ranges', 'equity', 'peaks', 'trades', 'groups',
    'streaks', 'candles', 'sma', 'latest', 'prev', 'base', 'last_session',
    'all_symbols', 'recent', 'avgs', 'ordered', 'total', 'bad', 'market',
    'freshness',
}

issues = []
total_panels = 0

for fname in sorted(os.listdir(DASHBOARD_DIR)):
    if not fname.endswith('.json'):
        continue
    fpath = os.path.join(DASHBOARD_DIR, fname)
    with open(fpath) as fh:
        d = json.load(fh)

    panels = d.get('panels', [])
    total_panels += len(panels)
    print("=== %s ===" % fname)
    print("  Title: %s" % d.get('title'))
    print("  UID: %s" % d.get('uid'))
    print("  Schema: %s" % d.get('schemaVersion'))
    print("  Panels: %d" % len(panels))

    tvars = d.get('templating', {}).get('list', [])
    for tv in tvars:
        print("  Template var: $%s (multi=%s)" % (tv['name'], tv.get('multi')))

    if d.get('schemaVersion') != 38:
        issues.append("%s: schemaVersion is %s, expected 38" % (fname, d.get('schemaVersion')))

    panel_ids = set()
    for p in panels:
        pid = p['id']
        if pid in panel_ids:
            issues.append("%s: Duplicate panel ID %d" % (fname, pid))
        panel_ids.add(pid)

        # Datasource check
        pds = p.get('datasource', {})
        if pds.get('uid') != DS_UID:
            issues.append("%s panel %d: Wrong datasource uid '%s'" % (fname, pid, pds.get('uid')))

        # Targets check
        targets = p.get('targets', [])
        if not targets:
            issues.append("%s panel %d: No targets" % (fname, pid))

        for tidx, t in enumerate(targets):
            tds = t.get('datasource', {})
            if tds.get('uid') != DS_UID:
                issues.append("%s panel %d target %d: Wrong datasource uid" % (fname, pid, tidx))
            sql = t.get('rawSql', '')
            if not sql:
                issues.append("%s panel %d target %d: Empty SQL" % (fname, pid, tidx))
            # Check referenced tables (exclude SQL functions/keywords after FROM)
            sql_keywords = {'now', 'timestamp', 'generate_series', 'select', 'lateral'}
            tables = re.findall(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
            tables += re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
            tables = [t for t in tables if t.lower() not in sql_keywords]
            for tbl in tables:
                if tbl.lower() not in VALID_TABLES:
                    issues.append("%s panel %d: Unknown table '%s'" % (fname, pid, tbl))
            # Check for unmatched parens
            if sql.count('(') != sql.count(')'):
                issues.append("%s panel %d: Unbalanced parentheses in SQL" % (fname, pid))

        # Grid check
        gp = p.get('gridPos', {})
        if not gp:
            issues.append("%s panel %d: Missing gridPos" % (fname, pid))
        elif gp.get('x', 0) + gp.get('w', 0) > 24:
            issues.append("%s panel %d: gridPos exceeds 24 columns (x=%d + w=%d)" % (
                fname, pid, gp.get('x', 0), gp.get('w', 0)))

        ptype = p.get('type', '?')
        title = p.get('title', '?')
        sql_short = targets[0]['rawSql'][:55] if targets else 'N/A'
        print("  Panel %d: [%-12s] %-35s | %s..." % (pid, ptype, title, sql_short))

    print()

print("=" * 60)
print("Total dashboards: %d | Total panels: %d" % (
    len([f for f in os.listdir(DASHBOARD_DIR) if f.endswith('.json')]),
    total_panels))

if issues:
    print("\nISSUES FOUND (%d):" % len(issues))
    for i in issues:
        print("  ! %s" % i)
    sys.exit(1)
else:
    print("\nALL CHECKS PASSED - No issues found")
    sys.exit(0)
