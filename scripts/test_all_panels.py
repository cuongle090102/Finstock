#!/usr/bin/env python3
"""Test every panel query from every Grafana dashboard against live TimescaleDB.

Properly expands Grafana macros and template variables with real values.
"""
import json
import os
import subprocess
import re
import sys

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), '..', 'config', 'dashboards')
PSQL_CMD = ['docker.exe', 'exec', 'finstock-postgres', 'psql', '-U', 'finstock_user',
            '-d', 'finstock_market_data', '--no-align', '-c']

# Real values for variable substitution
SYMBOL = 'VNM'
STRATEGY = 'Momentum'
TIME_FROM = '2025-01-01'
TIME_TO = '2026-12-31'


def expand_macros(sql):
    """Replace Grafana macros and template variables with real SQL values."""
    # $__timeFilter(timestamp) or $__timeFilter(m.timestamp) etc.
    sql = re.sub(
        r'\$__timeFilter\(([^)]+)\)',
        lambda m: "%s BETWEEN '%s' AND '%s'" % (m.group(1), TIME_FROM, TIME_TO),
        sql
    )
    # Template variable: $symbol (single or multi)
    # Replace IN ($symbol) with IN ('VNM')
    sql = re.sub(r"IN\s*\(\$symbol\)", "IN ('%s')" % SYMBOL, sql)
    # Replace symbol='$symbol' or symbol = '$symbol'
    sql = re.sub(r"symbol\s*=\s*'\$symbol'", "symbol = '%s'" % SYMBOL, sql)
    # Replace bare $symbol in string context like "as \"$symbol\""
    sql = sql.replace('$symbol', SYMBOL)

    # Template variable: $strategy
    sql = re.sub(r"IN\s*\(\$strategy\)", "IN ('%s')" % STRATEGY, sql)
    sql = re.sub(r"strategy\s*=\s*'\$strategy'", "strategy = '%s'" % STRATEGY, sql)
    sql = sql.replace('$strategy', STRATEGY)

    # Remove broken all-value patterns that may remain
    sql = sql.replace("('%s' = '$__all' OR " % SYMBOL, "(")
    sql = sql.replace("('%s' = '$__all' OR " % STRATEGY, "(")

    return sql


def run_query(sql):
    """Execute SQL via docker psql, return (success, output, row_count)."""
    try:
        result = subprocess.run(
            PSQL_CMD + [sql],
            capture_output=True, text=True, timeout=30
        )
        output = result.stdout.strip()
        error = result.stderr.strip()
        if result.returncode != 0:
            return False, error, 0
        # Count rows from output (--no-align format)
        row_match = re.search(r'\((\d+) rows?\)', output)
        rows = int(row_match.group(1)) if row_match else 0
        return True, output, rows
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT (30s)", 0
    except Exception as e:
        return False, str(e), 0


def main():
    total = 0
    passed = 0
    failed = 0
    no_data = 0
    errors = []
    no_data_panels = []

    for fname in sorted(os.listdir(DASHBOARD_DIR)):
        if not fname.endswith('.json'):
            continue
        fpath = os.path.join(DASHBOARD_DIR, fname)
        with open(fpath) as fh:
            d = json.load(fh)

        title = d.get('title', fname)
        panels = d.get('panels', [])
        print("=" * 75)
        print("  %s  (%d panels)" % (title, len(panels)))
        print("=" * 75)

        for p in panels:
            pid = p['id']
            ptitle = p.get('title', '(untitled)')
            ptype = p.get('type', '?')
            targets = p.get('targets', [])

            for tidx, t in enumerate(targets):
                total += 1
                raw_sql = t.get('rawSql', '')
                test_sql = expand_macros(raw_sql)

                # Wrap to limit output
                if 'LIMIT' not in test_sql.upper():
                    test_sql = test_sql.rstrip(';') + ' LIMIT 10'

                ok, output, rows = run_query(test_sql)

                ref = t.get('refId', '?')
                label = "P%d [%-12s] %-45s ref=%s" % (pid, ptype, ptitle[:45], ref)

                if ok:
                    passed += 1
                    if rows == 0:
                        no_data += 1
                        no_data_panels.append((fname, pid, ptitle, ptype))
                        print("  [OK  ] %s -> %d rows (no data)" % (label, rows))
                    else:
                        print("  [DATA] %s -> %d rows" % (label, rows))
                else:
                    failed += 1
                    errors.append((fname, pid, ptitle, ptype, ref, output, raw_sql))
                    print("  [FAIL] %s" % label)
                    for el in output.split('\n')[:2]:
                        print("         %s" % el)

        print()

    # Summary
    print("=" * 75)
    print("  SUMMARY: %d queries | %d DATA | %d NO-DATA | %d FAILED" % (
        total, passed - no_data, no_data, failed))
    print("=" * 75)

    if errors:
        print("\n  FAILED QUERIES (%d):" % len(errors))
        print("  " + "-" * 73)
        for fname, pid, ptitle, ptype, ref, err, raw in errors:
            print("  %s -> Panel %d: %s [%s] ref=%s" % (fname, pid, ptitle, ptype, ref))
            for line in err.split('\n')[:3]:
                print("    ERROR: %s" % line)
            print("    SQL: %s" % raw[:120])
            print()

    if no_data_panels:
        print("\n  NO-DATA PANELS (%d):" % len(no_data_panels))
        print("  " + "-" * 73)
        for fname, pid, ptitle, ptype in no_data_panels:
            print("  %s -> Panel %d: %s [%s]" % (fname, pid, ptitle, ptype))

    print()
    sys.exit(1 if failed > 0 else 0)


if __name__ == '__main__':
    main()
