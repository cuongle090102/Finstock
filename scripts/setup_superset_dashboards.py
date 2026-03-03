"""
Finstock Professional Dashboard Setup
======================================
Two dashboards designed for a quantitative finance / portfolio management
workflow — not operational monitoring (that's Grafana's job).

Dashboard 1 — Strategy Alpha Research
  For the quant analyst: evaluate strategy quality, regime sensitivity,
  and performance stability. Core question: "which strategies have real
  edge and in which market conditions?"

Dashboard 2 — Execution & Risk Analytics
  For the risk manager / PM: real P&L after costs, payoff ratio, VN
  market-specific cost structure (securities tax), regime-aware execution
  quality. Core question: "after all costs, am I actually profitable?"

Usage:
    docker exec -e SUPERSET_ADMIN_PASSWORD=<pwd> finstock-superset \\
        python3 /app/scripts/setup_superset_dashboards.py
"""

import json
import os
import sys
import urllib.request
import urllib.error
from typing import Any

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "")

DS = {
    "market_data_hot":  1,
    "paper_trades":     2,
    "backtest_results": 3,
    "daily_pnl":        4,
}

# Main datetime column per dataset — required by ALL legacy chart types in Superset 3.x
# even when time_range="No filter". Legacy 'bar' is NVD3TimeSeriesViz (is_timeseries=True).
DS_GRAN = {
    1: "timestamp",  # market_data_hot
    2: "timestamp",  # paper_trades
    3: "timestamp",  # backtest_results
    4: "date",       # daily_pnl.main_dttm_col = date
}


# ── HTTP client ────────────────────────────────────────────────────────────────
class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base = base_url
        self.token = self._login(username, password)

    def _login(self, username: str, password: str) -> str:
        body = json.dumps({
            "username": username, "password": password,
            "provider": "db", "refresh": True,
        }).encode()
        req = urllib.request.Request(
            f"{self.base}/api/v1/security/login",
            data=body, headers={"Content-Type": "application/json"})
        resp = json.loads(urllib.request.urlopen(req).read())
        token = resp.get("access_token", "")
        if not token:
            raise RuntimeError(f"Login failed: {resp}")
        print(f"  Logged in as {username}")
        return token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def get(self, path: str) -> Any:
        req = urllib.request.Request(
            f"{self.base}{path}", headers=self._headers())
        return json.loads(urllib.request.urlopen(req).read())

    def post(self, path: str, body: dict) -> Any:
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"{self.base}{path}", data=data,
            headers=self._headers(), method="POST")
        try:
            return json.loads(urllib.request.urlopen(req).read())
        except urllib.error.HTTPError as e:
            raise RuntimeError(
                f"POST {path} failed {e.code}: {e.read().decode()}") from e

    def delete(self, path: str) -> None:
        req = urllib.request.Request(
            f"{self.base}{path}", headers=self._headers(), method="DELETE")
        try:
            urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise

    def create_chart(self, name: str, viz_type: str, ds_id: int,
                     params: dict, description: str = "") -> int:
        params["viz_type"] = viz_type
        params["datasource"] = f"{ds_id}__table"
        # Superset 3.x legacy charts (bar, dist_bar, table, big_number_total)
        # all require granularity_sqla even when time_range="No filter".
        # Inject it automatically so no chart ever hits the "Datetime column
        # not provided" error.
        if "granularity_sqla" not in params:
            params["granularity_sqla"] = DS_GRAN.get(ds_id, "timestamp")
        body = {
            "slice_name": name,
            "viz_type": viz_type,
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(params),
            "description": description,
        }
        result = self.post("/api/v1/chart/", body)
        cid = result.get("id") or result.get("result", {}).get("id")
        print(f"    Chart [{cid}]: {name}")
        return cid

    def create_dashboard(self, title: str, slug: str,
                         chart_ids: list, layout: dict) -> int:
        body = {
            "dashboard_title": title,
            "slug": slug,
            "published": True,
            "position_json": json.dumps(layout),
        }
        result = self.post("/api/v1/dashboard/", body)
        did = result.get("id") or result.get("result", {}).get("id")
        # Superset REST API does not auto-populate dashboard_slices from
        # position_json. Link charts explicitly via SQLAlchemy metadata DB.
        self._link_charts(did, chart_ids)
        print(f"  Dashboard [{did}]: {title}")
        return did

    def _link_charts(self, dashboard_id: int, chart_ids: list) -> None:
        """Populate dashboard_slices so charts render inside the dashboard."""
        try:
            from sqlalchemy import create_engine, text
            import os
            db_url = (
                f"postgresql://{os.getenv('DATABASE_USER','superset')}"
                f":{os.getenv('DATABASE_PASSWORD','')}"
                f"@{os.getenv('DATABASE_HOST','localhost')}"
                f":{os.getenv('DATABASE_PORT','5432')}"
                f"/{os.getenv('DATABASE_DB','superset')}"
            )
            engine = create_engine(db_url)
            # engine.begin() auto-commits on context exit (SQLAlchemy 2.x)
            with engine.begin() as conn:
                conn.execute(text(
                    "DELETE FROM dashboard_slices WHERE dashboard_id = :did"
                ), {"did": dashboard_id})
                for cid in chart_ids:
                    conn.execute(text(
                        "INSERT INTO dashboard_slices (dashboard_id, slice_id) "
                        "VALUES (:did, :cid) ON CONFLICT DO NOTHING"
                    ), {"did": dashboard_id, "cid": cid})
        except Exception as exc:
            print(f"    Warning: could not link charts via DB ({exc}). "
                  "Run manually: INSERT INTO dashboard_slices ...")

    def cleanup_finstock(self) -> None:
        """Delete all existing Finstock dashboards and charts."""
        print("  Cleaning up existing dashboards/charts...")
        resp = self.get("/api/v1/dashboard/?q=(page_size:100)")
        deleted_dash = 0
        for dash in resp.get("result", []):
            self.delete(f"/api/v1/dashboard/{dash['id']}")
            deleted_dash += 1
        if deleted_dash:
            print(f"    Deleted {deleted_dash} dashboards")

        resp = self.get("/api/v1/chart/?q=(page_size:200)")
        deleted_charts = len(resp.get("result", []))
        for chart in resp.get("result", []):
            self.delete(f"/api/v1/chart/{chart['id']}")
        print(f"    Deleted {deleted_charts} charts")


# ── Layout helpers ──────────────────────────────────────────────────────────────
def grid_layout(rows: list, chart_ids: list) -> dict:
    """
    rows: list of rows. Each row is a list of (chart_index, width, height).
    Total width per row must be <= 24 (Superset grid).
    """
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
        "GRID_ID": {
            "children": [],
            "id": "GRID_ID",
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "type": "GRID",
        },
    }
    row_ids = []
    col_counter = 0

    for r_idx, row in enumerate(rows):
        row_id = f"ROW-{r_idx}"
        row_children = []

        for chart_idx, width, height in row:
            if chart_idx >= len(chart_ids):
                continue
            col_id = f"CHART-{col_counter}"
            col_counter += 1
            pos[col_id] = {
                "children": [],
                "id": col_id,
                "meta": {
                    "chartId": chart_ids[chart_idx],
                    "height": height,
                    "sliceName": f"Chart {chart_ids[chart_idx]}",
                    "width": width,
                },
                "type": "CHART",
            }
            row_children.append(col_id)

        pos[row_id] = {
            "children": row_children,
            "id": row_id,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "type": "ROW",
        }
        row_ids.append(row_id)

    pos["GRID_ID"]["children"] = row_ids
    return pos


def m(col: str, agg: str = "SUM", label: str = "") -> dict:
    """Simple column metric."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label or f"{agg}({col})",
    }


def sql_m(expression: str, label: str) -> dict:
    """Custom SQL metric."""
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
    }


# ── Dashboard 1: Strategy Alpha Research ───────────────────────────────────────
def build_strategy_alpha(c: SupersetClient) -> int:
    """
    Answers the quant analyst's core questions:
    - Which strategy has the best risk-adjusted returns?
    - Is the edge stable over time, or is it decaying (overfitting signal)?
    - Which regime should I activate each strategy in?
    - How much statistical confidence do I have in each strategy?
    """
    print("\n[1/2] Building Strategy Alpha Research...")
    ds = DS["backtest_results"]
    ids = []

    # ── Row 1: Strategy Scorecard (full-width ranked table) ───────────────────
    # The single most important view — ranked by Sharpe, all key metrics.
    # Professionals refer to this table when deciding allocation.
    ids.append(c.create_chart(
        "Strategy Scorecard", "table", ds,
        {
            "query_mode": "aggregate",
            "groupby": ["strategy"],
            "metrics": [
                sql_m("ROUND(AVG(sharpe_ratio)::numeric, 2)",      "Avg Sharpe"),
                sql_m("ROUND(AVG(total_return * 100)::numeric, 2)", "Avg Return %"),
                sql_m("ROUND(AVG(ABS(max_drawdown) * 100)::numeric, 2)", "Avg Drawdown %"),
                sql_m("ROUND(AVG(win_rate * 100)::numeric, 1)",    "Win Rate %"),
                sql_m("ROUND(AVG(total_return * 100) / NULLIF(AVG(ABS(max_drawdown) * 100), 0), 2)",
                      "Calmar Ratio"),
                sql_m("COUNT(*)", "Backtest Runs"),
            ],
            "time_range": "No filter",
            "order_desc": True,
            "row_limit": 50,
            "table_timestamp_format": "smart_date",
            "conditional_formatting": [
                {"col": "Avg Sharpe",    "operator": ">",  "val": 1.0,
                 "colorScheme": "rgb(0,200,100)"},
                {"col": "Avg Drawdown %", "operator": ">", "val": 20.0,
                 "colorScheme": "rgb(255,80,80)"},
            ],
        },
        "Ranked by Avg Sharpe. Calmar Ratio = Return / Max Drawdown (>1.0 is professional target).",
    ))

    # ── Row 2: Three analytical bars ──────────────────────────────────────────

    # Sharpe ratio — primary selection criterion
    ids.append(c.create_chart(
        "Avg Sharpe by Strategy", "dist_bar", ds,
        {
            "metrics": [m("sharpe_ratio", "AVG", "Avg Sharpe")],
            "groupby": ["strategy"],
            "columns": [],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": False,
            "show_legend": False,
            "x_axis_label": "Strategy",
            "y_axis_label": "Avg Sharpe Ratio",
            "row_limit": 50,
        },
        ">1.0 acceptable, >2.0 excellent. Benchmark: VN30 passive = ~0.4 Sharpe.",
    ))

    # Win rate — consistency of strategy signal
    ids.append(c.create_chart(
        "Win Rate % by Strategy", "dist_bar", ds,
        {
            "metrics": [sql_m("ROUND(AVG(win_rate * 100)::numeric, 1)", "Win Rate %")],
            "groupby": ["strategy"],
            "columns": [],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": False,
            "show_legend": False,
            "x_axis_label": "Strategy",
            "y_axis_label": "Win Rate %",
            "row_limit": 50,
        },
        ">55% with good payoff ratio = robust. High win rate alone (without Sharpe) means little.",
    ))

    # Max drawdown — tail risk per strategy
    ids.append(c.create_chart(
        "Avg Max Drawdown % by Strategy", "dist_bar", ds,
        {
            "metrics": [sql_m("ROUND(AVG(ABS(max_drawdown) * 100)::numeric, 2)", "Avg Drawdown %")],
            "groupby": ["strategy"],
            "columns": [],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": False,
            "show_legend": False,
            "x_axis_label": "Strategy",
            "y_axis_label": "Avg Max Drawdown %",
            "row_limit": 50,
        },
        "Lower is better. >20% drawdown makes position sizing and client reporting very difficult.",
    ))

    # ── Row 3: Regime alpha + Sharpe stability ────────────────────────────────

    # Regime alpha matrix — which strategy works in which regime
    # This is THE chart that validates regime detection is adding value.
    ids.append(c.create_chart(
        "Return % by Strategy x Regime", "dist_bar", ds,
        {
            "metrics": [sql_m("ROUND(AVG(total_return * 100)::numeric, 2)", "Avg Return %")],
            "groupby": ["strategy"],
            "columns": ["regime"],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": False,
            "show_legend": True,
            "x_axis_label": "Strategy",
            "y_axis_label": "Avg Return %",
            "row_limit": 100,
        },
        "Each strategy should dominate in its optimal regime. "
        "Flat bars across regimes = regime detection not helping. "
        "Diverging bars = regime selection adds real alpha.",
    ))

    # Sharpe stability over time — the most important signal of overfitting
    ids.append(c.create_chart(
        "Sharpe Stability Over Time", "echarts_timeseries_line", ds,
        {
            "metrics": [m("sharpe_ratio", "AVG", "Avg Sharpe")],
            "groupby": ["strategy"],
            "x_axis": "timestamp",
            "granularity_sqla": "timestamp",
            "time_grain_sqla": "P1W",
            "time_range": "No filter",
            "adhoc_filters": [],
            "show_legend": True,
            "rich_tooltip": True,
            "row_limit": 50000,
        },
        "A monotonically declining Sharpe is the #1 signal of overfitting or regime change. "
        "Stable or improving Sharpe = real edge.",
    ))

    # ── Row 4: Research depth + full results table ────────────────────────────

    # Backtest volume — statistical confidence gauge
    ids.append(c.create_chart(
        "Backtest Volume by Strategy & Regime", "dist_bar", ds,
        {
            "metrics": [sql_m("COUNT(*)", "Runs")],
            "groupby": ["strategy"],
            "columns": ["regime"],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": True,
            "show_legend": True,
            "x_axis_label": "Strategy",
            "y_axis_label": "Number of Backtest Runs",
            "row_limit": 100,
        },
        "Statistical confidence: need >30 runs per regime for reliable Sharpe estimates. "
        "Low run count = don't trust the metrics.",
    ))

    # Raw results table — sortable, filterable drill-down
    ids.append(c.create_chart(
        "Backtest Results — Full Detail", "table", ds,
        {
            "query_mode": "raw",
            "columns": [
                "timestamp", "strategy", "regime",
                "sharpe_ratio", "total_return", "max_drawdown",
                "win_rate", "total_trades",
            ],
            "metrics": [],
            "groupby": [],
            "time_range": "No filter",
            "row_limit": 200,
            "table_timestamp_format": "smart_date",
        },
        "Full backtest run log — click any column header to sort.",
    ))

    # Layout: scorecard full-width tall, 3 equal bars, wide regime + stability, volume + tall table
    layout = grid_layout(
        rows=[
            [(0, 24, 66)],                              # Scorecard — tall so all rows readable
            [(1, 8, 34), (2, 8, 34), (3, 8, 34)],      # Sharpe | Win Rate | Drawdown — equal thirds
            [(4, 16, 38), (5, 8, 38)],                  # Regime Alpha wider | Sharpe Stability
            [(6, 8, 52), (7, 16, 52)],                  # Research Depth | Detail Table — tall bottom
        ],
        chart_ids=ids,
    )
    return c.create_dashboard(
        "Strategy Alpha Research", "finstock-alpha", ids, layout)


# ── Dashboard 2: Execution & Risk Analytics ────────────────────────────────────
def build_execution_risk(c: SupersetClient) -> int:
    """
    Answers the risk manager / PM's core questions:
    - After all costs, am I actually making money?
    - What is the payoff ratio — are wins bigger than losses?
    - Does regime-aware execution improve per-trade P&L?
    - VN30 vs standard stocks: where is real edge with lower cost drag?
    - Is transaction cost (0.20-0.25% round-trip) eating my alpha?
    """
    print("\n[2/2] Building Execution & Risk Analytics...")
    pt  = DS["paper_trades"]
    pnl = DS["daily_pnl"]
    ids = []

    # ── Row 1: 4 KPIs ─────────────────────────────────────────────────────────

    ids.append(c.create_chart(
        "Net Realized P&L", "big_number_total", pt,
        {
            "metric": sql_m("SUM(pnl)", "Net P&L (VND)"),
            "subheader": "Total realized P&L — after entry/exit",
            "time_range": "No filter",
            "number_format": ",.0f",
        },
    ))

    ids.append(c.create_chart(
        "Total Transaction Costs", "big_number_total", pt,
        {
            "metric": sql_m("SUM(commission + tax)", "Total Costs (VND)"),
            "subheader": "Commission + Securities Tax (0.10–0.25% round-trip in VN)",
            "time_range": "No filter",
            "number_format": ",.0f",
        },
    ))

    # Payoff ratio: the most important single metric for a trading system.
    # P&L > 0 even with <50% win rate is possible IF payoff ratio is >2.
    ids.append(c.create_chart(
        "Payoff Ratio (Avg Win / Avg Loss)", "big_number_total", pt,
        {
            "metric": sql_m(
                "ROUND("
                "  AVG(CASE WHEN pnl > 0 THEN pnl END) / "
                "  NULLIF(ABS(AVG(CASE WHEN pnl < 0 THEN pnl END)), 0)"
                ", 2)",
                "Payoff Ratio",
            ),
            "subheader": "Must be >1.0 to survive long-term. >2.0 = strong edge.",
            "time_range": "No filter",
            "number_format": ".2f",
        },
    ))

    ids.append(c.create_chart(
        "Live Win Rate %", "big_number_total", pt,
        {
            "metric": sql_m(
                "ROUND("
                "  COUNT(CASE WHEN pnl > 0 THEN 1 END) * 100.0 / "
                "  NULLIF(COUNT(*), 0)"
                ", 1)",
                "Win Rate %",
            ),
            "subheader": "% of closed trades profitable. 55%+ with >1.5 payoff = healthy.",
            "time_range": "No filter",
            "number_format": ".1f",
        },
    ))

    # ── Row 2: Portfolio equity curve (full-width) ────────────────────────────
    # The fundamental chart — smooth upward slope = consistent alpha.
    # daily_pnl has main_dttm_col="date", so use "date" as x_axis.
    ids.append(c.create_chart(
        "Portfolio Equity Curve", "echarts_timeseries_line", pnl,
        {
            "metrics": [m("portfolio_value", "MAX", "Portfolio Value (VND)")],
            "groupby": [],
            "x_axis": "date",
            "granularity_sqla": "date",
            "time_grain_sqla": "P1D",
            "time_range": "No filter",
            "adhoc_filters": [],
            "show_legend": False,
            "rich_tooltip": True,
            "area": True,
            "row_limit": 1000,
        },
        "Daily portfolio value. Smooth upward = consistent alpha. "
        "Sharp drawdowns = risk events to investigate. "
        "Flat line = no trades (check paper trading service).",
    ))

    # ── Row 3: P&L attribution + cost structure ───────────────────────────────

    # Net P&L by strategy — which strategies earn in live trading
    ids.append(c.create_chart(
        "Net P&L by Strategy", "dist_bar", pt,
        {
            "metrics": [sql_m("SUM(pnl)", "Net P&L (VND)")],
            "groupby": ["strategy"],
            "columns": [],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": False,
            "show_legend": False,
            "x_axis_label": "Strategy",
            "y_axis_label": "Net P&L (VND)",
            "row_limit": 20,
        },
        "Real P&L attribution. Cross-reference with backtest Sharpe — "
        "a high-Sharpe backtest that earns nothing live = overfitting or market change.",
    ))

    # Cost drag by strategy — is cost eating alpha for high-frequency strategies?
    ids.append(c.create_chart(
        "Transaction Cost Drag by Strategy", "dist_bar", pt,
        {
            "metrics": [sql_m("SUM(commission + tax)", "Total Cost (VND)")],
            "groupby": ["strategy"],
            "columns": [],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": False,
            "show_legend": False,
            "x_axis_label": "Strategy",
            "y_axis_label": "Cost (VND)",
            "row_limit": 20,
        },
        "High-frequency strategies pay more cost. "
        "If cost > net P&L for a strategy, reduce trade frequency.",
    ))

    # ── Row 4: Regime quality + VN30 analysis ────────────────────────────────

    # Regime execution quality — validates that regime detection improves execution
    ids.append(c.create_chart(
        "Avg Trade P&L by Strategy x Regime", "dist_bar", pt,
        {
            "metrics": [sql_m("ROUND(AVG(pnl)::numeric, 0)", "Avg P&L per Trade (VND)")],
            "groupby": ["strategy"],
            "columns": ["regime"],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": False,
            "show_legend": True,
            "x_axis_label": "Strategy",
            "y_axis_label": "Avg P&L per Trade (VND)",
            "row_limit": 100,
        },
        "Key validation: each strategy should earn MORE in its optimal regime. "
        "If a trend-following strategy earns equally in trending and ranging markets, "
        "regime detection is not helping live execution.",
    ))

    # VN30 vs standard — where is the real edge?
    # VN30: 0.10% commission, high liquidity, institutional activity
    # Standard: 0.15% commission, lower liquidity, more retail
    ids.append(c.create_chart(
        "VN30 vs Standard Stocks Performance", "dist_bar", pt,
        {
            "metrics": [
                sql_m("COUNT(*)",                                              "Trade Count"),
                sql_m("ROUND(AVG(pnl)::numeric, 0)",                          "Avg P&L (VND)"),
                sql_m("ROUND(AVG((commission + tax) / total_cost * 100)::numeric, 3)",
                      "Avg Cost %"),
            ],
            "groupby": ["is_vn30"],
            "columns": [],
            "time_range": "No filter",
            "adhoc_filters": [],
            "bar_stacked": False,
            "show_legend": True,
            "x_axis_label": "Is VN30 Stock",
            "row_limit": 10,
        },
        "VN30 = lower commission (0.10%) + higher liquidity. "
        "Compare avg P&L and cost % to decide where to concentrate capital.",
    ))

    # Layout: KPIs tall, equity curve full-width, equal analysis rows, tall bottom
    layout = grid_layout(
        rows=[
            [(0, 6, 22), (1, 6, 22), (2, 6, 22), (3, 6, 22)],    # 4 KPIs — taller for readability
            [(4, 24, 44)],                                           # Equity curve full width
            [(5, 12, 36), (6, 12, 36)],                             # P&L | Cost equal halves
            [(7, 14, 52), (8, 10, 52)],                             # Regime (wider) | VN30 — tall bottom
        ],
        chart_ids=ids,
    )
    return c.create_dashboard(
        "Execution & Risk Analytics", "finstock-risk", ids, layout)


# ── Main ────────────────────────────────────────────────────────────────────────
def main() -> None:
    if not PASSWORD:
        print("ERROR: SUPERSET_ADMIN_PASSWORD env var not set.")
        sys.exit(1)

    print("=" * 60)
    print("Finstock Professional Dashboard Setup")
    print("=" * 60)

    c = SupersetClient(BASE_URL, USERNAME, PASSWORD)
    c.cleanup_finstock()

    ids = []
    ids.append(build_strategy_alpha(c))
    ids.append(build_execution_risk(c))

    print("\n" + "=" * 60)
    print(f"✓ {len(ids)} professional dashboards created")
    print(f"  Dashboard IDs: {ids}")
    print(f"  http://localhost:8088/dashboard/list/")
    print("=" * 60)


if __name__ == "__main__":
    main()
