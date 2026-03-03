#!/usr/bin/env python3
"""
Backtesting Report Generator for Vietnamese Trading System

This module generates comprehensive HTML and PDF reports for backtesting results
with Vietnamese market-specific analysis and visualizations.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional
import json
import base64
from io import BytesIO
import logging

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    plt.style.use('seaborn-v0_8-darkgrid')
    PLOTTING_AVAILABLE = True
except ImportError:
    logging.warning("Matplotlib/Seaborn not available - reports will be text-only")
    PLOTTING_AVAILABLE = False

class BacktestReportGenerator:
    """Generate comprehensive backtesting reports"""
    
    def __init__(self):
        self.report_data = {}
        
    def generate_html_report(self, backtest_results: Dict[str, Any], output_path: str = None) -> str:
        """Generate comprehensive HTML report"""
        
        strategy_name = backtest_results.get('strategy_name', 'Unknown Strategy')
        performance = backtest_results.get('performance', {})
        trades = backtest_results.get('trades', [])
        portfolio_values = backtest_results.get('portfolio_values', [])
        config = backtest_results.get('config', {})
        
        # Generate report sections
        html_content = self._generate_html_template()
        
        # Replace placeholders
        html_content = html_content.replace('{{STRATEGY_NAME}}', strategy_name)
        html_content = html_content.replace('{{GENERATION_TIME}}', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        html_content = html_content.replace('{{EXECUTIVE_SUMMARY}}', self._generate_executive_summary(performance))
        html_content = html_content.replace('{{PERFORMANCE_TABLE}}', self._generate_performance_table(performance))
        html_content = html_content.replace('{{TRADING_STATISTICS}}', self._generate_trading_statistics(trades, performance))
        html_content = html_content.replace('{{VIETNAMESE_MARKET_ANALYSIS}}', self._generate_vietnamese_analysis(trades, performance))
        html_content = html_content.replace('{{RISK_ANALYSIS}}', self._generate_risk_analysis(performance))
        html_content = html_content.replace('{{PORTFOLIO_CHART}}', self._generate_portfolio_chart(portfolio_values))
        html_content = html_content.replace('{{DRAWDOWN_CHART}}', self._generate_drawdown_chart(portfolio_values))
        html_content = html_content.replace('{{TRADES_TABLE}}', self._generate_trades_table(trades))
        html_content = html_content.replace('{{CONFIG_DETAILS}}', self._generate_config_details(config))
        
        # Save to file if path provided
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"HTML report saved to {output_path}")
        
        return html_content
    
    def generate_comparison_report(self, comparison_results: Dict[str, Any], output_path: str = None) -> str:
        """Generate comparison report for multiple strategies"""
        
        individual_results = comparison_results.get('individual_results', {})
        comparison = comparison_results.get('comparison', {})
        config = comparison_results.get('config', {})
        
        html_content = self._generate_comparison_template()
        
        # Replace placeholders
        html_content = html_content.replace('{{GENERATION_TIME}}', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        html_content = html_content.replace('{{STRATEGY_COUNT}}', str(len(individual_results)))
        html_content = html_content.replace('{{COMPARISON_SUMMARY}}', self._generate_comparison_summary(comparison))
        html_content = html_content.replace('{{COMPARISON_TABLE}}', self._generate_comparison_table(comparison))
        html_content = html_content.replace('{{RANKINGS}}', self._generate_rankings(comparison))
        html_content = html_content.replace('{{PERFORMANCE_CHARTS}}', self._generate_comparison_charts(individual_results))
        html_content = html_content.replace('{{INDIVIDUAL_SUMMARIES}}', self._generate_individual_summaries(individual_results))
        
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logging.info(f"Comparison report saved to {output_path}")
        
        return html_content
    
    def _generate_html_template(self) -> str:
        """Generate base HTML template"""
        return '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{STRATEGY_NAME}} - Backtesting Report</title>
    <style>
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 0; padding: 20px; 
            background-color: #f5f5f5;
        }
        .container { 
            max-width: 1200px; 
            margin: 0 auto; 
            background: white; 
            padding: 30px; 
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header { 
            text-align: center; 
            border-bottom: 3px solid #007acc; 
            padding-bottom: 20px; 
            margin-bottom: 30px;
        }
        .header h1 { 
            color: #007acc; 
            margin: 0; 
            font-size: 2.5em;
        }
        .section { 
            margin: 30px 0; 
            padding: 20px; 
            border-left: 4px solid #007acc;
            background-color: #fafafa;
        }
        .section h2 { 
            color: #333; 
            margin-top: 0;
            font-size: 1.5em;
        }
        .metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            text-align: center;
        }
        .metric-value {
            font-size: 2em;
            font-weight: bold;
            color: #007acc;
            margin: 10px 0;
        }
        .metric-label {
            color: #666;
            font-size: 0.9em;
        }
        .positive { color: #28a745; }
        .negative { color: #dc3545; }
        .neutral { color: #6c757d; }
        table { 
            width: 100%; 
            border-collapse: collapse; 
            margin: 20px 0;
            background: white;
        }
        th, td { 
            padding: 12px; 
            text-align: left; 
            border-bottom: 1px solid #ddd; 
        }
        th { 
            background-color: #007acc; 
            color: white; 
            font-weight: bold;
        }
        tr:hover { background-color: #f5f5f5; }
        .chart-container { 
            text-align: center; 
            margin: 30px 0; 
        }
        .chart-container img {
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.15);
        }
        .vietnamese-flag {
            display: inline-block;
            width: 20px;
            height: 13px;
            background: linear-gradient(to bottom, #da020e 0%, #da020e 100%);
            margin-right: 5px;
            border-radius: 2px;
        }
        .vn30-badge {
            background: #007acc;
            color: white;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 0.8em;
            margin-left: 5px;
        }
        .timestamp {
            color: #666;
            font-size: 0.9em;
            margin-top: 10px;
        }
        @media print {
            .container { box-shadow: none; }
            .chart-container { page-break-inside: avoid; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1><span class="vietnamese-flag"></span>{{STRATEGY_NAME}}</h1>
            <h2>Vietnamese Market Backtesting Report</h2>
            <div class="timestamp">Generated on {{GENERATION_TIME}}</div>
        </div>
        
        <div class="section">
            <h2>📊 Executive Summary</h2>
            {{EXECUTIVE_SUMMARY}}
        </div>
        
        <div class="section">
            <h2>📈 Performance Metrics</h2>
            {{PERFORMANCE_TABLE}}
        </div>
        
        <div class="section">
            <h2>💹 Trading Statistics</h2>
            {{TRADING_STATISTICS}}
        </div>
        
        <div class="section">
            <h2>🇻🇳 Vietnamese Market Analysis</h2>
            {{VIETNAMESE_MARKET_ANALYSIS}}
        </div>
        
        <div class="section">
            <h2>⚠️ Risk Analysis</h2>
            {{RISK_ANALYSIS}}
        </div>
        
        <div class="section">
            <h2>📊 Portfolio Performance</h2>
            {{PORTFOLIO_CHART}}
        </div>
        
        <div class="section">
            <h2>📉 Drawdown Analysis</h2>
            {{DRAWDOWN_CHART}}
        </div>
        
        <div class="section">
            <h2>🔄 Trade History</h2>
            {{TRADES_TABLE}}
        </div>
        
        <div class="section">
            <h2>⚙️ Configuration Details</h2>
            {{CONFIG_DETAILS}}
        </div>
    </div>
</body>
</html>
        '''
    
    def _generate_executive_summary(self, performance: Dict[str, Any]) -> str:
        """Generate executive summary"""
        total_return = performance.get('total_return', 0) * 100
        sharpe_ratio = performance.get('sharpe_ratio', 0)
        max_drawdown = performance.get('max_drawdown', 0) * 100
        win_rate = performance.get('win_rate', 0) * 100
        total_trades = performance.get('total_trades', 0)
        
        return_class = 'positive' if total_return > 0 else 'negative' if total_return < 0 else 'neutral'
        sharpe_class = 'positive' if sharpe_ratio > 1 else 'neutral' if sharpe_ratio > 0.5 else 'negative'
        dd_class = 'positive' if max_drawdown > -5 else 'neutral' if max_drawdown > -15 else 'negative'
        
        return f'''
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-value {return_class}">{total_return:.2f}%</div>
                <div class="metric-label">Total Return</div>
            </div>
            <div class="metric-card">
                <div class="metric-value {sharpe_class}">{sharpe_ratio:.2f}</div>
                <div class="metric-label">Sharpe Ratio</div>
            </div>
            <div class="metric-card">
                <div class="metric-value {dd_class}">{max_drawdown:.2f}%</div>
                <div class="metric-label">Max Drawdown</div>
            </div>
            <div class="metric-card">
                <div class="metric-value neutral">{win_rate:.1f}%</div>
                <div class="metric-label">Win Rate</div>
            </div>
            <div class="metric-card">
                <div class="metric-value neutral">{total_trades}</div>
                <div class="metric-label">Total Trades</div>
            </div>
        </div>
        <p>This backtesting report analyzes the performance of the trading strategy on Vietnamese market data. 
        The strategy {'outperformed' if total_return > 8 else 'underperformed' if total_return < -2 else 'performed in line with'} 
        typical Vietnamese market returns, with a Sharpe ratio of {sharpe_ratio:.2f} indicating 
        {'excellent' if sharpe_ratio > 1.5 else 'good' if sharpe_ratio > 1 else 'moderate' if sharpe_ratio > 0.5 else 'poor'} 
        risk-adjusted returns.</p>
        '''
    
    def _generate_performance_table(self, performance: Dict[str, Any]) -> str:
        """Generate performance metrics table"""
        metrics = [
            ('Total Return', f"{performance.get('total_return', 0)*100:.2f}%"),
            ('Annualized Return', f"{performance.get('annualized_return', 0)*100:.2f}%"),
            ('Volatility (Annual)', f"{performance.get('volatility', 0)*100:.2f}%"),
            ('Sharpe Ratio', f"{performance.get('sharpe_ratio', 0):.3f}"),
            ('Sortino Ratio', f"{performance.get('sortino_ratio', 0):.3f}"),
            ('Calmar Ratio', f"{performance.get('calmar_ratio', 0):.3f}"),
            ('Maximum Drawdown', f"{performance.get('max_drawdown', 0)*100:.2f}%"),
            ('Max DD Duration (days)', f"{performance.get('max_drawdown_duration', 0)}"),
            ('Value at Risk (95%)', f"{performance.get('var_95', 0)*100:.2f}%"),
            ('Conditional VaR (95%)', f"{performance.get('cvar_95', 0)*100:.2f}%"),
            ('Market Beta', f"{performance.get('beta', 0):.3f}"),
            ('Market Alpha', f"{performance.get('alpha', 0)*100:.2f}%"),
        ]
        
        table_html = '<table><tr><th>Metric</th><th>Value</th></tr>'
        for metric, value in metrics:
            table_html += f'<tr><td>{metric}</td><td>{value}</td></tr>'
        table_html += '</table>'
        
        return table_html
    
    def _generate_trading_statistics(self, trades: List[Dict], performance: Dict[str, Any]) -> str:
        """Generate trading statistics"""
        total_trades = performance.get('total_trades', 0)
        winning_trades = performance.get('winning_trades', 0)
        losing_trades = performance.get('losing_trades', 0)
        win_rate = performance.get('win_rate', 0) * 100
        avg_win = performance.get('avg_win', 0)
        avg_loss = performance.get('avg_loss', 0)
        profit_factor = performance.get('profit_factor', 0)
        
        # Analyze trades by session
        session_stats = {}
        if trades:
            sessions = ['morning', 'afternoon', 'pre_market', 'post_market', 'unknown']
            for session in sessions:
                session_trades = [t for t in trades if t.get('market_session', 'unknown') == session]
                session_stats[session] = len(session_trades)
        
        return f'''
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-value neutral">{total_trades}</div>
                <div class="metric-label">Total Trades</div>
            </div>
            <div class="metric-card">
                <div class="metric-value positive">{winning_trades}</div>
                <div class="metric-label">Winning Trades</div>
            </div>
            <div class="metric-card">
                <div class="metric-value negative">{losing_trades}</div>
                <div class="metric-label">Losing Trades</div>
            </div>
            <div class="metric-card">
                <div class="metric-value {'positive' if win_rate > 50 else 'negative'}">{win_rate:.1f}%</div>
                <div class="metric-label">Win Rate</div>
            </div>
            <div class="metric-card">
                <div class="metric-value positive">{avg_win:,.0f} VND</div>
                <div class="metric-label">Avg Win</div>
            </div>
            <div class="metric-card">
                <div class="metric-value negative">{avg_loss:,.0f} VND</div>
                <div class="metric-label">Avg Loss</div>
            </div>
            <div class="metric-card">
                <div class="metric-value {'positive' if profit_factor > 1 else 'negative'}">{profit_factor:.2f}</div>
                <div class="metric-label">Profit Factor</div>
            </div>
        </div>
        
        <h3>Trading Session Distribution</h3>
        <table>
            <tr><th>Session</th><th>Trades</th><th>Percentage</th></tr>
            {self._generate_session_table(session_stats, total_trades)}
        </table>
        '''
    
    def _generate_session_table(self, session_stats: Dict, total_trades: int) -> str:
        """Generate session distribution table"""
        table_rows = ""
        session_names = {
            'morning': '🌅 Morning (09:00-11:30)',
            'afternoon': '🌇 Afternoon (13:00-15:00)',
            'pre_market': '🌄 Pre-Market (08:30-09:00)',
            'post_market': '🌃 Post-Market (15:00-15:30)',
            'unknown': '❓ Unknown/Other'
        }
        
        for session, count in session_stats.items():
            name = session_names.get(session, session)
            percentage = (count / total_trades * 100) if total_trades > 0 else 0
            table_rows += f'<tr><td>{name}</td><td>{count}</td><td>{percentage:.1f}%</td></tr>'
        
        return table_rows
    
    def _generate_vietnamese_analysis(self, trades: List[Dict], performance: Dict[str, Any]) -> str:
        """Generate Vietnamese market specific analysis"""
        vn30_exposure = performance.get('vn30_exposure', 0) * 100
        commission_paid = performance.get('commission_paid', 0)
        tax_paid = performance.get('tax_paid', 0)
        
        # Analyze VN30 vs non-VN30 trades
        vn30_trades = 0
        non_vn30_trades = 0
        
        vn30_symbols = {
            'ACB', 'BCM', 'BID', 'BVH', 'CTG', 'EIB', 'FPT', 'GAS', 'GVR', 'HDB',
            'HPG', 'KDH', 'KHG', 'MSN', 'MWG', 'PLX', 'POW', 'SAB', 'SHB', 'SSB',
            'SSI', 'STB', 'TCB', 'TPB', 'VCB', 'VHM', 'VIC', 'VJC', 'VNM', 'VPB'
        }
        
        for trade in trades:
            if trade.get('symbol', '') in vn30_symbols:
                vn30_trades += 1
            else:
                non_vn30_trades += 1
        
        total_costs = commission_paid + tax_paid
        initial_capital = 1000000  # Default assumption
        cost_percentage = (total_costs / initial_capital) * 100
        
        return f'''
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-value neutral">{vn30_exposure:.1f}%</div>
                <div class="metric-label">VN30 Exposure</div>
            </div>
            <div class="metric-card">
                <div class="metric-value neutral">{vn30_trades}</div>
                <div class="metric-label">VN30 Trades <span class="vn30-badge">VN30</span></div>
            </div>
            <div class="metric-card">
                <div class="metric-value neutral">{non_vn30_trades}</div>
                <div class="metric-label">Non-VN30 Trades</div>
            </div>
            <div class="metric-card">
                <div class="metric-value negative">{commission_paid:,.0f} VND</div>
                <div class="metric-label">Commission Paid</div>
            </div>
            <div class="metric-card">
                <div class="metric-value negative">{tax_paid:,.0f} VND</div>
                <div class="metric-label">Securities Tax</div>
            </div>
            <div class="metric-card">
                <div class="metric-value negative">{cost_percentage:.2f}%</div>
                <div class="metric-label">Total Costs</div>
            </div>
        </div>
        
        <h3>Vietnamese Market Compliance</h3>
        <ul>
            <li><strong>Trading Hours:</strong> All trades executed during official Vietnamese market hours (09:00-15:00 UTC+7)</li>
            <li><strong>Commission Structure:</strong> VN30 stocks: 0.10%, Other stocks: 0.15%, Minimum: 1,000 VND</li>
            <li><strong>Securities Tax:</strong> 0.10% on sell orders only</li>
            <li><strong>Lot Size:</strong> All trades in multiples of 100 shares (Vietnamese standard)</li>
            <li><strong>Currency:</strong> All amounts in Vietnamese Dong (VND)</li>
        </ul>
        '''
    
    def _generate_risk_analysis(self, performance: Dict[str, Any]) -> str:
        """Generate risk analysis section"""
        max_drawdown = performance.get('max_drawdown', 0) * 100
        volatility = performance.get('volatility', 0) * 100
        var_95 = performance.get('var_95', 0) * 100
        cvar_95 = performance.get('cvar_95', 0) * 100
        sharpe_ratio = performance.get('sharpe_ratio', 0)
        sortino_ratio = performance.get('sortino_ratio', 0)
        
        risk_level = "Low" if max_drawdown > -5 else "Medium" if max_drawdown > -15 else "High"
        risk_class = "positive" if risk_level == "Low" else "neutral" if risk_level == "Medium" else "negative"
        
        return f'''
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-value {risk_class}">{risk_level}</div>
                <div class="metric-label">Risk Level</div>
            </div>
            <div class="metric-card">
                <div class="metric-value negative">{max_drawdown:.2f}%</div>
                <div class="metric-label">Maximum Drawdown</div>
            </div>
            <div class="metric-card">
                <div class="metric-value neutral">{volatility:.2f}%</div>
                <div class="metric-label">Annual Volatility</div>
            </div>
            <div class="metric-card">
                <div class="metric-value negative">{var_95:.2f}%</div>
                <div class="metric-label">Value at Risk (95%)</div>
            </div>
            <div class="metric-card">
                <div class="metric-value negative">{cvar_95:.2f}%</div>
                <div class="metric-label">Conditional VaR (95%)</div>
            </div>
        </div>
        
        <h3>Risk Assessment</h3>
        <ul>
            <li><strong>Risk Level:</strong> {risk_level} - Based on maximum drawdown of {max_drawdown:.2f}%</li>
            <li><strong>Volatility:</strong> {volatility:.2f}% annual volatility {'(above average)' if volatility > 20 else '(moderate)' if volatility > 15 else '(below average)'}</li>
            <li><strong>Risk-Adjusted Returns:</strong> Sharpe ratio of {sharpe_ratio:.2f} {'(excellent)' if sharpe_ratio > 1.5 else '(good)' if sharpe_ratio > 1 else '(moderate)' if sharpe_ratio > 0.5 else '(poor)'}</li>
            <li><strong>Downside Risk:</strong> Sortino ratio of {sortino_ratio:.2f} focuses on downside volatility</li>
            <li><strong>Tail Risk:</strong> 95% VaR suggests potential daily loss of {var_95:.2f}%</li>
        </ul>
        '''
    
    def _generate_portfolio_chart(self, portfolio_values: List) -> str:
        """Generate portfolio performance chart"""
        if not PLOTTING_AVAILABLE or not portfolio_values:
            return '<p>Chart generation not available - install matplotlib and seaborn for visualizations.</p>'
        
        try:
            # Create portfolio performance chart
            df = pd.DataFrame(portfolio_values, columns=['date', 'value'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            
            plt.figure(figsize=(12, 6))
            plt.plot(df.index, df['value'], linewidth=2, color='#007acc')
            plt.title('Portfolio Value Over Time', fontsize=16, fontweight='bold')
            plt.xlabel('Date')
            plt.ylabel('Portfolio Value (VND)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            # Convert to base64 for embedding
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            buffer.seek(0)
            image_png = buffer.getvalue()
            buffer.close()
            plt.close()
            
            graphic = base64.b64encode(image_png)
            graphic = graphic.decode('utf-8')
            
            return f'<div class="chart-container"><img src="data:image/png;base64,{graphic}" alt="Portfolio Performance Chart"></div>'
            
        except Exception as e:
            logging.error(f"Failed to generate portfolio chart: {e}")
            return '<p>Chart generation failed - see logs for details.</p>'
    
    def _generate_drawdown_chart(self, portfolio_values: List) -> str:
        """Generate drawdown analysis chart"""
        if not PLOTTING_AVAILABLE or not portfolio_values:
            return '<p>Chart generation not available - install matplotlib and seaborn for visualizations.</p>'
        
        try:
            # Calculate drawdown
            df = pd.DataFrame(portfolio_values, columns=['date', 'value'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            
            # Calculate running maximum and drawdown
            df['running_max'] = df['value'].expanding().max()
            df['drawdown'] = (df['value'] - df['running_max']) / df['running_max'] * 100
            
            plt.figure(figsize=(12, 6))
            plt.fill_between(df.index, df['drawdown'], 0, alpha=0.3, color='red')
            plt.plot(df.index, df['drawdown'], linewidth=1, color='darkred')
            plt.title('Drawdown Analysis', fontsize=16, fontweight='bold')
            plt.xlabel('Date')
            plt.ylabel('Drawdown (%)')
            plt.grid(True, alpha=0.3)
            plt.axhline(y=0, color='black', linestyle='-', alpha=0.5)
            plt.tight_layout()
            
            # Convert to base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png', dpi=150, bbox_inches='tight')
            buffer.seek(0)
            image_png = buffer.getvalue()
            buffer.close()
            plt.close()
            
            graphic = base64.b64encode(image_png)
            graphic = graphic.decode('utf-8')
            
            return f'<div class="chart-container"><img src="data:image/png;base64,{graphic}" alt="Drawdown Analysis Chart"></div>'
            
        except Exception as e:
            logging.error(f"Failed to generate drawdown chart: {e}")
            return '<p>Chart generation failed - see logs for details.</p>'
    
    def _generate_trades_table(self, trades: List[Dict]) -> str:
        """Generate trades table (show recent trades)"""
        if not trades:
            return '<p>No trades executed during the backtest period.</p>'
        
        # Show last 20 trades
        recent_trades = trades[-20:] if len(trades) > 20 else trades
        
        table_html = '''
        <table>
            <tr>
                <th>Date</th>
                <th>Symbol</th>
                <th>Side</th>
                <th>Quantity</th>
                <th>Price</th>
                <th>Commission</th>
                <th>Tax</th>
                <th>Session</th>
                <th>Strategy</th>
            </tr>
        '''
        
        for trade in recent_trades:
            date = pd.to_datetime(trade['timestamp']).strftime('%Y-%m-%d')
            symbol = trade['symbol']
            side = trade['side']
            quantity = f"{trade['quantity']:,.0f}"
            price = f"{trade['price']:,.0f}"
            commission = f"{trade['commission']:,.0f}"
            tax = f"{trade['tax']:,.0f}"
            session = trade.get('market_session', 'Unknown')
            strategy = trade.get('strategy', 'Unknown')
            
            side_class = 'positive' if side == 'BUY' else 'negative'
            
            table_html += f'''
            <tr>
                <td>{date}</td>
                <td>{symbol}</td>
                <td class="{side_class}">{side}</td>
                <td>{quantity}</td>
                <td>{price}</td>
                <td>{commission}</td>
                <td>{tax}</td>
                <td>{session}</td>
                <td>{strategy}</td>
            </tr>
            '''
        
        table_html += '</table>'
        
        if len(trades) > 20:
            table_html += f'<p><em>Showing last 20 of {len(trades)} total trades.</em></p>'
        
        return table_html
    
    def _generate_config_details(self, config: Dict[str, Any]) -> str:
        """Generate configuration details"""
        return f'''
        <table>
            <tr><th>Parameter</th><th>Value</th></tr>
            <tr><td>Start Date</td><td>{config.get('start_date', 'N/A')}</td></tr>
            <tr><td>End Date</td><td>{config.get('end_date', 'N/A')}</td></tr>
            <tr><td>Initial Capital</td><td>{config.get('initial_capital', 0):,.0f} VND</td></tr>
            <tr><td>Commission Rate</td><td>{config.get('commission_rate', 0)*100:.2f}%</td></tr>
            <tr><td>VN30 Commission Rate</td><td>{config.get('vn30_commission_rate', 0)*100:.2f}%</td></tr>
            <tr><td>Securities Tax Rate</td><td>{config.get('tax_rate', 0)*100:.2f}%</td></tr>
            <tr><td>Slippage</td><td>{config.get('slippage', 0)*100:.3f}%</td></tr>
            <tr><td>Position Sizing</td><td>{config.get('position_sizing', 'N/A')}</td></tr>
            <tr><td>Max Position Size</td><td>{config.get('max_position_size', 0)*100:.1f}%</td></tr>
        </table>
        '''
    
    def _generate_comparison_template(self) -> str:
        """Generate comparison report template"""
        return '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Strategy Comparison Report</title>
    <style>
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            margin: 0; padding: 20px; 
            background-color: #f5f5f5;
        }
        .container { 
            max-width: 1400px; 
            margin: 0 auto; 
            background: white; 
            padding: 30px; 
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header { 
            text-align: center; 
            border-bottom: 3px solid #007acc; 
            padding-bottom: 20px; 
            margin-bottom: 30px;
        }
        .header h1 { 
            color: #007acc; 
            margin: 0; 
            font-size: 2.5em;
        }
        .section { 
            margin: 30px 0; 
            padding: 20px; 
            border-left: 4px solid #007acc;
            background-color: #fafafa;
        }
        .section h2 { 
            color: #333; 
            margin-top: 0;
            font-size: 1.5em;
        }
        .positive { color: #28a745; }
        .negative { color: #dc3545; }
        .neutral { color: #6c757d; }
        table { 
            width: 100%; 
            border-collapse: collapse; 
            margin: 20px 0;
            background: white;
        }
        th, td { 
            padding: 12px; 
            text-align: left; 
            border-bottom: 1px solid #ddd; 
        }
        th { 
            background-color: #007acc; 
            color: white; 
            font-weight: bold;
        }
        tr:hover { background-color: #f5f5f5; }
        .winner { background-color: #d4edda !important; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Strategy Comparison Report</h1>
            <h2>{{STRATEGY_COUNT}} Strategies Analyzed</h2>
            <div class="timestamp">Generated on {{GENERATION_TIME}}</div>
        </div>
        
        <div class="section">
            <h2>📊 Executive Summary</h2>
            {{COMPARISON_SUMMARY}}
        </div>
        
        <div class="section">
            <h2>📈 Performance Comparison</h2>
            {{COMPARISON_TABLE}}
        </div>
        
        <div class="section">
            <h2>🏆 Rankings</h2>
            {{RANKINGS}}
        </div>
        
        <div class="section">
            <h2>📊 Performance Charts</h2>
            {{PERFORMANCE_CHARTS}}
        </div>
        
        <div class="section">
            <h2>📋 Individual Strategy Summaries</h2>
            {{INDIVIDUAL_SUMMARIES}}
        </div>
    </div>
</body>
</html>
        '''
    
    def _generate_comparison_summary(self, comparison: Dict[str, Any]) -> str:
        """Generate comparison summary"""
        avg_perf = comparison.get('avg_performance', {})
        rankings = comparison.get('rankings', {})
        
        return f'''
        <p><strong>Best Overall Return:</strong> {rankings.get('best_return', 'N/A')}</p>
        <p><strong>Best Risk-Adjusted Return:</strong> {rankings.get('best_sharpe', 'N/A')}</p>
        <p><strong>Lowest Drawdown:</strong> {rankings.get('lowest_drawdown', 'N/A')}</p>
        <p><strong>Best Win Rate:</strong> {rankings.get('best_win_rate', 'N/A')}</p>
        
        <h3>Average Performance Across All Strategies</h3>
        <ul>
            <li>Average Return: {avg_perf.get('avg_return', 0)*100:.2f}%</li>
            <li>Average Sharpe Ratio: {avg_perf.get('avg_sharpe', 0):.2f}</li>
            <li>Average Volatility: {avg_perf.get('avg_volatility', 0)*100:.2f}%</li>
            <li>Average Max Drawdown: {avg_perf.get('avg_max_drawdown', 0)*100:.2f}%</li>
        </ul>
        '''
    
    def _generate_comparison_table(self, comparison: Dict[str, Any]) -> str:
        """Generate comparison table"""
        summary_table = comparison.get('summary_table', [])
        
        if not summary_table:
            return '<p>No comparison data available.</p>'
        
        # Find best performers
        best_return = max(summary_table, key=lambda x: x['total_return'])['strategy']
        best_sharpe = max(summary_table, key=lambda x: x['sharpe_ratio'])['strategy']
        
        table_html = '''
        <table>
            <tr>
                <th>Strategy</th>
                <th>Total Return</th>
                <th>Annualized Return</th>
                <th>Volatility</th>
                <th>Sharpe Ratio</th>
                <th>Max Drawdown</th>
                <th>Win Rate</th>
                <th>Total Trades</th>
            </tr>
        '''
        
        for row in summary_table:
            winner_class = 'winner' if row['strategy'] in [best_return, best_sharpe] else ''
            
            table_html += f'''
            <tr class="{winner_class}">
                <td><strong>{row['strategy']}</strong></td>
                <td class="{'positive' if row['total_return'] > 0 else 'negative'}">{row['total_return']*100:.2f}%</td>
                <td class="{'positive' if row['annualized_return'] > 0 else 'negative'}">{row['annualized_return']*100:.2f}%</td>
                <td class="neutral">{row['volatility']*100:.2f}%</td>
                <td class="{'positive' if row['sharpe_ratio'] > 1 else 'neutral'}">{row['sharpe_ratio']:.3f}</td>
                <td class="negative">{row['max_drawdown']*100:.2f}%</td>
                <td class="{'positive' if row['win_rate'] > 0.5 else 'negative'}">{row['win_rate']*100:.1f}%</td>
                <td class="neutral">{row['total_trades']}</td>
            </tr>
            '''
        
        table_html += '</table>'
        return table_html
    
    def _generate_rankings(self, comparison: Dict[str, Any]) -> str:
        """Generate rankings section"""
        rankings = comparison.get('rankings', {})
        
        return f'''
        <table>
            <tr><th>Category</th><th>Winner</th></tr>
            <tr><td>🏆 Best Total Return</td><td><strong>{rankings.get('best_return', 'N/A')}</strong></td></tr>
            <tr><td>📊 Best Sharpe Ratio</td><td><strong>{rankings.get('best_sharpe', 'N/A')}</strong></td></tr>
            <tr><td>🛡️ Lowest Drawdown</td><td><strong>{rankings.get('lowest_drawdown', 'N/A')}</strong></td></tr>
            <tr><td>🎯 Best Win Rate</td><td><strong>{rankings.get('best_win_rate', 'N/A')}</strong></td></tr>
        </table>
        '''
    
    def _generate_comparison_charts(self, individual_results: Dict[str, Any]) -> str:
        """Generate comparison charts"""
        if not PLOTTING_AVAILABLE:
            return '<p>Chart generation not available - install matplotlib and seaborn for visualizations.</p>'
        
        return '<p>Comparison charts would be generated here with matplotlib.</p>'
    
    def _generate_individual_summaries(self, individual_results: Dict[str, Any]) -> str:
        """Generate individual strategy summaries"""
        summaries = ""
        
        for strategy_name, result in individual_results.items():
            if result is None:
                continue
            
            perf = result['performance']
            total_return = perf['total_return'] * 100
            sharpe = perf['sharpe_ratio']
            max_dd = perf['max_drawdown'] * 100
            trades = perf['total_trades']
            
            summaries += f'''
            <div style="border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px;">
                <h3>{strategy_name}</h3>
                <p><strong>Return:</strong> {total_return:.2f}% | 
                   <strong>Sharpe:</strong> {sharpe:.2f} | 
                   <strong>Max DD:</strong> {max_dd:.2f}% | 
                   <strong>Trades:</strong> {trades}</p>
            </div>
            '''
        
        return summaries