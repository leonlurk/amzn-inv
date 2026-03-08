"""
Pura Vitalia - Amazon Analytics Dashboard
Streamlit-based interface for the Amazon reporting pipeline.
"""
import sys
import os
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path so we can import src
sys.path.insert(0, str(Path(__file__).parent))

from sp_api.base import Marketplaces
from src.config import Config
from src.sp_api_client import SPAPIClient, SalesData, get_mock_sales_data
from src.ads_api_client import AmazonAdsClient, AdsData, get_mock_ads_data
from src.inventory_client import InventoryClient, InventoryItem, get_mock_inventory
from src.orders_client import OrdersClient, DailyOrders, get_mock_daily_orders
from src.finances_client import FinancesClient, ReconciliationResult, GL_ACCOUNTS
from src.metrics import CombinedMetrics, aggregate_weekly
from src.output import export_to_csv, export_to_google_sheets

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Pura Vitalia Analytics",
    page_icon="PV",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
    }

    /* KPI cards */
    .kpi-card {
        background: linear-gradient(145deg, #1e1e30, #252540);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 20px 24px;
        text-align: center;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        transition: transform 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
    }
    .kpi-label {
        color: #8b8fa3;
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1.2px;
        margin-bottom: 6px;
    }
    .kpi-value {
        color: #ffffff;
        font-size: 1.8rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .kpi-delta-up {
        color: #00e676;
        font-size: 0.82rem;
        font-weight: 500;
        margin-top: 4px;
    }
    .kpi-delta-down {
        color: #ff5252;
        font-size: 0.82rem;
        font-weight: 500;
        margin-top: 4px;
    }

    /* Section headers */
    .section-header {
        color: #e0e0e0;
        font-size: 1.1rem;
        font-weight: 600;
        margin: 32px 0 16px 0;
        padding-bottom: 8px;
        border-bottom: 2px solid rgba(99, 102, 241, 0.4);
        letter-spacing: 0.5px;
    }

    /* Brand header */
    .brand-header {
        background: linear-gradient(90deg, rgba(99,102,241,0.15), rgba(168,85,247,0.1), transparent);
        border-left: 4px solid #6366f1;
        padding: 16px 24px;
        border-radius: 0 12px 12px 0;
        margin-bottom: 24px;
    }
    .brand-title {
        color: #ffffff;
        font-size: 1.6rem;
        font-weight: 700;
        letter-spacing: 0.5px;
    }
    .brand-subtitle {
        color: #8b8fa3;
        font-size: 0.85rem;
        margin-top: 2px;
    }

    /* Status badge */
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .status-live {
        background: rgba(0,230,118,0.15);
        color: #00e676;
        border: 1px solid rgba(0,230,118,0.3);
    }
    .status-mock {
        background: rgba(255,193,7,0.15);
        color: #ffc107;
        border: 1px solid rgba(255,193,7,0.3);
    }

    /* Inventory table */
    .inv-table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        border-radius: 12px;
        overflow: hidden;
    }
    .inv-table th {
        background: rgba(99,102,241,0.15);
        color: #a5b4fc;
        padding: 10px 14px;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 600;
    }
    .inv-table td {
        padding: 10px 14px;
        color: #e0e0e0;
        border-bottom: 1px solid rgba(255,255,255,0.05);
        font-size: 0.9rem;
    }
    .inv-table tr:last-child td {
        border-bottom: none;
        font-weight: 700;
        background: rgba(99,102,241,0.08);
    }

    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #12121f, #1a1a2e);
    }

    /* Hide default streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Metric container fix */
    [data-testid="stMetric"] {
        background: linear-gradient(145deg, #1e1e30, #252540);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 16px;
    }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------
COLORS = {
    'primary': '#6366f1',       # Indigo
    'secondary': '#a78bfa',     # Light purple
    'accent': '#22d3ee',        # Cyan
    'success': '#00e676',       # Green
    'warning': '#ffc107',       # Amber
    'danger': '#ff5252',        # Red
    'organic': '#00e676',       # Green
    'ppc': '#6366f1',           # Indigo
    'spend': '#ff5252',         # Red
    'revenue': '#22d3ee',       # Cyan
    'bg_chart': 'rgba(0,0,0,0)',
    'grid': 'rgba(255,255,255,0.06)',
    'text': '#e0e0e0',
    'text_muted': '#8b8fa3',
}

CHART_LAYOUT = dict(
    paper_bgcolor=COLORS['bg_chart'],
    plot_bgcolor=COLORS['bg_chart'],
    font=dict(color=COLORS['text'], family='Inter, sans-serif'),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor=COLORS['grid'], showgrid=True),
    yaxis=dict(gridcolor=COLORS['grid'], showgrid=True),
    legend=dict(
        bgcolor='rgba(0,0,0,0)',
        font=dict(size=11),
        orientation='h',
        yanchor='bottom', y=1.02,
        xanchor='right', x=1,
    ),
    hoverlabel=dict(bgcolor='#1e1e30', font_size=12),
)


# ---------------------------------------------------------------------------
# Data fetching (cached)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=900, show_spinner=False)
def load_sales(start_str: str, end_str: str, use_mock: bool):
    """Fetch sales data only. Cached for 15 minutes."""
    start_date = datetime.strptime(start_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_str, '%Y-%m-%d')
    days = (end_date - start_date).days + 1

    if use_mock:
        return get_mock_sales_data(start_date, days)
    if Config.validate_sp_api():
        return SPAPIClient().fetch_sales_data(start_date, end_date)
    return get_mock_sales_data(start_date, days)


@st.cache_data(ttl=900, show_spinner=False)
def load_ads(start_str: str, end_str: str, use_mock: bool):
    """Fetch ads data only. Cached for 15 minutes. Timeout after 120s."""
    start_date = datetime.strptime(start_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_str, '%Y-%m-%d')
    days = (end_date - start_date).days + 1

    if use_mock:
        return get_mock_ads_data(start_date, days)
    if Config.validate_ads_api():
        return AmazonAdsClient().fetch_ads_data(start_date, end_date, max_wait_seconds=120)
    return get_mock_ads_data(start_date, days)


def _empty_ads(sales_data):
    """Return zero-filled ads data matching sales dates."""
    return [AdsData(date=s.date, spend=0, attributed_orders=0,
                    attributed_revenue=0, attributed_units=0,
                    clicks=0, impressions=0, acos=0, roas=0)
            for s in sales_data]


@st.cache_data(ttl=300, show_spinner=False)
def load_inventory(use_mock: bool):
    """Fetch inventory snapshot. Cached for 5 minutes."""
    if use_mock:
        return get_mock_inventory()
    if Config.validate_sp_api():
        client = InventoryClient()
        return client.fetch_inventory()
    return get_mock_inventory()


@st.cache_data(ttl=300, show_spinner=False)
def load_orders(start_str: str, end_str: str, use_mock: bool):
    """Fetch daily orders. Cached for 5 minutes."""
    start_date = datetime.strptime(start_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_str, '%Y-%m-%d')
    days = (end_date - start_date).days + 1

    if use_mock:
        return get_mock_daily_orders(start_date, days)
    if Config.validate_sp_api():
        client = OrdersClient()
        return client.fetch_orders_by_day(start_date, end_date)
    return get_mock_daily_orders(start_date, days)


@st.cache_data(ttl=600, show_spinner=False)
def load_settlements(use_mock: bool, _v: int = 2):
    """Fetch recent settlement reports. Cached for 10 minutes."""
    if use_mock:
        return []
    if Config.validate_sp_api():
        client = FinancesClient()
        reports = client.list_settlement_reports(max_results=5)
        settlements = []
        for report in reports:
            try:
                doc_id = report.get('reportDocumentId')
                if doc_id:
                    tsv = client.download_settlement_report(doc_id)
                    summary = client.parse_settlement_tsv(tsv)
                    settlements.append(summary)
            except Exception as e:
                print(f"Error downloading settlement: {e}")
        return settlements
    return []


def build_metrics(sales_data, ads_data):
    """Combine sales + ads into CombinedMetrics."""
    sales_by_date = {s.date: s for s in sales_data}
    ads_by_date = {a.date: a for a in ads_data}
    all_dates = sorted(set(sales_by_date.keys()) | set(ads_by_date.keys()))

    metrics = []
    for date in all_dates:
        sales = sales_by_date.get(date)
        ads = ads_by_date.get(date)
        if sales and ads:
            metrics.append(CombinedMetrics.from_data(sales, ads))
        elif sales:
            zero_ads = AdsData(date=date, spend=0, attributed_orders=0,
                               attributed_revenue=0, attributed_units=0,
                               clicks=0, impressions=0, acos=0, roas=0)
            metrics.append(CombinedMetrics.from_data(sales, zero_ads))
    return metrics


# ---------------------------------------------------------------------------
# KPI Card helper
# ---------------------------------------------------------------------------
def kpi_card(label: str, value: str, delta: str = "", delta_type: str = "up"):
    delta_class = "kpi-delta-up" if delta_type == "up" else "kpi-delta-down"
    delta_html = f'<div class="{delta_class}">{delta}</div>' if delta else ''
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 16px 0;">
        <div style="color:#6366f1; font-size:1.6rem; font-weight:800; letter-spacing:2px;">PV</div>
        <div style="color:#fff; font-size:1.1rem; font-weight:700; margin-top:4px;">Pura Vitalia</div>
        <div style="color:#8b8fa3; font-size:0.75rem;">Amazon Analytics</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # Date range
    st.markdown("##### Date Range")
    default_end = datetime.now() - timedelta(days=1)
    default_start = default_end - timedelta(days=6)

    col_s, col_e = st.columns(2)
    with col_s:
        start_date = st.date_input("Start", value=default_start, key="start")
    with col_e:
        end_date = st.date_input("End", value=default_end, key="end")

    st.markdown("---")

    # Data source
    st.markdown("##### Data Source")
    use_mock = st.toggle("Use Mock Data", value=Config.USE_SANDBOX, help="Use simulated data for testing")

    api_status = "mock" if use_mock else "live"
    badge_class = "status-mock" if use_mock else "status-live"
    badge_text = "MOCK DATA" if use_mock else "LIVE API"
    st.markdown(f'<span class="status-badge {badge_class}">{badge_text}</span>', unsafe_allow_html=True)

    st.markdown("---")

    # Options
    st.markdown("##### Include")
    include_inventory = st.checkbox("Inventory Snapshot", value=True)
    include_orders = st.checkbox("Daily Orders", value=True)

    st.markdown("---")

    # Actions
    st.markdown("##### Actions")
    refresh = st.button("Refresh Data", use_container_width=True, type="primary")
    if refresh:
        st.cache_data.clear()
        st.rerun()

    export_csv_btn = st.button("Export CSV", use_container_width=True)

    if Config.GOOGLE_SHEET_ID:
        push_sheets_btn = st.button("Push to Google Sheets", use_container_width=True)
    else:
        push_sheets_btn = False

    st.markdown("---")
    st.markdown(
        '<div style="color:#555; font-size:0.7rem; text-align:center;">Knightstower LLC<br>v1.0</div>',
        unsafe_allow_html=True
    )


# ---------------------------------------------------------------------------
# Main content
# ---------------------------------------------------------------------------

# Brand header
st.markdown("""
<div class="brand-header">
    <div class="brand-title">Amazon Analytics Dashboard</div>
    <div class="brand-subtitle">Pura Vitalia — Sales, Advertising & Inventory Intelligence</div>
</div>
""", unsafe_allow_html=True)

# Convert dates
start_str = start_date.strftime('%Y-%m-%d')
end_str = end_date.strftime('%Y-%m-%d')

# ---------- Load data ----------
metrics = []
inventory = None
daily_orders = None

ads_warning = False

with st.status("Loading data from Amazon APIs...", expanded=True) as load_status:
    # 1. Sales (SP-API — usually 30-60s)
    st.write("Fetching sales report...")
    try:
        sales_data = load_sales(start_str, end_str, use_mock)
        st.write(f"Sales: {len(sales_data)} days")
    except Exception as e:
        st.write(f"Sales error: {e}")

    # 2. Ads (Ads API — can take 2+ min, timeout at 120s)
    st.write("Fetching ads report... (timeout: 2 min)")
    try:
        ads_data = load_ads(start_str, end_str, use_mock)
        st.write(f"Ads: {len(ads_data)} days")
    except Exception as e:
        st.write(f"Ads timed out or failed: {e} — showing dashboard without ads data")
        ads_data = _empty_ads(sales_data) if sales_data else []
        ads_warning = True

    if sales_data:
        metrics = build_metrics(sales_data, ads_data)

    # 3. Inventory
    if include_inventory:
        st.write("Fetching inventory snapshot...")
        try:
            inventory = load_inventory(use_mock)
            st.write(f"Inventory: {len(inventory)} products")
        except Exception as e:
            st.write(f"Inventory error: {e}")

    # 4. Orders
    if include_orders:
        st.write("Fetching daily orders...")
        try:
            daily_orders = load_orders(start_str, end_str, use_mock)
            st.write(f"Orders: {len(daily_orders)} days")
        except Exception as e:
            st.write(f"Orders error: {e}")

    if metrics:
        load_status.update(label="All data loaded!", state="complete", expanded=False)
    else:
        load_status.update(label="Failed to load data", state="error", expanded=True)

if ads_warning:
    st.warning("Ads data unavailable (report timed out after 2 min). Sales, inventory, orders, and settlements are all loaded. Ads metrics will show as $0.")


# ---------- Handle export actions ----------
if export_csv_btn and metrics:
    path = export_to_csv(metrics, inventory=inventory, daily_orders=daily_orders)
    st.sidebar.success(f"CSV exported: {path}")

if push_sheets_btn and metrics:
    sid = Config.GOOGLE_SHEET_ID
    sname = Config.GOOGLE_SHEET_NAME
    with st.spinner("Pushing to Google Sheets..."):
        success = export_to_google_sheets(metrics, sid, sname,
                                          inventory=inventory, daily_orders=daily_orders)
    if success:
        st.sidebar.success(f"Google Sheet updated: {sname}")
    else:
        st.sidebar.error("Failed to update Google Sheets")


# ---------- KPI Cards ----------
if metrics:
    total_sales = sum(m.total_sales for m in metrics)
    total_spend = sum(m.ppc_spend for m in metrics)
    total_units = sum(m.total_units for m in metrics)
    total_orders_sum = sum(m.total_orders for m in metrics)
    avg_roas = sum(m.roas for m in metrics) / len(metrics)
    avg_tacos = sum(m.tacos for m in metrics) / len(metrics)
    avg_acos = sum(m.acos for m in metrics) / len(metrics)
    total_available = sum(i.fulfillable for i in inventory) if inventory else 0

    cols = st.columns(6)
    with cols[0]:
        kpi_card("Total Revenue", f"${total_sales:,.2f}", f"{len(metrics)} days")
    with cols[1]:
        kpi_card("Total Units", f"{total_units:,}", f"{total_orders_sum} orders")
    with cols[2]:
        kpi_card("PPC Spend", f"${total_spend:,.2f}", f"ACoS {avg_acos:.1f}%",
                 "down" if avg_acos > 30 else "up")
    with cols[3]:
        kpi_card("ROAS", f"{avg_roas:.2f}x", f"Avg across period",
                 "up" if avg_roas >= 2 else "down")
    with cols[4]:
        kpi_card("TACoS", f"{avg_tacos:.1f}%", f"Ad spend vs total sales",
                 "up" if avg_tacos < 15 else "down")
    with cols[5]:
        kpi_card("Inventory", f"{total_available:,}", "Units available" if inventory else "N/A")

    st.markdown("<br>", unsafe_allow_html=True)

    # ---------- Charts Row 1: Sales + Media ----------
    st.markdown('<div class="section-header">SALES PERFORMANCE</div>', unsafe_allow_html=True)

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        # Revenue breakdown
        dates = [m.date for m in metrics]
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=dates,
            y=[m.organic_sales for m in metrics],
            name='Organic Sales',
            marker_color=COLORS['organic'],
            opacity=0.85,
        ))
        fig.add_trace(go.Bar(
            x=dates,
            y=[m.ppc_sales for m in metrics],
            name='PPC Sales',
            marker_color=COLORS['ppc'],
            opacity=0.85,
        ))
        fig.add_trace(go.Scatter(
            x=dates,
            y=[m.total_sales for m in metrics],
            name='Total Sales',
            mode='lines+markers',
            line=dict(color=COLORS['accent'], width=2),
            marker=dict(size=6),
        ))
        fig.update_layout(
            **CHART_LAYOUT,
            title=dict(text='Revenue Breakdown', font=dict(size=14)),
            barmode='stack',
            yaxis_title='Revenue ($)',
            height=380,
        )
        st.plotly_chart(fig, use_container_width=True)

    with chart_col2:
        # Units breakdown
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=dates,
            y=[m.organic_units for m in metrics],
            name='Organic Units',
            marker_color=COLORS['organic'],
            opacity=0.85,
        ))
        fig2.add_trace(go.Bar(
            x=dates,
            y=[m.ppc_units for m in metrics],
            name='PPC Units',
            marker_color=COLORS['ppc'],
            opacity=0.85,
        ))
        fig2.add_trace(go.Scatter(
            x=dates,
            y=[m.total_units for m in metrics],
            name='Total Units',
            mode='lines+markers',
            line=dict(color=COLORS['accent'], width=2),
            marker=dict(size=6),
        ))
        fig2.update_layout(
            **CHART_LAYOUT,
            title=dict(text='Units Sold Breakdown', font=dict(size=14)),
            barmode='stack',
            yaxis_title='Units',
            height=380,
        )
        st.plotly_chart(fig2, use_container_width=True)

    # ---------- Charts Row 2: Media Performance ----------
    st.markdown('<div class="section-header">ADVERTISING PERFORMANCE</div>', unsafe_allow_html=True)

    media_col1, media_col2 = st.columns(2)

    with media_col1:
        # Spend vs Revenue
        fig3 = go.Figure()
        fig3.add_trace(go.Bar(
            x=dates,
            y=[m.ppc_spend for m in metrics],
            name='PPC Spend',
            marker_color=COLORS['danger'],
            opacity=0.7,
        ))
        fig3.add_trace(go.Scatter(
            x=dates,
            y=[m.attributed_revenue for m in metrics],
            name='Attributed Revenue',
            mode='lines+markers',
            line=dict(color=COLORS['accent'], width=2.5),
            marker=dict(size=7),
            fill='tozeroy',
            fillcolor='rgba(34,211,238,0.1)',
        ))
        fig3.update_layout(
            **CHART_LAYOUT,
            title=dict(text='Ad Spend vs Attributed Revenue', font=dict(size=14)),
            yaxis_title='Amount ($)',
            height=380,
        )
        st.plotly_chart(fig3, use_container_width=True)

    with media_col2:
        # ROAS + ACoS dual axis
        fig4 = go.Figure()
        fig4.add_trace(go.Scatter(
            x=dates,
            y=[m.roas for m in metrics],
            name='ROAS',
            mode='lines+markers',
            line=dict(color=COLORS['success'], width=2.5),
            marker=dict(size=7),
        ))
        fig4.add_trace(go.Scatter(
            x=dates,
            y=[m.acos for m in metrics],
            name='ACoS %',
            mode='lines+markers',
            line=dict(color=COLORS['warning'], width=2.5, dash='dash'),
            marker=dict(size=7),
            yaxis='y2',
        ))
        roas_layout = {k: v for k, v in CHART_LAYOUT.items() if k != 'yaxis'}
        fig4.update_layout(
            **roas_layout,
            title=dict(text='ROAS & ACoS Trends', font=dict(size=14)),
            yaxis=dict(title='ROAS (x)', gridcolor=COLORS['grid'], showgrid=True),
            yaxis2=dict(title='ACoS (%)', overlaying='y', side='right', gridcolor=COLORS['grid']),
            height=380,
        )
        st.plotly_chart(fig4, use_container_width=True)

    # ---------- Health Metrics ----------
    st.markdown('<div class="section-header">BUSINESS HEALTH</div>', unsafe_allow_html=True)

    health_col1, health_col2, health_col3 = st.columns(3)

    with health_col1:
        fig_tacos = go.Figure()
        fig_tacos.add_trace(go.Scatter(
            x=dates,
            y=[m.tacos for m in metrics],
            name='TACoS',
            mode='lines+markers+text',
            text=[f"{m.tacos:.1f}%" for m in metrics],
            textposition='top center',
            textfont=dict(size=10, color=COLORS['text_muted']),
            line=dict(color=COLORS['warning'], width=2.5),
            marker=dict(size=8),
            fill='tozeroy',
            fillcolor='rgba(255,193,7,0.08)',
        ))
        fig_tacos.update_layout(
            **CHART_LAYOUT,
            title=dict(text='TACoS (Ad Spend / Total Sales)', font=dict(size=13)),
            yaxis_title='%',
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig_tacos, use_container_width=True)

    with health_col2:
        # Organic vs PPC orders pie (average)
        avg_organic_pct = sum(m.percent_orders_organic for m in metrics) / len(metrics)
        avg_ppc_pct = sum(m.percent_orders_ppc for m in metrics) / len(metrics)

        fig_pie = go.Figure(data=[go.Pie(
            labels=['Organic', 'PPC'],
            values=[avg_organic_pct, avg_ppc_pct],
            hole=0.6,
            marker=dict(colors=[COLORS['organic'], COLORS['ppc']]),
            textinfo='label+percent',
            textfont=dict(size=12),
        )])
        fig_pie.update_layout(
            **CHART_LAYOUT,
            title=dict(text='Order Source Mix (Avg)', font=dict(size=13)),
            height=300,
            showlegend=False,
            annotations=[dict(text=f"{avg_organic_pct:.0f}%<br>Organic", x=0.5, y=0.5,
                              font_size=14, showarrow=False, font_color=COLORS['text'])],
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with health_col3:
        fig_cpa = go.Figure()
        fig_cpa.add_trace(go.Bar(
            x=dates,
            y=[m.ad_spend_per_unit for m in metrics],
            name='Ad Spend/Unit',
            marker_color=COLORS['secondary'],
            opacity=0.8,
            text=[f"${m.ad_spend_per_unit:.2f}" for m in metrics],
            textposition='outside',
            textfont=dict(size=10, color=COLORS['text_muted']),
        ))
        fig_cpa.update_layout(
            **CHART_LAYOUT,
            title=dict(text='Ad Spend per Unit Sold', font=dict(size=13)),
            yaxis_title='$/unit',
            height=300,
            showlegend=False,
        )
        st.plotly_chart(fig_cpa, use_container_width=True)

    # ---------- Orders by Day ----------
    if daily_orders:
        st.markdown('<div class="section-header">ORDERS BY DAY</div>', unsafe_allow_html=True)

        order_dates = [o.date for o in daily_orders]

        fig_orders = go.Figure()
        fig_orders.add_trace(go.Bar(
            x=order_dates,
            y=[o.paid for o in daily_orders],
            name='Paid',
            marker_color=COLORS['success'],
            opacity=0.85,
        ))
        fig_orders.add_trace(go.Bar(
            x=order_dates,
            y=[o.pending for o in daily_orders],
            name='Pending',
            marker_color=COLORS['warning'],
            opacity=0.85,
        ))
        fig_orders.add_trace(go.Bar(
            x=order_dates,
            y=[o.cancelled for o in daily_orders],
            name='Cancelled',
            marker_color=COLORS['danger'],
            opacity=0.85,
        ))
        fig_orders.add_trace(go.Scatter(
            x=order_dates,
            y=[o.total for o in daily_orders],
            name='Total',
            mode='lines+markers+text',
            text=[str(o.total) for o in daily_orders],
            textposition='top center',
            textfont=dict(size=11, color=COLORS['text']),
            line=dict(color=COLORS['accent'], width=2),
            marker=dict(size=6),
        ))
        fig_orders.update_layout(
            **CHART_LAYOUT,
            barmode='stack',
            yaxis_title='Orders',
            height=350,
        )
        st.plotly_chart(fig_orders, use_container_width=True)

        # Orders summary row
        total_paid = sum(o.paid for o in daily_orders)
        total_pending = sum(o.pending for o in daily_orders)
        total_cancelled = sum(o.cancelled for o in daily_orders)
        total_all_orders = sum(o.total for o in daily_orders)

        oc1, oc2, oc3, oc4 = st.columns(4)
        with oc1:
            kpi_card("Total Orders", str(total_all_orders), f"{len(daily_orders)} days")
        with oc2:
            kpi_card("Paid", str(total_paid),
                     f"{(total_paid/total_all_orders*100):.0f}% of total" if total_all_orders else "")
        with oc3:
            kpi_card("Pending", str(total_pending),
                     f"{(total_pending/total_all_orders*100):.0f}% of total" if total_all_orders else "",
                     "down" if total_pending > total_paid * 0.2 else "up")
        with oc4:
            kpi_card("Cancelled", str(total_cancelled),
                     f"{(total_cancelled/total_all_orders*100):.0f}% of total" if total_all_orders else "",
                     "down" if total_cancelled > 0 else "up")

    # ---------- Inventory ----------
    if inventory:
        st.markdown('<div class="section-header">INVENTORY — CURRENT SNAPSHOT</div>', unsafe_allow_html=True)

        inv_col1, inv_col2 = st.columns([2, 1])

        with inv_col1:
            # Inventory bar chart
            product_names = [i.product_name[:30] for i in inventory]
            fig_inv = go.Figure()
            fig_inv.add_trace(go.Bar(
                y=product_names, x=[i.fulfillable for i in inventory],
                name='Available', orientation='h',
                marker_color=COLORS['success'], opacity=0.85,
            ))
            fig_inv.add_trace(go.Bar(
                y=product_names, x=[i.reserved for i in inventory],
                name='Reserved', orientation='h',
                marker_color=COLORS['warning'], opacity=0.85,
            ))
            fig_inv.add_trace(go.Bar(
                y=product_names, x=[i.unsellable for i in inventory],
                name='Unsellable', orientation='h',
                marker_color=COLORS['danger'], opacity=0.85,
            ))
            fig_inv.add_trace(go.Bar(
                y=product_names,
                x=[i.inbound_working + i.inbound_shipped + i.inbound_receiving for i in inventory],
                name='Inbound', orientation='h',
                marker_color=COLORS['accent'], opacity=0.85,
            ))
            fig_inv.add_trace(go.Bar(
                y=product_names, x=[i.researching for i in inventory],
                name='Researching', orientation='h',
                marker_color=COLORS['secondary'], opacity=0.85,
            ))
            inv_layout = {k: v for k, v in CHART_LAYOUT.items() if k != 'yaxis'}
            fig_inv.update_layout(
                **inv_layout,
                barmode='stack',
                xaxis_title='Units',
                height=max(250, len(inventory) * 70),
                yaxis=dict(autorange='reversed', gridcolor=COLORS['grid'], showgrid=True),
            )
            st.plotly_chart(fig_inv, use_container_width=True)

        with inv_col2:
            # Inventory table
            inv_rows = []
            for item in inventory:
                inbound = item.inbound_working + item.inbound_shipped + item.inbound_receiving
                inv_rows.append({
                    'Product': item.product_name[:28],
                    'Avail': item.fulfillable,
                    'Resv': item.reserved,
                    'Rsrch': item.researching,
                    'Inbnd': inbound,
                    'Total': item.total_quantity,
                })
            # Totals row
            inv_rows.append({
                'Product': 'TOTAL',
                'Avail': sum(i.fulfillable for i in inventory),
                'Resv': sum(i.reserved for i in inventory),
                'Rsrch': sum(i.researching for i in inventory),
                'Inbnd': sum(i.inbound_working + i.inbound_shipped + i.inbound_receiving for i in inventory),
                'Total': sum(i.total_quantity for i in inventory),
            })
            inv_df = pd.DataFrame(inv_rows)
            st.dataframe(inv_df, use_container_width=True, hide_index=True)

    # ---------- Detailed Data Table ----------
    st.markdown('<div class="section-header">DAILY DATA TABLE</div>', unsafe_allow_html=True)

    with st.expander("View detailed daily metrics", expanded=False):
        table_data = []
        for m in metrics:
            table_data.append({
                'Date': m.date,
                'Total Sales': f"${m.total_sales:,.2f}",
                'Organic Sales': f"${m.organic_sales:,.2f}",
                'PPC Sales': f"${m.ppc_sales:,.2f}",
                'Units': m.total_units,
                'Orders': m.total_orders,
                'PPC Spend': f"${m.ppc_spend:,.2f}",
                'ROAS': f"{m.roas:.2f}x",
                'ACoS': f"{m.acos:.1f}%",
                'TACoS': f"{m.tacos:.1f}%",
                'Conv Rate': f"{m.conversion_rate:.1f}%",
            })
        df = pd.DataFrame(table_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

else:
    st.warning("No metrics data available. Adjust date range or check API configuration.")

# ---------------------------------------------------------------------------
# Reconciliation Section
# ---------------------------------------------------------------------------
st.markdown('<div class="section-header">SETTLEMENT RECONCILIATION</div>', unsafe_allow_html=True)

if use_mock:
    st.info("Settlement reconciliation is only available with live API data.")
else:
    with st.spinner("Loading settlement reports..."):
        settlements = load_settlements(use_mock)

    if settlements:
        # Settlement selector — prominent styling
        st.markdown("""
        <style>
            div[data-testid="stSelectbox"][aria-label="Select Settlement Period"] {
                background: linear-gradient(145deg, #1a1a35, #252550);
                border: 2px solid rgba(99,102,241,0.5);
                border-radius: 12px;
                padding: 8px 12px;
            }
        </style>
        """, unsafe_allow_html=True)

        settle_sel_col1, settle_sel_col2 = st.columns([1, 2])
        with settle_sel_col1:
            st.markdown("""
            <div style="background: linear-gradient(145deg, #1e1e30, #252540);
                        border: 1px solid rgba(99,102,241,0.4);
                        border-radius: 12px; padding: 16px 20px; height: 100%;">
                <div style="color:#a5b4fc; font-size:0.75rem; font-weight:600; text-transform:uppercase; letter-spacing:1px;">Settlement Period</div>
                <div style="color:#fff; font-size:1rem; margin-top:6px;">Select a closed settlement to analyze</div>
            </div>
            """, unsafe_allow_html=True)
        with settle_sel_col2:
            settlement_options = {
                f"📋  {s.start_date[:10]}  →  {s.end_date[:10]}   |   Payout: ${s.total_amount:,.2f}": i
                for i, s in enumerate(settlements)
            }
            selected_label = st.selectbox(
                "Select Settlement Period",
                list(settlement_options.keys()),
                label_visibility="collapsed",
            )
        sel = settlements[settlement_options[selected_label]]

        st.markdown("<br>", unsafe_allow_html=True)

        # ── 8-Tab Analysis Interface ──
        tab_recon, tab_je, tab_sku_sales, tab_overview, tab_explorer, tab_orders, tab_skus, tab_fees = st.tabs([
            "📊 Reconciliation", "📋 JE Summary", "📦 SKU Sales",
            "Overview", "Transaction Explorer", "Per-Order P&L",
            "SKU Profitability", "Fee Analysis",
        ])

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TAB: RECONCILIATION (4a-4h Structure)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        with tab_recon:
            # ═══════════════════════════════════════════════════════════════
            # HOW THIS RECONCILIATION WORKS (Cover Sheet per Mike's spec)
            # ═══════════════════════════════════════════════════════════════
            with st.expander("📋 How This Reconciliation Works", expanded=False):
                st.markdown("""
                ### Reconciliation Methodology (Mike's Specification)

                **Formula: (A - B) = (D - E)**

                | Component | Description |
                |-----------|-------------|
                | **A** | Amazon Payment — The actual payout deposited to your bank |
                | **B** | Adjust Payment — Seller repayments/billing charges deducted |
                | **D** | Sales Data — Gross revenue (Product Charges + Shipping + Reimbursements) |
                | **E** | Adjustments — Timing/data differences explaining why D ≠ A-B |

                ---

                ### Adjustment Categories (4a through 4h) — Mike's Exact Definitions

                Each bucket explains **WHY** there's a difference between Sales Data and Amazon Payment:

                | Line | Category | What It Captures |
                |------|----------|------------------|
                | **4a** | Taxes | Tax collected vs. withheld (usually nets to ~$0) |
                | **4b** | Unsettled Orders | Reserves held by Amazon / orders not yet settled |
                | **4c** | Service Fee Timing | **Account-level fees assessed period N, deducted N+1:** Subscription, FBA Storage, Long-Term Storage, Inbound Transportation, Inbound Placement, AWD, Coupon, Removal/Disposal |
                | **4d** | Ad Spend Timing | Advertising charges (ServiceFee transactions) |
                | **4e** | Prior Period Orders | **Per-order fees:** FBA fulfillment fees + Commission on orders settling in this payout |
                | **4f** | Fees Not in Sales | Items with **NO sales data entry:** Buy Shipping Labels, Amazon Deal fees, Order promotions |
                | **4g** | Cross-Period Refunds | Refunded sales amounts (ideally only cross-period refunds) |
                | **4h** | Opening Balance | Other adjustments, reimbursements, uncategorized items |

                ---

                ### Key Distinction: 4c vs 4e

                - **4c (Service Fee Timing)**: Account-level recurring fees like Subscription, Storage, Inbound — NOT per-order
                - **4e (Prior Period Orders)**: Per-order fees like FBAPerUnitFulfillmentFee, Commission — tied to specific orders

                ---

                ### Quality Control (Per Mike's Spec)

                - **NO PLUGS ALLOWED** — Every item must be individually identified
                - **Final Difference must be $0.00** — If not, there are unresolved items
                - **Notes section** appears ONLY when there are exceptions
                - Every row is identified by order number, date, and amount
                """)

            st.markdown("""
            **Full Settlement Reconciliation** — Matching Amazon Payment to Sales Data with timing adjustments.

            Formula: **(A - B) = (D - E)** where E = Σ(4a through 4h)
            """)

            # Option to fetch order dates (default ON per Mike's spec for accurate 4e/4g)
            fetch_dates = st.checkbox(
                "Fetch PurchaseDate for accurate 4e/4g detection",
                value=True,
                help="Fetches order dates to identify TRUE prior-period orders and cross-period refunds. Takes ~20-30 seconds."
            )

            # Run reconciliation with order dates by default (per Mike's spec)
            with st.spinner("Running reconciliation..." + (" (fetching order dates)" if fetch_dates else "")):
                fc = FinancesClient()
                recon = fc.reconcile_settlement(sel, fetch_order_dates=fetch_dates)

            # Main reconciliation table
            st.markdown("### Settlement Reconciliation")

            recon_cols = st.columns(4)
            with recon_cols[0]:
                kpi_card("A - Amazon Payment", f"${recon.amazon_payment:,.2f}", "Payout from settlement")
            with recon_cols[1]:
                kpi_card("B - Adjust Payment", f"${recon.adjust_payment:,.2f}", "Seller repayments/billing")
            with recon_cols[2]:
                kpi_card("A-B Net Payment", f"${recon.adjusted_amazon_payment:,.2f}", "Adjusted payout")
            with recon_cols[3]:
                status = "✅ RECONCILED" if recon.is_reconciled else f"❌ Diff: ${recon.final_difference:,.2f}"
                kpi_card("Status", status, f"Final difference: ${recon.final_difference:,.2f}",
                        "up" if recon.is_reconciled else "down")

            st.markdown("<br>", unsafe_allow_html=True)

            # Adjustments breakdown (E = 4a through 4h)
            st.markdown("### Adjustments (E) — Deductions from Revenue")

            adj_data = [
                {"Line": "4a", "Category": "Taxes", "Amount": f"${recon.adj_4a_taxes:,.2f}",
                 "Description": "Tax collected vs withheld (usually nets to $0)", "Items": len(recon.detail_4a)},
                {"Line": "4b", "Category": "Unsettled Orders", "Amount": f"${recon.adj_4b_unsettled_orders:,.2f}",
                 "Description": "Reserves/orders not yet settled by Amazon", "Items": len(recon.detail_4b)},
                {"Line": "4c", "Category": "Service Fee Timing", "Amount": f"${recon.adj_4c_service_fee_timing:,.2f}",
                 "Description": "Subscription, Storage, Inbound, Disposal, Coupon fees", "Items": len(recon.detail_4c)},
                {"Line": "4d", "Category": "Ad Spend Timing", "Amount": f"${recon.adj_4d_ad_spend_timing:,.2f}",
                 "Description": "Advertising charges", "Items": len(recon.detail_4d)},
                {"Line": "4e", "Category": "Prior Period Orders", "Amount": f"${recon.adj_4e_prior_period_orders:,.2f}",
                 "Description": "Per-order fees (FBA fulfillment + Commission)", "Items": len(recon.detail_4e)},
                {"Line": "4f", "Category": "Fees Not in Sales", "Amount": f"${recon.adj_4f_fees_not_in_sales:,.2f}",
                 "Description": "Shipping labels, Deal fees, Promotions (no sales entry)", "Items": len(recon.detail_4f)},
                {"Line": "4g", "Category": "Cross-Period Refunds", "Amount": f"${recon.adj_4g_cross_period_refunds:,.2f}",
                 "Description": "Refunded sales amounts", "Items": len(recon.detail_4g)},
                {"Line": "4h", "Category": "Opening Balance", "Amount": f"${recon.adj_4h_opening_balance:,.2f}",
                 "Description": "Other adjustments, reimbursements", "Items": len(recon.detail_4h)},
            ]
            df_adj = pd.DataFrame(adj_data)
            st.dataframe(df_adj, use_container_width=True, hide_index=True)

            # Summary row
            sum_cols = st.columns(3)
            with sum_cols[0]:
                kpi_card("D - Sales Data", f"${recon.sales_data_total:,.2f}", "Orders in period")
            with sum_cols[1]:
                kpi_card("E - Total Adjustments", f"${recon.total_adjustments:,.2f}", "Sum of 4a-4h")
            with sum_cols[2]:
                kpi_card("D-E Adjusted Sales", f"${recon.adjusted_sales_data:,.2f}", "Should match A-B")

            # ═══════════════════════════════════════════════════════════════
            # NOTES SECTION - Per Mike's spec: EXCEPTIONS ONLY
            # This section only appears when Final Difference ≠ $0
            # ═══════════════════════════════════════════════════════════════
            if recon.exceptions or not recon.is_reconciled:
                st.markdown("---")
                st.markdown("### Notes (Exceptions Only)")
                st.caption("This section lists unresolved items requiring investigation. It should be empty when fully reconciled.")

                if not recon.is_reconciled:
                    st.error(f"**Final Difference: ${recon.final_difference:,.2f}** — Reconciliation incomplete")

                note_num = 1
                for exc in recon.exceptions:
                    if exc.get('type') == 'unresolved_difference':
                        st.warning(f"**Note {note_num}:** Unresolved difference of ${exc['amount']:,.2f}")
                        st.markdown(f"  - {exc.get('message', 'Requires investigation')}")
                        if exc.get('note'):
                            st.markdown(f"  - {exc['note']}")
                        note_num += 1
                    else:
                        st.info(f"**Note {note_num}:** {exc.get('message', '')}")
                        note_num += 1

            # Detail expanders for each adjustment category
            with st.expander("View Adjustment Details (4a-4h)", expanded=False):
                detail_tab = st.selectbox("Select adjustment category:",
                    ["4a - Taxes", "4b - Unsettled Orders", "4c - Service Fee Timing",
                     "4d - Ad Spend Timing", "4e - Prior Period Orders",
                     "4f - Fees Not in Sales", "4g - Cross-Period Refunds", "4h - Opening Balance"])

                if detail_tab == "4a - Taxes" and recon.detail_4a:
                    st.dataframe(pd.DataFrame(recon.detail_4a), use_container_width=True, hide_index=True)
                elif detail_tab == "4b - Unsettled Orders" and recon.detail_4b:
                    st.dataframe(pd.DataFrame(recon.detail_4b), use_container_width=True, hide_index=True)
                elif detail_tab == "4c - Service Fee Timing" and recon.detail_4c:
                    st.dataframe(pd.DataFrame(recon.detail_4c), use_container_width=True, hide_index=True)
                elif detail_tab == "4d - Ad Spend Timing" and recon.detail_4d:
                    st.dataframe(pd.DataFrame(recon.detail_4d), use_container_width=True, hide_index=True)
                elif detail_tab == "4e - Prior Period Orders" and recon.detail_4e:
                    st.dataframe(pd.DataFrame(recon.detail_4e), use_container_width=True, hide_index=True)
                elif detail_tab == "4f - Fees Not in Sales" and recon.detail_4f:
                    st.dataframe(pd.DataFrame(recon.detail_4f), use_container_width=True, hide_index=True)
                elif detail_tab == "4g - Cross-Period Refunds" and recon.detail_4g:
                    st.dataframe(pd.DataFrame(recon.detail_4g), use_container_width=True, hide_index=True)
                elif detail_tab == "4h - Opening Balance" and recon.detail_4h:
                    st.dataframe(pd.DataFrame(recon.detail_4h), use_container_width=True, hide_index=True)
                else:
                    st.info("No detail items for this category.")

            # Export to Google Sheets button
            st.markdown("---")
            st.markdown("### Export to Google Sheets")
            st.markdown("""
            Export full reconciliation with detail tabs to Google Sheets:
            - **Reconciliation** (summary with Notes section)
            - **4c Service Fee Timing** (individual transactions)
            - **4e Prior Period Orders** (with PurchaseDate)
            - **4f Fees Not in Sales** (shipping labels, deal fees)
            - **4g Cross-Period Refunds** (with PurchaseDate)
            - **4h Opening Balance** (other adjustments)
            - **JE Summary** (by GL account)
            - **SKU Sales** (units and revenue)
            """)

            export_cols = st.columns([2, 1])
            with export_cols[0]:
                num_periods = st.selectbox(
                    "Number of periods to export:",
                    options=[1, 3, 5, 10],
                    index=1,
                    help="How many settlement periods to include in the export"
                )
            with export_cols[1]:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("Export to Google Sheets", key="export_recon_btn", type="primary"):
                    if Config.GOOGLE_SHEET_ID:
                        with st.spinner(f"Exporting {num_periods} periods to Google Sheets..."):
                            from src.output import export_reconciliation_to_sheets
                            success = export_reconciliation_to_sheets(
                                spreadsheet_id=Config.GOOGLE_SHEET_ID,
                                count=num_periods,
                                fetch_order_dates=True,
                            )
                        if success:
                            st.success(f"Exported {num_periods} periods with all detail tabs!")
                        else:
                            st.error("Export failed - check credentials and sheet ID")
                    else:
                        st.error("GOOGLE_SHEET_ID not configured in .env")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TAB: JE SUMMARY (Journal Entry Preparation)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        with tab_je:
            st.markdown("""
            **Journal Entry Preparation** — Settlement transactions mapped to GL accounts, ready for Odoo/QuickBooks.
            """)

            # Get JE summary
            je_data = sel.je_summary()
            df_je = pd.DataFrame(je_data)

            # Summary metrics
            total_debits = df_je['debit'].sum()
            total_credits = df_je['credit'].sum()

            je_cols = st.columns(4)
            with je_cols[0]:
                kpi_card("Total Debits", f"${total_debits:,.2f}", "DR entries")
            with je_cols[1]:
                kpi_card("Total Credits", f"${total_credits:,.2f}", "CR entries")
            with je_cols[2]:
                balance = round(total_debits - total_credits, 2)
                kpi_card("Balance", f"${balance:,.2f}", "Should be $0.00",
                        "up" if abs(balance) < 0.01 else "down")
            with je_cols[3]:
                kpi_card("GL Accounts", f"{len(je_data)}", "Unique accounts")

            st.markdown("<br>", unsafe_allow_html=True)

            # JE Table
            st.markdown(f"### Journal Entry for Period {sel.start_date[:10]} to {sel.end_date[:10]}")

            # Format for display
            df_je_display = df_je.copy()
            df_je_display['debit'] = df_je_display['debit'].apply(lambda x: f"${x:,.2f}" if x > 0 else "")
            df_je_display['credit'] = df_je_display['credit'].apply(lambda x: f"${x:,.2f}" if x > 0 else "")
            df_je_display.columns = ['Account', 'Account Name', 'Debit', 'Credit', 'Description']

            st.dataframe(df_je_display, use_container_width=True, hide_index=True)

            # GL Account reference
            with st.expander("GL Account Reference", expanded=False):
                gl_ref = [{"Account": k, "Name": v['name'], "Type": v['type']}
                          for k, v in GL_ACCOUNTS.items()]
                st.dataframe(pd.DataFrame(gl_ref), use_container_width=True, hide_index=True)

            # Download JE as CSV
            je_csv = df_je.to_csv(index=False)
            st.download_button(
                "Download JE Summary CSV",
                je_csv,
                file_name=f"je_summary_{sel.settlement_id}.csv",
                mime="text/csv",
                key="je_csv_download",
            )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TAB: SKU SALES (Units & Revenue by SKU)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        with tab_sku_sales:
            st.markdown("""
            **SKU-Level Sales Data** — Units sold and revenue by product for this settlement period.
            Use this for COGS calculation and inventory reconciliation.
            """)

            sku_sales = sel.sku_sales_summary()
            if sku_sales:
                df_sku_sales = pd.DataFrame(sku_sales)

                # Summary metrics
                total_units = df_sku_sales['units_sold'].sum()
                total_revenue = df_sku_sales['gross_revenue'].sum()
                total_refund_units = df_sku_sales['refund_units'].sum()
                net_units = df_sku_sales['net_units'].sum()

                sku_cols = st.columns(4)
                with sku_cols[0]:
                    kpi_card("Total Units Sold", f"{total_units}", "Gross units")
                with sku_cols[1]:
                    kpi_card("Gross Revenue", f"${total_revenue:,.2f}", "Before refunds")
                with sku_cols[2]:
                    kpi_card("Refund Units", f"{total_refund_units}", "Units returned", "down")
                with sku_cols[3]:
                    kpi_card("Net Units", f"{net_units}", "After refunds")

                st.markdown("<br>", unsafe_allow_html=True)

                # SKU Sales table
                st.markdown("### Sales by SKU")
                df_sku_display = df_sku_sales.copy()
                df_sku_display.columns = ['SKU', 'Units Sold', 'Gross Revenue', 'Refund Units',
                                          'Refund Amount', 'Net Units', 'Net Revenue']
                st.dataframe(
                    df_sku_display.style.applymap(
                        lambda v: 'color: #00e676' if isinstance(v, (int, float)) and v > 0
                        else ('color: #ff5252' if isinstance(v, (int, float)) and v < 0 else ''),
                        subset=['Gross Revenue', 'Net Revenue']
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

                # Download SKU sales
                sku_csv = df_sku_sales.to_csv(index=False)
                st.download_button(
                    "Download SKU Sales CSV",
                    sku_csv,
                    file_name=f"sku_sales_{sel.settlement_id}.csv",
                    mime="text/csv",
                    key="sku_sales_csv",
                )
            else:
                st.info("No SKU sales data found in this settlement.")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TAB: OVERVIEW (Original)
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        with tab_overview:
            # Reserve detection alert
            reserves = sel.detect_reserves()
            if reserves['has_reserves']:
                net_r = reserves['net_change']
                if abs(net_r) > 0.01:
                    st.warning(
                        f"**Reserve hold detected:** Amazon is holding ${abs(reserves['current_reserve']):,.2f} "
                        f"in reserves this period. Previous reserve released: ${reserves['previous_reserve']:,.2f}. "
                        f"Net impact on payout: **${net_r:+,.2f}**. "
                        f"This is a common reason your deposit is lower than expected."
                    )

            # KPI cards
            rec_cols = st.columns(4)
            diff = round(sel.sum_of_rows - sel.total_amount, 2)
            with rec_cols[0]:
                kpi_card("Payout Amount", f"${sel.total_amount:,.2f}", f"Settlement {sel.settlement_id[:8]}")
            with rec_cols[1]:
                kpi_card("Sum of Rows", f"${sel.sum_of_rows:,.2f}", f"{len(sel.rows)} transactions")
            with rec_cols[2]:
                kpi_card("Difference", f"${diff:,.2f}",
                         "RECONCILED" if sel.reconciles else "MISMATCH",
                         "up" if sel.reconciles else "down")
            with rec_cols[3]:
                kpi_card("Orders", f"{len(sel.unique_order_ids)}", "Unique order IDs")

            st.markdown("<br>", unsafe_allow_html=True)

            # Breakdown chart + table
            breakdown_col, table_col = st.columns([3, 2])
            categories = [
                'Product Charges', 'Shipping', 'Inv. Reimbursements', 'Refunded Expenses',
                'Refunded Sales', 'Promo Rebates', 'FBA Fees',
                'Cost of Advertising', 'Shipping Charges', 'Amazon Fees', 'Other Fees',
            ]
            values = [
                sel.product_charges, sel.shipping_revenue, sel.inventory_reimbursements,
                sel.refunded_expenses, sel.refunded_sales, sel.promo_rebates,
                sel.fba_fees, sel.advertising_costs, sel.shipping_charges,
                sel.amazon_fees, sel.other_fees,
            ]
            bar_colors = [
                COLORS['success'], COLORS['accent'], COLORS['success'], COLORS['success'],
                COLORS['danger'], COLORS['warning'], COLORS['danger'],
                COLORS['danger'], COLORS['danger'], COLORS['danger'], COLORS['danger'],
            ]

            with breakdown_col:
                fig_breakdown = go.Figure()
                fig_breakdown.add_trace(go.Bar(
                    x=categories, y=values,
                    marker_color=bar_colors, opacity=0.85,
                    text=[f"${v:,.2f}" for v in values],
                    textposition='outside',
                    textfont=dict(size=11, color=COLORS['text']),
                ))
                fig_breakdown.update_layout(
                    **CHART_LAYOUT,
                    title=dict(text='Settlement Breakdown', font=dict(size=14)),
                    yaxis_title='Amount ($)', height=420, showlegend=False,
                )
                fig_breakdown.add_hline(
                    y=sel.total_amount, line_dash="dash",
                    line_color=COLORS['accent'], opacity=0.6,
                    annotation_text=f"Payout: ${sel.total_amount:,.2f}",
                    annotation_font_color=COLORS['accent'],
                )
                st.plotly_chart(fig_breakdown, use_container_width=True)

            with table_col:
                st.markdown("**Category Breakdown**")
                breakdown_rows = [{'Category': c, 'Amount': f"${v:,.2f}"} for c, v in zip(categories, values)]
                breakdown_rows.append({'Category': 'NET PAYOUT', 'Amount': f"${sel.total_amount:,.2f}"})
                st.dataframe(pd.DataFrame(breakdown_rows), use_container_width=True, hide_index=True)

                if sel.reconciles:
                    st.success(f"RECONCILED: Sum of {len(sel.rows)} rows = ${sel.sum_of_rows:,.2f} matches payout of ${sel.total_amount:,.2f}")
                else:
                    st.error(f"MISMATCH: Sum ${sel.sum_of_rows:,.2f} vs Payout ${sel.total_amount:,.2f} (diff: ${diff:,.2f})")

            # Transaction type distribution
            with st.expander("Transaction type distribution", expanded=False):
                type_counts = {}
                type_amounts = {}
                for r in sel.rows:
                    t = r.transaction_type or 'Unknown'
                    type_counts[t] = type_counts.get(t, 0) + 1
                    type_amounts[t] = type_amounts.get(t, 0.0) + r.amount
                type_rows = [
                    {'Transaction Type': t, 'Count': type_counts[t], 'Total Amount': f"${type_amounts[t]:,.2f}"}
                    for t in sorted(type_counts.keys())
                ]
                st.dataframe(pd.DataFrame(type_rows), use_container_width=True, hide_index=True)

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TAB 2: TRANSACTION EXPLORER
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        with tab_explorer:
            st.markdown("**Every transaction in this settlement period — search, filter, export.**")

            df_all = sel.rows_as_dataframe()

            # Filters
            filter_cols = st.columns([2, 2, 2, 3])
            with filter_cols[0]:
                types_available = sorted(df_all['Type'].dropna().unique().tolist())
                type_filter = st.multiselect("Transaction Type", types_available, default=types_available, key="explorer_type")
            with filter_cols[1]:
                cats_available = sorted(df_all['Category'].dropna().unique().tolist())
                cat_filter = st.multiselect("Category", cats_available, default=cats_available, key="explorer_cat")
            with filter_cols[2]:
                skus_available = sorted([s for s in df_all['SKU'].dropna().unique().tolist() if s])
                sku_filter = st.multiselect("SKU", skus_available, key="explorer_sku")
            with filter_cols[3]:
                order_search = st.text_input("Search Order ID", key="explorer_order")

            # Apply filters
            df_filtered = df_all[
                (df_all['Type'].isin(type_filter)) &
                (df_all['Category'].isin(cat_filter))
            ]
            if sku_filter:
                df_filtered = df_filtered[df_filtered['SKU'].isin(sku_filter)]
            if order_search:
                df_filtered = df_filtered[df_filtered['Order ID'].str.contains(order_search, case=False, na=False)]

            # Summary metrics
            sum_cols = st.columns(4)
            with sum_cols[0]:
                kpi_card("Filtered Rows", f"{len(df_filtered)}", f"of {len(df_all)} total")
            with sum_cols[1]:
                filtered_total = round(df_filtered['Amount'].sum(), 2)
                kpi_card("Filtered Total", f"${filtered_total:,.2f}", "Sum of filtered amounts")
            with sum_cols[2]:
                income = round(df_filtered[df_filtered['Amount'] > 0]['Amount'].sum(), 2)
                kpi_card("Income", f"${income:,.2f}", "Positive amounts", "up")
            with sum_cols[3]:
                expenses = round(df_filtered[df_filtered['Amount'] < 0]['Amount'].sum(), 2)
                kpi_card("Expenses", f"${expenses:,.2f}", "Negative amounts", "down")

            st.markdown("<br>", unsafe_allow_html=True)

            # Display table with color formatting
            st.dataframe(
                df_filtered.style.applymap(
                    lambda v: 'color: #00e676' if isinstance(v, (int, float)) and v > 0
                    else ('color: #ff5252' if isinstance(v, (int, float)) and v < 0 else ''),
                    subset=['Amount']
                ),
                use_container_width=True,
                hide_index=True,
                height=500,
            )

            # CSV download
            csv_data = df_filtered.to_csv(index=False)
            st.download_button(
                "Download Filtered CSV",
                csv_data,
                file_name=f"settlement_{sel.settlement_id}_transactions.csv",
                mime="text/csv",
                key="explorer_csv",
            )

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TAB 3: PER-ORDER P&L
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        with tab_orders:
            st.markdown("**Profit & Loss per order — see exactly what Amazon charged on each sale.**")

            order_data = sel.per_order_breakdown()
            if order_data:
                df_orders = pd.DataFrame(order_data)

                # Summary metrics
                total_orders = len(df_orders)
                avg_fee_pct = df_orders['Fee %'].mean()
                high_fee_orders = len(df_orders[df_orders['Fee %'] > 35])

                ord_cols = st.columns(4)
                with ord_cols[0]:
                    kpi_card("Total Orders", f"{total_orders}", "In this settlement")
                with ord_cols[1]:
                    kpi_card("Avg Fee %", f"{avg_fee_pct:.1f}%", "Across all orders")
                with ord_cols[2]:
                    kpi_card("High-Fee Orders", f"{high_fee_orders}", "Fee > 35%", "down" if high_fee_orders > 0 else "up")
                with ord_cols[3]:
                    total_net = round(df_orders['Net'].sum(), 2)
                    kpi_card("Total Net", f"${total_net:,.2f}", "After all fees")

                st.markdown("<br>", unsafe_allow_html=True)

                # Styled table
                st.dataframe(
                    df_orders.style
                        .applymap(
                            lambda v: 'background-color: rgba(255,82,82,0.2)' if isinstance(v, (int, float)) and v > 35 else '',
                            subset=['Fee %']
                        )
                        .applymap(
                            lambda v: 'color: #00e676' if isinstance(v, (int, float)) and v > 0
                            else ('color: #ff5252' if isinstance(v, (int, float)) and v < 0 else ''),
                            subset=['Gross Sale', 'Fees', 'Net']
                        ),
                    use_container_width=True,
                    hide_index=True,
                    height=500,
                )

                # Expand detail for specific order
                with st.expander("Drill into specific order"):
                    order_ids_available = df_orders['Order ID'].tolist()
                    selected_order = st.selectbox("Select Order", order_ids_available, key="order_detail_select")
                    if selected_order:
                        order_rows = [r for r in sel.rows if r.order_id == selected_order]
                        detail_data = [{
                            'Type': r.transaction_type,
                            'Amount Type': r.amount_type,
                            'Description': r.amount_description,
                            'Amount': r.amount,
                            'Category': sel._row_category(r),
                        } for r in order_rows]
                        st.dataframe(pd.DataFrame(detail_data), use_container_width=True, hide_index=True)

                # Optional Finances API enrichment
                with st.expander("Enrich with Finances API (detailed fee breakdown)"):
                    st.markdown("Pulls per-order fee details from Amazon's Finances API for the top 5 orders.")
                    if st.button("Fetch Fee Details", key="enrich_btn"):
                        import time
                        fc = FinancesClient()
                        top_orders = [o['Order ID'] for o in order_data[:5]]
                        enriched = []
                        progress = st.progress(0, text="Fetching fee details...")
                        for idx, oid in enumerate(top_orders):
                            fees = fc.get_order_fees(oid)
                            enriched.append({
                                'Order ID': oid,
                                'Principal': f"${fees.get('principal', 0):,.2f}",
                                'Commission': f"${fees.get('commission', 0):,.2f}",
                                'FBA Fee': f"${fees.get('fba_fee', 0):,.2f}",
                                'Shipping': f"${fees.get('shipping', 0):,.2f}",
                                'Promo': f"${fees.get('promo', 0):,.2f}",
                                'Other': f"${fees.get('other_fees', 0):,.2f}",
                                'Net': f"${fees.get('net', 0):,.2f}",
                            })
                            progress.progress((idx + 1) / len(top_orders), text=f"Order {idx + 1}/{len(top_orders)}...")
                            time.sleep(0.5)
                        progress.empty()
                        st.dataframe(pd.DataFrame(enriched), use_container_width=True, hide_index=True)
            else:
                st.info("No orders found in this settlement.")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TAB 4: SKU PROFITABILITY
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        with tab_skus:
            st.markdown("**Product-level profitability — which SKUs are making money?**")

            sku_data = sel.sku_profitability()
            if sku_data:
                df_skus = pd.DataFrame(sku_data)

                # KPIs
                sku_cols = st.columns(4)
                with sku_cols[0]:
                    kpi_card("Products", f"{len(df_skus)}", "Unique SKUs")
                with sku_cols[1]:
                    best = df_skus.iloc[0]
                    kpi_card("Top SKU", f"${best['Net']:,.2f}", best['SKU'][:20], "up")
                with sku_cols[2]:
                    worst = df_skus.iloc[-1]
                    kpi_card("Worst SKU", f"${worst['Net']:,.2f}", worst['SKU'][:20],
                             "down" if worst['Net'] < 0 else "up")
                with sku_cols[3]:
                    avg_margin = df_skus['Margin %'].mean()
                    kpi_card("Avg Margin", f"{avg_margin:.1f}%", "Across all SKUs",
                             "up" if avg_margin > 0 else "down")

                st.markdown("<br>", unsafe_allow_html=True)

                # Chart: margin by SKU
                fig_sku = go.Figure()
                fig_sku.add_trace(go.Bar(
                    x=df_skus['SKU'],
                    y=df_skus['Net'],
                    marker_color=[COLORS['success'] if n >= 0 else COLORS['danger'] for n in df_skus['Net']],
                    opacity=0.85,
                    text=[f"${n:,.2f}" for n in df_skus['Net']],
                    textposition='outside',
                    textfont=dict(size=11, color=COLORS['text']),
                ))
                fig_sku.update_layout(
                    **CHART_LAYOUT,
                    title=dict(text='Net Profit by SKU', font=dict(size=14)),
                    yaxis_title='Net ($)', height=380, showlegend=False,
                )
                st.plotly_chart(fig_sku, use_container_width=True)

                # Table
                st.dataframe(
                    df_skus.style.applymap(
                        lambda v: 'color: #00e676' if isinstance(v, (int, float)) and v > 0
                        else ('color: #ff5252' if isinstance(v, (int, float)) and v < 0 else ''),
                        subset=['Revenue', 'Fees', 'Net', 'Margin %']
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No SKU data found in this settlement.")

        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # TAB 5: FEE ANALYSIS
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        with tab_fees:
            st.markdown("**Where your money goes — fee breakdown as percentage of gross revenue.**")

            ratios = sel.fee_ratios()
            gross_revenue = sel.product_charges + sel.shipping_revenue

            # KPIs
            fee_kpi_cols = st.columns(5)
            with fee_kpi_cols[0]:
                kpi_card("Gross Revenue", f"${gross_revenue:,.2f}", "Products + Shipping")
            with fee_kpi_cols[1]:
                kpi_card("Amazon Fees", f"{ratios['amazon_fees_pct']}%", f"${abs(sel.amazon_fees):,.2f}", "down")
            with fee_kpi_cols[2]:
                kpi_card("FBA Fees", f"{ratios['fba_pct']}%", f"${abs(sel.fba_fees):,.2f}", "down")
            with fee_kpi_cols[3]:
                kpi_card("Advertising", f"{ratios['ads_pct']}%", f"${abs(sel.advertising_costs):,.2f}", "down")
            with fee_kpi_cols[4]:
                kpi_card("Total Fees", f"{ratios['total_fee_pct']}%", "Of gross revenue", "down")

            st.markdown("<br>", unsafe_allow_html=True)

            # Pie chart + waterfall
            pie_col, waterfall_col = st.columns(2)

            with pie_col:
                fee_labels = ['Amazon Fees', 'FBA Fees', 'Advertising', 'Shipping Charges']
                fee_values = [abs(sel.amazon_fees), abs(sel.fba_fees), abs(sel.advertising_costs), abs(sel.shipping_charges)]
                if sel.other_fees:
                    fee_labels.append('Other Fees')
                    fee_values.append(abs(sel.other_fees))

                fig_pie = go.Figure(data=[go.Pie(
                    labels=fee_labels,
                    values=fee_values,
                    hole=0.45,
                    marker=dict(colors=[COLORS['danger'], COLORS['warning'], '#a78bfa', COLORS['accent'], '#64748b']),
                    textinfo='label+percent',
                    textfont=dict(size=12, color='white'),
                )])
                fig_pie.update_layout(
                    **CHART_LAYOUT,
                    title=dict(text='Fee Distribution', font=dict(size=14)),
                    height=380,
                    showlegend=False,
                )
                st.plotly_chart(fig_pie, use_container_width=True)

            with waterfall_col:
                # Waterfall: Revenue → Fees → Net
                waterfall_cats = ['Gross Revenue', 'Amazon Fees', 'FBA Fees', 'Advertising',
                                  'Shipping Charges', 'Refunds', 'Promos', 'Other', 'Net Payout']
                waterfall_vals = [
                    gross_revenue, sel.amazon_fees, sel.fba_fees, sel.advertising_costs,
                    sel.shipping_charges, sel.refunded_sales, sel.promo_rebates,
                    sel.other_fees + sel.inventory_reimbursements + sel.refunded_expenses,
                    sel.total_amount,
                ]
                waterfall_measure = ['absolute'] + ['relative'] * 7 + ['total']

                fig_wf = go.Figure(data=[go.Waterfall(
                    x=waterfall_cats,
                    y=waterfall_vals,
                    measure=waterfall_measure,
                    increasing=dict(marker_color=COLORS['success']),
                    decreasing=dict(marker_color=COLORS['danger']),
                    totals=dict(marker_color=COLORS['accent']),
                    textposition='outside',
                    text=[f"${v:,.2f}" for v in waterfall_vals],
                    textfont=dict(size=10, color=COLORS['text']),
                )])
                fig_wf.update_layout(
                    **CHART_LAYOUT,
                    title=dict(text='Revenue to Payout Waterfall', font=dict(size=14)),
                    height=380, showlegend=False,
                )
                st.plotly_chart(fig_wf, use_container_width=True)

            # Settlement comparison (if multiple settlements)
            if len(settlements) > 1:
                st.markdown("---")
                st.markdown("**Fee Trends Across Settlements**")
                trend_data = []
                for s in settlements:
                    r = s.fee_ratios()
                    trend_data.append({
                        'Period': f"{s.start_date[:10]} → {s.end_date[:10]}",
                        'Amazon Fees %': r['amazon_fees_pct'],
                        'FBA %': r['fba_pct'],
                        'Ads %': r['ads_pct'],
                        'Total Fee %': r['total_fee_pct'],
                    })
                df_trend = pd.DataFrame(trend_data)

                fig_trend = go.Figure()
                for col in ['Amazon Fees %', 'FBA %', 'Ads %', 'Total Fee %']:
                    fig_trend.add_trace(go.Scatter(
                        x=df_trend['Period'], y=df_trend[col],
                        mode='lines+markers', name=col,
                    ))
                fig_trend.update_layout(
                    **CHART_LAYOUT,
                    title=dict(text='Fee % of Gross Revenue Over Time', font=dict(size=14)),
                    yaxis_title='%', height=350,
                )
                st.plotly_chart(fig_trend, use_container_width=True)

    else:
        st.warning("No closed settlement reports found. Reports are generated by Amazon every ~14 days.")

# ---------- Footer ----------
st.markdown("""
<div style="text-align:center; color:#555; font-size:0.7rem; margin-top:40px; padding:20px 0;">
    Pura Vitalia Analytics — Powered by Amazon SP-API & Advertising API
</div>
""", unsafe_allow_html=True)
