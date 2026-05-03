"""
DataPulse Analytics — Main Dashboard
Executive KPI Overview
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from utils.db_connection import check_connection, run_query

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataPulse Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d6a9f 100%);
        border-radius: 12px;
        padding: 20px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    .metric-value { font-size: 2.2rem; font-weight: 700; margin: 8px 0; }
    .metric-label { font-size: 0.9rem; opacity: 0.85; }
    .metric-delta { font-size: 0.85rem; color: #7fff7f; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { border-radius: 8px 8px 0 0; padding: 10px 20px; }
    div[data-testid="stMetricValue"] { font-size: 2rem; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/combo-chart.png", width=64)
    st.title("DataPulse")
    st.caption("Retail Analytics Platform")
    st.divider()

    # Connection status
    if check_connection():
        st.success("✓ Database Connected")
    else:
        st.error("✗ Database Offline")
        st.info("Run `make up` to start PostgreSQL")
        st.stop()

    st.subheader("Filters")
    year_filter = st.selectbox("Year", options=["All", "2023", "2024"], index=0)
    segment_filter = st.multiselect(
        "Customer Segment",
        options=["B2C", "B2B"],
        default=["B2C", "B2B"],
    )

    st.divider()
    st.caption("Last pipeline run:")
    try:
        last_run = run_query(
            "SELECT MAX(completed_at) AS last_run FROM bronze.pipeline_runs WHERE status = 'success'"
        )
        ts = last_run["last_run"].iloc[0]
        st.caption(str(ts)[:19] if ts else "No runs yet")
    except Exception:
        st.caption("N/A")

# ── Build SQL filters ─────────────────────────────────────────────────────────
year_condition = f"AND order_year = {year_filter}" if year_filter != "All" else ""
segment_list = "(" + ", ".join(f"'{s}'" for s in segment_filter) + ")"
segment_condition = f"AND customer_segment IN {segment_list}" if segment_filter else ""

# ── Load KPI data ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_kpis(year_cond: str, seg_cond: str) -> dict:
    sql = f"""
        SELECT
            ROUND(SUM(gross_revenue), 2)                AS total_revenue,
            COUNT(DISTINCT order_id)                    AS total_orders,
            COUNT(DISTINCT customer_id)                 AS unique_customers,
            ROUND(AVG(gross_revenue / NULLIF(quantity, 0)), 2) AS avg_order_value,
            ROUND(SUM(gross_profit), 2)                 AS total_profit,
            ROUND(100.0 * SUM(gross_profit) / NULLIF(SUM(gross_revenue), 0), 2) AS overall_margin_pct
        FROM gold.fact_orders
        WHERE is_completed = TRUE
          {year_cond}
          {seg_cond}
    """
    row = run_query(sql).iloc[0]
    return row.to_dict()


@st.cache_data(ttl=300)
def load_revenue_trend(year_cond: str, seg_cond: str) -> object:
    sql = f"""
        SELECT
            order_month       AS month,
            month_name,
            year,
            ROUND(SUM(gross_revenue), 2)    AS revenue,
            COUNT(DISTINCT order_id)         AS orders,
            COUNT(DISTINCT customer_id)      AS customers
        FROM gold.fact_orders
        WHERE is_completed = TRUE
          {year_cond}
          {seg_cond}
        GROUP BY 1, 2, 3
        ORDER BY 1
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_category_breakdown(year_cond: str, seg_cond: str) -> object:
    sql = f"""
        SELECT
            category,
            ROUND(SUM(gross_revenue), 2)  AS revenue,
            ROUND(SUM(gross_profit), 2)   AS profit,
            COUNT(DISTINCT order_id)       AS orders,
            SUM(quantity)                  AS units_sold
        FROM gold.fact_orders
        WHERE is_completed = TRUE
          {year_cond}
          {seg_cond}
        GROUP BY 1
        ORDER BY revenue DESC
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_geo_sales(year_cond: str) -> object:
    sql = f"""
        SELECT
            customer_state      AS state,
            ROUND(SUM(gross_revenue), 2) AS revenue,
            COUNT(DISTINCT customer_id)  AS customers,
            COUNT(DISTINCT order_id)     AS orders
        FROM gold.fact_orders
        WHERE is_completed = TRUE
          AND customer_state IS NOT NULL
          {year_cond}
        GROUP BY 1
        ORDER BY revenue DESC
    """
    return run_query(sql)


# ── Main layout ───────────────────────────────────────────────────────────────
st.title("📊 DataPulse — Executive Dashboard")
st.caption(f"Retail Analytics | Medallion Architecture | dbt + Airflow + PostgreSQL")
st.divider()

try:
    kpis = load_kpis(year_condition, segment_condition)

    # ── KPI Cards ─────────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric("💰 Total Revenue", f"${kpis['total_revenue']:,.0f}")
    with col2:
        st.metric("📦 Total Orders", f"{int(kpis['total_orders']):,}")
    with col3:
        st.metric("👥 Customers", f"{int(kpis['unique_customers']):,}")
    with col4:
        st.metric("🛒 Avg Order Value", f"${kpis['avg_order_value']:,.2f}")
    with col5:
        st.metric("💹 Gross Profit", f"${kpis['total_profit']:,.0f}")
    with col6:
        st.metric("📈 Margin %", f"{kpis['overall_margin_pct']:.1f}%")

    st.divider()

    # ── Revenue trend + Category ──────────────────────────────────────────────
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("Revenue Trend")
        df_trend = load_revenue_trend(year_condition, segment_condition)
        if not df_trend.empty:
            fig = px.area(
                df_trend,
                x="month",
                y="revenue",
                color="year" if year_filter == "All" else None,
                markers=True,
                labels={"revenue": "Revenue ($)", "month": "Month"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                hovermode="x unified",
                legend_title="Year",
                yaxis_tickprefix="$",
                yaxis_tickformat=",.0f",
                height=380,
            )
            fig.add_vline(
                x=df_trend["month"].max(), line_dash="dot",
                annotation_text="Latest", annotation_position="top left",
                line_color="gray",
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Revenue by Category")
        df_cat = load_category_breakdown(year_condition, segment_condition)
        if not df_cat.empty:
            fig_pie = px.pie(
                df_cat,
                names="category",
                values="revenue",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            fig_pie.update_layout(
                showlegend=False,
                paper_bgcolor="rgba(0,0,0,0)",
                height=380,
                margin=dict(t=20, b=20, l=20, r=20),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    # ── Category bar + Geography ──────────────────────────────────────────────
    col_bar, col_geo = st.columns([1, 1])

    with col_bar:
        st.subheader("Category Performance")
        if not df_cat.empty:
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                x=df_cat["category"],
                y=df_cat["revenue"],
                name="Revenue",
                marker_color="#2563EB",
                text=df_cat["revenue"].apply(lambda x: f"${x:,.0f}"),
                textposition="outside",
            ))
            fig_bar.add_trace(go.Bar(
                x=df_cat["category"],
                y=df_cat["profit"],
                name="Gross Profit",
                marker_color="#10B981",
            ))
            fig_bar.update_layout(
                barmode="group",
                xaxis_tickangle=-30,
                yaxis_tickprefix="$",
                yaxis_tickformat=",.0f",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                height=380,
                legend=dict(orientation="h", y=1.0),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    with col_geo:
        st.subheader("Sales by State (US)")
        df_geo = load_geo_sales(year_condition)
        if not df_geo.empty:
            fig_map = px.choropleth(
                df_geo,
                locations="state",
                locationmode="USA-states",
                color="revenue",
                hover_data=["customers", "orders"],
                scope="usa",
                color_continuous_scale="Blues",
                labels={"revenue": "Revenue ($)"},
            )
            fig_map.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                geo_bgcolor="rgba(0,0,0,0)",
                height=380,
                margin=dict(t=0, b=0, l=0, r=0),
                coloraxis_colorbar=dict(tickprefix="$", tickformat=",.0f"),
            )
            st.plotly_chart(fig_map, use_container_width=True)

    # ── Category table ────────────────────────────────────────────────────────
    st.subheader("Category Summary Table")
    df_display = df_cat.copy()
    df_display["revenue"] = df_display["revenue"].apply(lambda x: f"${x:,.2f}")
    df_display["profit"] = df_display["profit"].apply(lambda x: f"${x:,.2f}")
    df_display.columns = ["Category", "Revenue", "Gross Profit", "Orders", "Units Sold"]
    st.dataframe(df_display, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Failed to load dashboard data: {e}")
    st.info("Make sure the pipeline has been run: `make pipeline`")
