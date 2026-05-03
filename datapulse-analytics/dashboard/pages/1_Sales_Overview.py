"""
DataPulse — Sales Overview Page
Deep-dive into revenue trends, order patterns, and seasonal analysis
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db_connection import run_query

st.set_page_config(page_title="Sales Overview | DataPulse", page_icon="📈", layout="wide")
st.title("📈 Sales Overview")
st.caption("Revenue trends, order patterns, and performance analysis")
st.divider()

# ── Filters ──────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
with col1:
    granularity = st.selectbox("Time Granularity", ["Daily", "Weekly", "Monthly"])
with col2:
    year = st.selectbox("Year", ["All", "2023", "2024"])
with col3:
    segment = st.multiselect("Segment", ["B2C", "B2B"], default=["B2C", "B2B"])

year_cond = f"AND order_year = {year}" if year != "All" else ""
seg_list = "(" + ", ".join(f"'{s}'" for s in segment) + ")" if segment else "('B2C','B2B')"
seg_cond = f"AND customer_segment IN {seg_list}"

gran_map = {"Daily": ("order_date", "order_date"), "Weekly": ("order_week", "week"), "Monthly": ("order_month", "month")}
gran_col, gran_label = gran_map[granularity]

st.divider()


@st.cache_data(ttl=300)
def load_time_series(gran: str, year_c: str, seg_c: str):
    sql = f"""
        SELECT
            {gran}                              AS period,
            ROUND(SUM(gross_revenue), 2)        AS revenue,
            ROUND(SUM(gross_profit), 2)         AS profit,
            COUNT(DISTINCT order_id)             AS orders,
            COUNT(DISTINCT customer_id)          AS customers,
            SUM(quantity)                        AS units,
            ROUND(SUM(gross_revenue) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS aov
        FROM gold.fact_orders
        WHERE is_completed = TRUE
          {year_c}
          {seg_c}
        GROUP BY 1
        ORDER BY 1
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_order_status_dist(year_c: str, seg_c: str):
    sql = f"""
        SELECT
            order_status,
            COUNT(DISTINCT order_id)   AS orders,
            ROUND(SUM(gross_revenue), 2) AS revenue
        FROM gold.fact_orders
        WHERE TRUE {year_c} {seg_c}
        GROUP BY 1
        ORDER BY orders DESC
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_shipping_analysis(year_c: str):
    sql = f"""
        SELECT
            shipping_method,
            COUNT(DISTINCT order_id)                     AS orders,
            ROUND(AVG(days_to_ship), 1)                  AS avg_days_to_ship,
            ROUND(SUM(gross_revenue), 2)                 AS revenue
        FROM gold.fact_orders
        WHERE is_completed = TRUE
          AND days_to_ship IS NOT NULL
          {year_c}
        GROUP BY 1
        ORDER BY orders DESC
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_weekday_heatmap(year_c: str):
    sql = f"""
        SELECT
            EXTRACT(DOW FROM order_date)::INT  AS dow,
            TO_CHAR(order_date, 'Dy')          AS day_name,
            EXTRACT(HOUR FROM NOW())::INT       AS hour,  -- placeholder
            COUNT(DISTINCT order_id)            AS orders,
            ROUND(SUM(gross_revenue), 2)        AS revenue
        FROM gold.fact_orders
        WHERE is_completed = TRUE
          {year_c}
        GROUP BY 1, 2, 3
        ORDER BY 1
    """
    return run_query(sql)


try:
    df_ts = load_time_series(gran_col, year_cond, seg_cond)

    # ── Revenue + Profit trend ────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["📊 Revenue & Profit", "📦 Orders & AOV", "🔍 Deep Analysis"])

    with tab1:
        col_l, col_r = st.columns([3, 1])
        with col_l:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_ts["period"], y=df_ts["revenue"],
                mode="lines+markers", name="Revenue",
                line=dict(color="#2563EB", width=2.5),
                fill="tozeroy", fillcolor="rgba(37,99,235,0.08)",
            ))
            fig.add_trace(go.Scatter(
                x=df_ts["period"], y=df_ts["profit"],
                mode="lines", name="Gross Profit",
                line=dict(color="#10B981", width=2, dash="dash"),
            ))
            fig.update_layout(
                title=f"{granularity} Revenue vs Gross Profit",
                yaxis_tickprefix="$", yaxis_tickformat=",.0f",
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                hovermode="x unified", height=420,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            total_rev = df_ts["revenue"].sum()
            total_prof = df_ts["profit"].sum()
            margin = (total_prof / total_rev * 100) if total_rev else 0
            st.metric("Total Revenue", f"${total_rev:,.0f}")
            st.metric("Total Profit", f"${total_prof:,.0f}")
            st.metric("Margin %", f"{margin:.1f}%")
            st.metric("Peak Revenue", f"${df_ts['revenue'].max():,.0f}")
            best_period = df_ts.loc[df_ts["revenue"].idxmax(), "period"]
            st.caption(f"Best period: {best_period}")

    with tab2:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=df_ts["period"], y=df_ts["orders"],
            name="Orders", marker_color="#6366F1", yaxis="y",
        ))
        fig2.add_trace(go.Scatter(
            x=df_ts["period"], y=df_ts["aov"],
            mode="lines+markers", name="Avg Order Value ($)",
            line=dict(color="#F59E0B", width=2.5), yaxis="y2",
        ))
        fig2.update_layout(
            title=f"{granularity} Orders & Average Order Value",
            yaxis=dict(title="Orders", showgrid=False),
            yaxis2=dict(title="AOV ($)", overlaying="y", side="right", tickprefix="$"),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            hovermode="x unified", height=420,
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    with tab3:
        cola, colb = st.columns(2)

        with cola:
            st.subheader("Order Status Distribution")
            df_status = load_order_status_dist(year_cond, seg_cond)
            fig_status = px.bar(
                df_status, x="order_status", y="orders",
                color="order_status", text="orders",
                color_discrete_sequence=px.colors.qualitative.Set3,
                labels={"order_status": "Status", "orders": "Order Count"},
            )
            fig_status.update_traces(textposition="outside")
            fig_status.update_layout(
                showlegend=False,
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=350,
            )
            st.plotly_chart(fig_status, use_container_width=True)

        with colb:
            st.subheader("Shipping Method Performance")
            df_ship = load_shipping_analysis(year_cond)
            fig_ship = px.scatter(
                df_ship, x="avg_days_to_ship", y="revenue",
                size="orders", color="shipping_method",
                text="shipping_method",
                labels={"avg_days_to_ship": "Avg Days to Ship", "revenue": "Revenue ($)"},
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig_ship.update_traces(textposition="top center")
            fig_ship.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=350, showlegend=False,
                yaxis_tickprefix="$", yaxis_tickformat=",.0f",
            )
            st.plotly_chart(fig_ship, use_container_width=True)

    # ── Revenue by weekday ────────────────────────────────────────────────────
    st.subheader("Revenue by Day of Week")
    day_order = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    df_dow = load_weekday_heatmap(year_cond)
    if not df_dow.empty:
        dow_agg = df_dow.groupby("day_name").agg(revenue=("revenue", "sum"), orders=("orders", "sum")).reset_index()
        fig_dow = px.bar(
            dow_agg, x="day_name", y="revenue",
            color="revenue", category_orders={"day_name": day_order},
            color_continuous_scale="Blues",
            text=dow_agg["revenue"].apply(lambda x: f"${x:,.0f}"),
            labels={"revenue": "Revenue ($)", "day_name": "Day"},
        )
        fig_dow.update_traces(textposition="outside")
        fig_dow.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False, height=320,
            yaxis_tickprefix="$", yaxis_tickformat=",.0f",
        )
        st.plotly_chart(fig_dow, use_container_width=True)

except Exception as e:
    st.error(f"Error loading sales data: {e}")
    st.code(str(e))
