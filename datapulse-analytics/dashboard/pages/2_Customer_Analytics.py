"""
DataPulse — Customer Analytics Page
RFM Segmentation, Cohort Analysis, LTV, Churn Signals
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db_connection import run_query

st.set_page_config(page_title="Customer Analytics | DataPulse", page_icon="👥", layout="wide")
st.title("👥 Customer Analytics")
st.caption("RFM segmentation · Cohort retention · Lifetime value · Churn prediction")
st.divider()

# ── Filters ───────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    segment = st.multiselect("Segment", ["B2C", "B2B"], default=["B2C", "B2B"])
with col2:
    min_orders = st.slider("Min Orders to Include", 0, 10, 1)

seg_list = "(" + ", ".join(f"'{s}'" for s in segment) + ")" if segment else "('B2C','B2B')"

st.divider()


@st.cache_data(ttl=300)
def load_rfm_segments(seg_l: str, min_ord: int):
    sql = f"""
        SELECT
            rfm_segment,
            customer_tier,
            customer_segment,
            COUNT(customer_id)                      AS customer_count,
            ROUND(AVG(lifetime_value), 2)            AS avg_ltv,
            ROUND(AVG(avg_order_value), 2)           AS avg_aov,
            ROUND(AVG(completed_orders), 2)          AS avg_orders,
            ROUND(SUM(lifetime_value), 2)            AS total_revenue
        FROM gold.dim_customers
        WHERE customer_segment IN {seg_l}
          AND completed_orders >= {min_ord}
        GROUP BY 1, 2, 3
        ORDER BY total_revenue DESC
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_cohort_retention():
    sql = """
        WITH cohort_base AS (
            SELECT
                customer_id,
                DATE_TRUNC('month', registration_date)::DATE  AS cohort_month
            FROM gold.dim_customers
            WHERE registration_date IS NOT NULL
        ),
        customer_monthly AS (
            SELECT
                customer_id,
                DATE_TRUNC('month', order_date)::DATE  AS activity_month
            FROM gold.fact_orders
            WHERE is_completed = TRUE
            GROUP BY 1, 2
        ),
        cohort_activity AS (
            SELECT
                cb.cohort_month,
                cm.activity_month,
                DATE_PART('month', AGE(cm.activity_month, cb.cohort_month))::INT AS months_since_join,
                COUNT(DISTINCT cb.customer_id)                                    AS active_customers
            FROM cohort_base cb
            JOIN customer_monthly cm USING (customer_id)
            WHERE cm.activity_month >= cb.cohort_month
            GROUP BY 1, 2, 3
        ),
        cohort_sizes AS (
            SELECT cohort_month, COUNT(DISTINCT customer_id) AS cohort_size
            FROM cohort_base
            GROUP BY 1
        )
        SELECT
            ca.cohort_month,
            ca.months_since_join,
            ca.active_customers,
            cs.cohort_size,
            ROUND(100.0 * ca.active_customers / cs.cohort_size, 1) AS retention_pct
        FROM cohort_activity ca
        JOIN cohort_sizes cs USING (cohort_month)
        WHERE ca.cohort_month >= '2023-01-01'
          AND ca.months_since_join <= 11
        ORDER BY 1, 2
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_ltv_distribution():
    sql = """
        SELECT
            customer_id,
            full_name,
            customer_segment,
            customer_tier,
            rfm_segment,
            lifetime_value,
            avg_order_value,
            completed_orders,
            churn_status,
            days_since_last_order
        FROM gold.customer_ltv
        WHERE lifetime_value > 0
        ORDER BY lifetime_value DESC
        LIMIT 500
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_churn_summary():
    sql = """
        SELECT
            churn_status,
            COUNT(customer_id)            AS customers,
            ROUND(SUM(lifetime_value), 2) AS at_risk_revenue,
            ROUND(AVG(lifetime_value), 2) AS avg_ltv
        FROM gold.customer_ltv
        GROUP BY 1
        ORDER BY customers DESC
    """
    return run_query(sql)


try:
    tab1, tab2, tab3, tab4 = st.tabs([
        "🎯 RFM Segments", "📅 Cohort Retention", "💎 LTV Analysis", "⚠️ Churn Signals"
    ])

    # ── Tab 1: RFM Segmentation ────────────────────────────────────────────────
    with tab1:
        df_rfm = load_rfm_segments(seg_list, min_orders)

        rfm_agg = df_rfm.groupby("rfm_segment").agg(
            customer_count=("customer_count", "sum"),
            avg_ltv=("avg_ltv", "mean"),
            total_revenue=("total_revenue", "sum"),
        ).reset_index()

        cola, colb = st.columns([1, 1])
        with cola:
            st.subheader("Customers per Segment")
            fig_treemap = px.treemap(
                rfm_agg,
                path=["rfm_segment"],
                values="customer_count",
                color="avg_ltv",
                color_continuous_scale="RdYlGn",
                hover_data={"customer_count": True, "avg_ltv": ":.2f"},
                labels={"avg_ltv": "Avg LTV ($)"},
            )
            fig_treemap.update_layout(height=450, margin=dict(t=30))
            st.plotly_chart(fig_treemap, use_container_width=True)

        with colb:
            st.subheader("Revenue Contribution by Segment")
            fig_seg = px.bar(
                rfm_agg.sort_values("total_revenue", ascending=True),
                x="total_revenue", y="rfm_segment",
                orientation="h",
                color="total_revenue",
                color_continuous_scale="Blues",
                text=rfm_agg.sort_values("total_revenue", ascending=True)["total_revenue"]
                    .apply(lambda x: f"${x:,.0f}"),
                labels={"total_revenue": "Total Revenue ($)", "rfm_segment": "Segment"},
            )
            fig_seg.update_traces(textposition="outside")
            fig_seg.update_layout(
                coloraxis_showscale=False,
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=450, xaxis_tickprefix="$", xaxis_tickformat=",.0f",
            )
            st.plotly_chart(fig_seg, use_container_width=True)

        # Scatter: RFM bubble
        st.subheader("RFM Bubble Chart — Avg LTV vs Avg Orders")
        fig_bubble = px.scatter(
            rfm_agg,
            x="avg_ltv",
            y="customer_count",
            size="total_revenue",
            color="rfm_segment",
            text="rfm_segment",
            labels={"avg_ltv": "Avg Lifetime Value ($)", "customer_count": "# Customers"},
            color_discrete_sequence=px.colors.qualitative.Set1,
            size_max=60,
        )
        fig_bubble.update_traces(textposition="top center")
        fig_bubble.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            height=380, xaxis_tickprefix="$",
        )
        st.plotly_chart(fig_bubble, use_container_width=True)

    # ── Tab 2: Cohort Retention Heatmap ───────────────────────────────────────
    with tab2:
        st.subheader("Monthly Cohort Retention Heatmap")
        st.caption("% of customers from each signup cohort still active after N months")

        df_cohort = load_cohort_retention()
        if not df_cohort.empty:
            pivot = df_cohort.pivot(
                index="cohort_month",
                columns="months_since_join",
                values="retention_pct",
            ).fillna(0)
            pivot.index = pivot.index.astype(str).str[:7]  # YYYY-MM
            pivot.columns = [f"Month {c}" for c in pivot.columns]

            fig_heat = px.imshow(
                pivot,
                text_auto=".1f",
                color_continuous_scale="RdYlGn",
                aspect="auto",
                labels={"x": "Months Since Signup", "y": "Cohort Month", "color": "Retention %"},
                zmin=0, zmax=100,
            )
            fig_heat.update_layout(height=600, margin=dict(t=40))
            fig_heat.update_xaxes(side="top")
            st.plotly_chart(fig_heat, use_container_width=True)

            # Average retention curve
            st.subheader("Average Retention Curve")
            avg_ret = df_cohort.groupby("months_since_join")["retention_pct"].mean().reset_index()
            fig_ret = px.line(
                avg_ret, x="months_since_join", y="retention_pct",
                markers=True,
                labels={"months_since_join": "Months Since Signup", "retention_pct": "Avg Retention %"},
                color_discrete_sequence=["#2563EB"],
            )
            fig_ret.add_hrule(y=20, line_dash="dot", line_color="red", annotation_text="20% threshold")
            fig_ret.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=300, yaxis_range=[0, 105],
            )
            st.plotly_chart(fig_ret, use_container_width=True)

    # ── Tab 3: LTV Analysis ───────────────────────────────────────────────────
    with tab3:
        df_ltv = load_ltv_distribution()

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("LTV Distribution")
            fig_hist = px.histogram(
                df_ltv, x="lifetime_value",
                nbins=40,
                color="customer_tier",
                labels={"lifetime_value": "Lifetime Value ($)"},
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig_hist.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=370, bargap=0.05, xaxis_tickprefix="$",
            )
            st.plotly_chart(fig_hist, use_container_width=True)

        with col2:
            st.subheader("LTV vs Orders (Segment)")
            fig_scat = px.scatter(
                df_ltv.sample(min(300, len(df_ltv))),
                x="completed_orders", y="lifetime_value",
                color="rfm_segment",
                size="avg_order_value",
                hover_data=["full_name", "customer_tier"],
                labels={"completed_orders": "# Orders", "lifetime_value": "LTV ($)"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig_scat.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=370, yaxis_tickprefix="$",
            )
            st.plotly_chart(fig_scat, use_container_width=True)

        st.subheader("Top 20 Customers by Lifetime Value")
        top20 = df_ltv.head(20)[["full_name", "customer_segment", "customer_tier",
                                   "rfm_segment", "lifetime_value", "completed_orders",
                                   "avg_order_value", "churn_status"]]
        top20["lifetime_value"] = top20["lifetime_value"].apply(lambda x: f"${x:,.2f}")
        top20["avg_order_value"] = top20["avg_order_value"].apply(lambda x: f"${x:,.2f}")
        st.dataframe(top20, use_container_width=True, hide_index=True)

    # ── Tab 4: Churn Signals ──────────────────────────────────────────────────
    with tab4:
        df_churn = load_churn_summary()

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Customer Status Overview")
            fig_churn = px.pie(
                df_churn, names="churn_status", values="customers",
                color_discrete_map={
                    "Active": "#10B981",
                    "Needs Attention": "#F59E0B",
                    "At Risk of Churn": "#EF4444",
                    "Churned": "#6B7280",
                    "Never Purchased": "#A3A3A3",
                },
                hole=0.45,
            )
            fig_churn.update_layout(height=380, paper_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_churn, use_container_width=True)

        with col_b:
            st.subheader("Revenue at Risk by Status")
            fig_risk = px.bar(
                df_churn,
                x="churn_status", y="at_risk_revenue",
                color="churn_status",
                text=df_churn["at_risk_revenue"].apply(lambda x: f"${x:,.0f}"),
                color_discrete_map={
                    "Active": "#10B981",
                    "Needs Attention": "#F59E0B",
                    "At Risk of Churn": "#EF4444",
                    "Churned": "#6B7280",
                    "Never Purchased": "#A3A3A3",
                },
            )
            fig_risk.update_traces(textposition="outside")
            fig_risk.update_layout(
                showlegend=False,
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=380, yaxis_tickprefix="$", yaxis_tickformat=",.0f",
            )
            st.plotly_chart(fig_risk, use_container_width=True)

        st.subheader("At-Risk & Churned Customers Summary")
        at_risk = df_churn[df_churn["churn_status"].isin(["At Risk of Churn", "Churned"])]
        if not at_risk.empty:
            at_risk_display = at_risk.copy()
            at_risk_display["at_risk_revenue"] = at_risk_display["at_risk_revenue"].apply(lambda x: f"${x:,.2f}")
            at_risk_display["avg_ltv"] = at_risk_display["avg_ltv"].apply(lambda x: f"${x:,.2f}")
            st.dataframe(at_risk_display, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error loading customer analytics: {e}")
    st.code(str(e))
