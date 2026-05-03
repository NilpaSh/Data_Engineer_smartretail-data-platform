"""
DataPulse — Product Performance Page
Top products, margin analysis, category deep-dive, inventory
"""

import sys
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.db_connection import run_query

st.set_page_config(page_title="Product Performance | DataPulse", page_icon="🛍️", layout="wide")
st.title("🛍️ Product Performance")
st.caption("Revenue leaders · Margin analysis · Category drill-down · Inventory health")
st.divider()

# ── Filters ───────────────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
with col1:
    category = st.selectbox("Category", ["All"] + [
        "Electronics", "Clothing", "Home & Kitchen", "Books",
        "Sports & Outdoors", "Beauty & Health", "Toys & Games",
        "Food & Grocery", "Automotive", "Office Supplies"
    ])
with col2:
    top_n = st.slider("Top N Products", 5, 50, 20)
with col3:
    metric = st.selectbox("Rank By", ["Revenue", "Units Sold", "Gross Profit", "Orders"])

cat_cond = f"AND category = '{category}'" if category != "All" else ""
metric_map = {"Revenue": "gross_revenue", "Units Sold": "units_sold",
              "Gross Profit": "gross_profit", "Orders": "total_orders"}
sort_col = metric_map[metric]

st.divider()


@st.cache_data(ttl=300)
def load_top_products(cat_c: str, n: int, sort: str):
    sql = f"""
        SELECT
            product_id,
            product_name,
            category,
            subcategory,
            brand,
            selling_price,
            margin_pct,
            margin_tier,
            stock_status,
            total_orders,
            units_sold,
            gross_revenue,
            gross_profit,
            performance_tier
        FROM gold.dim_products
        WHERE units_sold > 0
          {cat_c}
        ORDER BY {sort} DESC
        LIMIT {n}
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_category_margin(cat_c: str):
    sql = f"""
        SELECT
            category,
            subcategory,
            COUNT(product_id)                 AS product_count,
            ROUND(AVG(margin_pct), 2)         AS avg_margin_pct,
            ROUND(SUM(gross_revenue), 2)      AS total_revenue,
            ROUND(SUM(gross_profit), 2)       AS total_profit,
            SUM(units_sold)                   AS total_units,
            ROUND(AVG(selling_price), 2)      AS avg_price
        FROM gold.dim_products
        WHERE units_sold > 0
          {cat_c}
        GROUP BY 1, 2
        ORDER BY total_revenue DESC
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_margin_distribution():
    sql = """
        SELECT
            margin_tier,
            COUNT(product_id)            AS product_count,
            ROUND(AVG(margin_pct), 2)    AS avg_margin,
            ROUND(SUM(gross_revenue), 2) AS revenue
        FROM gold.dim_products
        WHERE units_sold > 0
        GROUP BY 1
        ORDER BY avg_margin DESC
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_brand_performance(cat_c: str):
    sql = f"""
        SELECT
            brand,
            category,
            COUNT(product_id)                   AS products,
            ROUND(SUM(gross_revenue), 2)        AS revenue,
            ROUND(SUM(gross_profit), 2)         AS profit,
            ROUND(AVG(margin_pct), 2)           AS avg_margin,
            SUM(units_sold)                     AS units_sold
        FROM gold.dim_products
        WHERE units_sold > 0
          {cat_c}
        GROUP BY 1, 2
        ORDER BY revenue DESC
        LIMIT 20
    """
    return run_query(sql)


@st.cache_data(ttl=300)
def load_inventory_health():
    sql = """
        SELECT
            stock_status,
            category,
            COUNT(product_id) AS product_count,
            SUM(stock_quantity) AS total_units
        FROM gold.dim_products
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """
    return run_query(sql)


try:
    tab1, tab2, tab3, tab4 = st.tabs([
        "🏆 Top Products", "📊 Category Analysis", "🏷️ Brand Performance", "📦 Inventory Health"
    ])

    # ── Tab 1: Top Products ───────────────────────────────────────────────────
    with tab1:
        df_top = load_top_products(cat_cond, top_n, sort_col)

        if not df_top.empty:
            # Horizontal bar chart
            col_chart, col_metrics = st.columns([3, 1])

            with col_chart:
                fig_top = px.bar(
                    df_top.sort_values(sort_col, ascending=True).tail(20),
                    x=sort_col, y="product_name",
                    orientation="h",
                    color="margin_pct",
                    color_continuous_scale="RdYlGn",
                    text=df_top.sort_values(sort_col, ascending=True)
                        .tail(20)[sort_col].apply(
                            lambda x: f"${x:,.0f}" if sort_col in ("gross_revenue", "gross_profit") else f"{x:,}"
                        ),
                    labels={sort_col: metric, "product_name": "Product", "margin_pct": "Margin %"},
                    hover_data=["category", "brand", "units_sold", "margin_pct"],
                )
                fig_top.update_traces(textposition="outside")
                fig_top.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    height=max(400, top_n * 22),
                    yaxis=dict(tickmode="linear"),
                    coloraxis_colorbar=dict(title="Margin %"),
                )
                if sort_col in ("gross_revenue", "gross_profit"):
                    fig_top.update_xaxes(tickprefix="$", tickformat=",.0f")
                st.plotly_chart(fig_top, use_container_width=True)

            with col_metrics:
                st.metric("Total Revenue", f"${df_top['gross_revenue'].sum():,.0f}")
                st.metric("Total Profit", f"${df_top['gross_profit'].sum():,.0f}")
                st.metric("Avg Margin", f"{df_top['margin_pct'].mean():.1f}%")
                st.metric("Units Sold", f"{df_top['units_sold'].sum():,}")

                st.divider()
                st.subheader("Performance Tiers")
                tier_counts = df_top["performance_tier"].value_counts()
                for tier, count in tier_counts.items():
                    st.caption(f"{tier}: {count}")

            # Scatter: Price vs Margin
            st.subheader("Price vs Margin (Bubble = Revenue)")
            fig_scatter = px.scatter(
                df_top,
                x="selling_price", y="margin_pct",
                size="gross_revenue", color="category",
                text="brand",
                hover_data=["product_name", "units_sold"],
                labels={"selling_price": "Selling Price ($)", "margin_pct": "Margin (%)"},
                color_discrete_sequence=px.colors.qualitative.Set2,
                size_max=50,
            )
            fig_scatter.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=400, xaxis_tickprefix="$",
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

    # ── Tab 2: Category Analysis ──────────────────────────────────────────────
    with tab2:
        df_cat = load_category_margin(cat_cond)
        df_margin = load_margin_distribution()

        cola, colb = st.columns(2)

        with cola:
            st.subheader("Revenue vs Margin by Category")
            cat_agg = df_cat.groupby("category").agg(
                total_revenue=("total_revenue", "sum"),
                avg_margin_pct=("avg_margin_pct", "mean"),
                total_units=("total_units", "sum"),
            ).reset_index()
            fig_cat = px.scatter(
                cat_agg,
                x="avg_margin_pct", y="total_revenue",
                size="total_units", color="category",
                text="category",
                labels={"avg_margin_pct": "Avg Margin (%)", "total_revenue": "Revenue ($)"},
                color_discrete_sequence=px.colors.qualitative.Pastel,
                size_max=60,
            )
            fig_cat.update_traces(textposition="top center")
            fig_cat.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=400, yaxis_tickprefix="$", yaxis_tickformat=",.0f",
                showlegend=False,
            )
            st.plotly_chart(fig_cat, use_container_width=True)

        with colb:
            st.subheader("Margin Tier Distribution")
            fig_margin = px.bar(
                df_margin,
                x="margin_tier", y="product_count",
                color="avg_margin",
                color_continuous_scale="RdYlGn",
                text="product_count",
                labels={"margin_tier": "Margin Tier", "product_count": "# Products"},
            )
            fig_margin.update_traces(textposition="outside")
            fig_margin.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                height=400, coloraxis_colorbar=dict(title="Avg Margin %"),
            )
            st.plotly_chart(fig_margin, use_container_width=True)

        # Sunburst: Category → Subcategory
        st.subheader("Revenue Breakdown: Category → Subcategory")
        fig_sun = px.sunburst(
            df_cat,
            path=["category", "subcategory"],
            values="total_revenue",
            color="avg_margin_pct",
            color_continuous_scale="RdYlGn",
            labels={"avg_margin_pct": "Avg Margin %", "total_revenue": "Revenue ($)"},
        )
        fig_sun.update_layout(height=550, margin=dict(t=30))
        st.plotly_chart(fig_sun, use_container_width=True)

    # ── Tab 3: Brand Performance ───────────────────────────────────────────────
    with tab3:
        df_brand = load_brand_performance(cat_cond)

        st.subheader("Top 20 Brands by Revenue")
        fig_brand = go.Figure()
        fig_brand.add_trace(go.Bar(
            x=df_brand["brand"], y=df_brand["revenue"],
            name="Revenue", marker_color="#2563EB",
            text=df_brand["revenue"].apply(lambda x: f"${x:,.0f}"),
            textposition="outside",
        ))
        fig_brand.add_trace(go.Bar(
            x=df_brand["brand"], y=df_brand["profit"],
            name="Gross Profit", marker_color="#10B981",
        ))
        fig_brand.update_layout(
            barmode="group",
            xaxis_tickangle=-40,
            yaxis_tickprefix="$", yaxis_tickformat=",.0f",
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            height=450, legend=dict(orientation="h", y=1.0),
        )
        st.plotly_chart(fig_brand, use_container_width=True)

        st.subheader("Brand Detail Table")
        df_brand_display = df_brand.copy()
        df_brand_display["revenue"] = df_brand_display["revenue"].apply(lambda x: f"${x:,.2f}")
        df_brand_display["profit"] = df_brand_display["profit"].apply(lambda x: f"${x:,.2f}")
        df_brand_display["avg_margin"] = df_brand_display["avg_margin"].apply(lambda x: f"{x:.1f}%")
        df_brand_display.columns = ["Brand", "Category", "Products", "Revenue", "Profit", "Avg Margin %", "Units"]
        st.dataframe(df_brand_display, use_container_width=True, hide_index=True)

    # ── Tab 4: Inventory Health ────────────────────────────────────────────────
    with tab4:
        df_inv = load_inventory_health()

        st.subheader("Inventory Status by Category")
        fig_inv = px.bar(
            df_inv,
            x="category", y="product_count",
            color="stock_status",
            barmode="stack",
            color_discrete_map={
                "Well Stocked": "#10B981",
                "Normal Stock": "#3B82F6",
                "Low Stock": "#F59E0B",
                "Out of Stock": "#EF4444",
            },
            labels={"product_count": "# Products", "category": "Category"},
        )
        fig_inv.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            height=420, xaxis_tickangle=-30,
            legend_title="Stock Status",
        )
        st.plotly_chart(fig_inv, use_container_width=True)

        st.subheader("Out of Stock Alert")
        try:
            df_oos = run_query("""
                SELECT product_id, product_name, category, brand, units_sold, gross_revenue
                FROM gold.dim_products
                WHERE stock_status = 'Out of Stock'
                  AND units_sold > 0
                ORDER BY gross_revenue DESC
                LIMIT 20
            """)
            if not df_oos.empty:
                df_oos["gross_revenue"] = df_oos["gross_revenue"].apply(lambda x: f"${x:,.2f}")
                st.warning(f"⚠️ {len(df_oos)} high-selling products are OUT OF STOCK!")
                st.dataframe(df_oos, use_container_width=True, hide_index=True)
            else:
                st.success("✓ No high-selling products are out of stock")
        except Exception:
            pass

except Exception as e:
    st.error(f"Error loading product data: {e}")
    st.code(str(e))
