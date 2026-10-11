import os

import pandas as pd
import psycopg2
import streamlit as st

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": os.environ.get("POSTGRES_PORT", "5433"),
    "dbname": os.environ.get("POSTGRES_DB", "postgres"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ["POSTGRES_PASSWORD"],
}


@st.cache_resource
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def run_query(sql, params=None):
    return pd.read_sql(sql, get_connection(), params=params)


def list_countries():
    df = run_query("SELECT DISTINCT country_name FROM dim_location ORDER BY country_name")
    return df["country_name"].tolist()


st.set_page_config(page_title="Glamira Page View Dashboard", layout="wide")
st.title("Glamira Page View Dashboard")

report = st.sidebar.radio(
    "Chon bao cao",
    [
        "Top 10 product_id view nhieu nhat hom nay",
        "Top 10 quoc gia view nhieu nhat hom nay",
        "Top 5 referrer_url view nhieu nhat hom nay",
        "Store_id theo quoc gia",
        "View theo gio cua 1 product_id",
        "View theo gio cua browser / os",
    ],
)

if report == "Top 10 product_id view nhieu nhat hom nay":
    df = run_query("""
        SELECT dp.product_id, COUNT(*) AS views
        FROM fact_page_view f
        JOIN dim_product dp ON dp.product_key = f.product_key
        JOIN dim_date dd ON dd.date_key = f.date_key
        WHERE dd.full_date = CURRENT_DATE
        GROUP BY dp.product_id
        ORDER BY views DESC
        LIMIT 10
    """)
    st.bar_chart(df.set_index("product_id"))
    st.dataframe(df)

elif report == "Top 10 quoc gia view nhieu nhat hom nay":
    df = run_query("""
        SELECT dl.country_name, COUNT(*) AS views
        FROM fact_page_view f
        JOIN dim_location dl ON dl.location_key = f.location_key
        JOIN dim_date dd ON dd.date_key = f.date_key
        WHERE dd.full_date = CURRENT_DATE
        GROUP BY dl.country_name
        ORDER BY views DESC
        LIMIT 10
    """)
    st.bar_chart(df.set_index("country_name"))
    st.dataframe(df)

elif report == "Top 5 referrer_url view nhieu nhat hom nay":
    df = run_query("""
        SELECT f.referrer_url, COUNT(*) AS views
        FROM fact_page_view f
        JOIN dim_date dd ON dd.date_key = f.date_key
        WHERE dd.full_date = CURRENT_DATE AND f.referrer_url IS NOT NULL AND f.referrer_url <> ''
        GROUP BY f.referrer_url
        ORDER BY views DESC
        LIMIT 5
    """)
    st.dataframe(df)

elif report == "Store_id theo quoc gia":
    country = st.selectbox("Chon quoc gia", list_countries())
    df = run_query(
        """
        SELECT ds.store_id, COUNT(*) AS views
        FROM fact_page_view f
        JOIN dim_store ds ON ds.store_key = f.store_key
        JOIN dim_location dl ON dl.location_key = f.location_key
        WHERE dl.country_name = %(country)s
        GROUP BY ds.store_id
        ORDER BY views DESC
        """,
        params={"country": country},
    )
    st.bar_chart(df.set_index("store_id"))
    st.dataframe(df)

elif report == "View theo gio cua 1 product_id":
    product_id = st.text_input("Nhap product_id", value="")
    if product_id:
        df = run_query(
            """
            SELECT dd.hour, COUNT(*) AS views
            FROM fact_page_view f
            JOIN dim_product dp ON dp.product_key = f.product_key
            JOIN dim_date dd ON dd.date_key = f.date_key
            WHERE dp.product_id = %(product_id)s AND dd.full_date = CURRENT_DATE
            GROUP BY dd.hour
            ORDER BY dd.hour
            """,
            params={"product_id": product_id},
        )
        st.line_chart(df.set_index("hour"))
        st.dataframe(df)
    else:
        st.info("Nhap product_id de xem bao cao")

elif report == "View theo gio cua browser / os":
    df = run_query("""
        SELECT dd.hour, f.browser, f.os, COUNT(*) AS views
        FROM fact_page_view f
        JOIN dim_date dd ON dd.date_key = f.date_key
        WHERE dd.full_date = CURRENT_DATE
        GROUP BY dd.hour, f.browser, f.os
        ORDER BY dd.hour, views DESC
    """)
    st.subheader("View theo gio cua tung browser")
    st.line_chart(df.pivot_table(index="hour", columns="browser", values="views", aggfunc="sum"))
    st.subheader("View theo gio cua tung os")
    st.line_chart(df.pivot_table(index="hour", columns="os", values="views", aggfunc="sum"))
    st.dataframe(df)
