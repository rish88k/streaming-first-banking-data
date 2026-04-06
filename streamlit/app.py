import streamlit as st
import pandas as pd

st.set_page_config(page_title="Banking Pipeline", layout="wide")
st.title("🏦 Banking Data Intelligence")

# --- CONNECTION ---
@st.cache_resource
def get_connection():
    return st.connection("snowflake")

@st.cache_data(ttl=600)
def run_query(query):
    try:
        conn = get_connection()
        return conn.query(query)
    except Exception as e:
        st.error(f"Query failed: {e}")
        raise e

# --- DEBUG ---
st.write("Current Session Context:")
st.write(run_query("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()"))

# --- SECTION 1 ---
st.header("📈 Transaction Volume Over Time")

df_vol = run_query("""
    SELECT COUNT(transaction_id) AS no_of_transactions,
           TRANSACTION_DATE
    FROM BANKING.PUBLIC_SILVER.SILVER_TRANS
    GROUP BY TRANSACTION_DATE
    ORDER BY TRANSACTION_DATE
""")

st.dataframe(df_vol)

# --- SECTION 2 ---
st.header("🛍️ Spending Categories")

df_cat = run_query("""
    WITH unpacked AS (
        SELECT transaction_id, 
               CASE 
                 WHEN amount_raw < 200 AND amount_raw > 0 THEN 'rainbet'
                 WHEN amount_raw >= 200 AND amount_raw < 1000 THEN 'grocers'
                 WHEN amount_raw >= 1000 THEN 'poker'
                 ELSE 'other' 
               END AS category
        FROM BANKING.PUBLIC_SILVER.SILVER_TRANS
    )
    SELECT COUNT(transaction_id) AS no_of_transactions,
           category 
    FROM unpacked
    GROUP BY category
    ORDER BY no_of_transactions DESC
""")

st.bar_chart(df_cat.set_index('CATEGORY'))

# --- SECTION 3 ---
st.header("💰 Account Health")

df_acc = run_query("""
    WITH unpacked AS (
        SELECT account_id, balance,
               CASE 
                 WHEN balance BETWEEN 0 AND 200 THEN 'Low Balance'
                 WHEN balance BETWEEN 201 AND 1000 THEN 'Mid Balance'
                 WHEN balance > 1000 THEN 'High Balance'
                 ELSE 'Negative' 
               END AS poorness
        FROM BANKING.PUBLIC_SILVER.SILVER_ACC
    )
    SELECT COUNT(account_id) AS no_of_accounts,
           poorness
    FROM unpacked
    GROUP BY poorness
""")

st.write("Distribution of Account Status:")
st.bar_chart(df_acc.set_index('POORNESS'))