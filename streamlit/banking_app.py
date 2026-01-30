import streamlit as st
import pandas as pd
import snowflake.connector

# 1. Page Config
st.set_page_config(page_title="Banking Data Stack", layout="wide")
st.title("🏦 Banking Operations Dashboard")

# 2. Connection Function (Best practice: use st.cache to avoid constant re-loading)
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user='<USER>',
        password='<PASSWORD>',
        account='<ACCOUNT_LOCATOR>',
        warehouse='COMPUTE_WH',
        database='BANKING_DB',
        schema='GOLD'
    )

conn = init_connection()

# 3. Pull Data from your dbt Gold Layer
query = "SELECT * FROM FCT_TRANSACTIONS ORDER BY TRANSACTION_AT DESC"
df = pd.read_sql(query, conn)

# 4. Building the UI
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Transaction Volume", f"${df['AMOUNT'].sum():,.2f}")
with col2:
    st.metric("Total Transactions", len(df))
with col3:
    st.metric("Avg Transaction Size", f"${df['AMOUNT'].mean():.2f}")

st.divider()

# 5. Visualizations
st.subheader("Transaction Trends Over Time")
# Convert to datetime for plotting
df['TRANSACTION_AT'] = pd.to_datetime(df['TRANSACTION_AT'])
chart_data = df.set_index('TRANSACTION_AT')['AMOUNT'].resample('H').sum()
st.line_chart(chart_data)

st.subheader("Recent High-Value Transactions")
st.dataframe(df[df['AMOUNT'] > 500].head(10))