import streamlit as st
import pandas as pd
import snowflake.connector


st.write("Secrets:", st.secrets)
conn= st.connection("snowflake")


st.title("VISUALIZER")
st.subheader("how happy people r")

@st.cache_data

def get_acc_ins():

    query=""" select * from accounts """
    return conn.query(query)

df= get_acc_ins()
st.line_chart(df)


def get_cust_ins():

    query=""" select * from customers """
    return conn.query(query)

df2= get_cust_ins()
st.bar_chart(df2)


def get_trans_ins():

    query=""" select * from trans_type """
    return conn.query(query)

df3= get_trans_ins()
st.bar_chart(df3)



