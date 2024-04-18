import streamlit as st
import psycopg2
import paramiko
from sshtunnel import SSHTunnelForwarder
import pandas as pd
from datetime import date

today = date.today()

with st.spinner('Querying Database...'):
    pkey = paramiko.Ed25519Key.from_private_key_file('keys/id_ed25519')
    tunnel = SSHTunnelForwarder(
        (st.secrets["tunnel_ip"], 22),
        ssh_username='ubuntu',
        ssh_pkey=pkey,
        remote_bind_address=(st.secrets['db_ip'], 5432),
        local_bind_address=('localhost',6544)
    )
    tunnel.start()
    conn = psycopg2.connect(
        database='user_service_production',
        user='postgres',
        host=tunnel.local_bind_host,
        port=tunnel.local_bind_port,
        password=st.secrets["db_pass"]
    )
    sql_query = \
    """
    select id, full_name, email, ktp_number
    from 
    ( select *, 
            count(1) over (partition by device_token) as occurs
        from public.user
    ) AS t 
    where occurs > 10 and is_banned = False and device_token is not null
    ;"""
    users = pd.read_sql_query(sql_query, conn)
    @st.cache_data
    def convert_df(df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

    csv = convert_df(users)

st.metric(label="Suspected Unbanned Scalpers", value=len(users))

st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name='scalper_list_{0}.csv'.format(today),
    mime='text/csv',
)