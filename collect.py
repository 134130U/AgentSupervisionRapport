import pandas as pd
import  psycopg2
from psycopg2 import Error

def get_data():
    query2 = open('Scripts/query2.sql')
    stock = open('Scripts/user_stock.sql')
    sup = open('Scripts/Supervisor.sql')
    vente = open('Scripts/ventes.sql')
    ticket = open('Scripts/ticket.sql')
    backlog = open('Scripts/backlog.sql')

    sql_text1 = query2.read()
    sql_text2 = stock.read()
    sql_text3 = vente.read()
    sql_text4 = sup.read()
    sql_text5 = ticket.read()
    sql_text6 = backlog.read()

    # Connection to oolusolar base
    try:
        connection = psycopg2.connect(user='chartio_read_only_user',
                                      password='2ZVF01USUWTKV3K9JJFY',
                                      host='oolu-main-postgresql.cfa4plgxjs0u.eu-central-1.rds.amazonaws.com',
                                      port='5432',
                                      database='oolusolar_analytics')
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print('You are Successfully connected to - ', record, '\n')
    except (Exception, Error) as error:
        print(" Connection failed, try again", error)
        cursor.close()

    df_ag_sup = pd.read_sql_query(sql_text1, connection)
    df_stock = pd.read_sql_query(sql_text2, connection)
    df_vente = pd.read_sql_query(sql_text3, connection)
    df_sup = pd.read_sql_query(sql_text4, connection)
    df_ticket = pd.read_sql_query(sql_text5, connection)
    df_backlog = pd.read_sql_query(sql_text6, connection)

    cursor.close()
    connection.close()

    df_ag_sup.to_csv('Data/ag_sup.csv', index=False)
    df_stock.to_csv('Data/stock.csv', index=False)
    df_vente.to_csv('Data/vente.csv', index=False)
    df_sup.to_csv('Data/superviseur.csv', index=False)
    df_ticket.to_csv('Data/ticket.csv', index=False)
    df_backlog.to_csv('Data/backlog.csv', index=False)

    print('data sucessfully refreshed')
    return ''
