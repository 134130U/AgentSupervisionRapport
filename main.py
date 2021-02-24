import calendar
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px
from dash.dependencies import Input, Output
import dash_table as dt
import  psycopg2
from psycopg2 import Error
from sqlalchemy import text
from datetime import date
from datetime import timedelta

query2 = open('Scripts/query2.sql')
stock = open('Scripts/user_stock.sql')
sup = open('Scripts/Supervisor.sql')
vente = open('Scripts/ventes.sql')
ticket = open('Scripts/ticket.sql')

sql_text1 = query2.read()
sql_text2 = stock.read()
sql_text3 = vente.read()
sql_text4 = sup.read()
sql_text5 = ticket.read()

# Connection to oolusolar base
try:
    connection = psycopg2.connect(user = 'chartio_read_only_user',
                                  password = '2ZVF01USUWTKV3K9JJFY',
                                  host = 'oolu-main-postgresql.cfa4plgxjs0u.eu-central-1.rds.amazonaws.com',
                                  port ='5432',
                                  database = 'oolusolar_analytics')
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print('You are Successfully connected to - ',record, '\n')
except (Exception, Error) as error:
    print(" Connection failed, try again", error)
    cursor.close()

df_ag_sup  = pd.read_sql_query(sql_text1 , connection)
df_stock  = pd.read_sql_query(sql_text2 , connection)
df_vente  = pd.read_sql_query(sql_text3 , connection)
df_sup  = pd.read_sql_query(sql_text4 , connection)
df_ticket  = pd.read_sql_query(sql_text5 , connection)

cursor.close()
connection.close()


table_header_style = {
    # "backgroundColor": "#E5E2E2",
    "color": "balck",
    "textAlign": "center",
    'fontSize': 14,
    "font-family":"Montserrat"
}
table_rapport_style = {
    # "backgroundColor": "#E5E2E2",
    "color": "black",
    "textAlign": "center",
    'fontSize': 18,"font-family":"Montserrat"
}

yesterday = date.today()-timedelta(days=1)
df_vente['created_at']= pd.to_datetime(df_vente['created_at'])
df_ag_sup['date_debut']= pd.to_datetime(df_ag_sup['date_debut'])
df_ticket['closed_date']= pd.to_datetime(df_ticket['closed_date'])
df_ag_stock = pd.merge(df_ag_sup,df_stock, how= 'left', on= 'agent_id')

#======================================== Rapport Agents =================================================================

agent_feat = ['agent_id','agent','date_debut','agent_role','superviseur','zone','stock']
df_ag_stock = df_ag_stock[agent_feat]
df_ag_stock.fillna(0,inplace=True)
stock_ag = df_ag_stock.groupby(['agent_id','agent','date_debut','superviseur','zone','agent_role']).sum().reset_index()
stock_ag_col = list(stock_ag.columns)
stock_ag_col.remove('agent_id')
#============================================ Agent Stock product=========================================================
df_ag_stock1 = pd.merge(df_ag_sup,df_stock, how= 'inner', on= 'agent_id')
stock_feat = ['agent_id','agent','superviseur','category','product_type','stock']
df_ag_stock1=df_ag_stock1[stock_feat]
df_ag_stock1['Type de produit'] = [df_ag_stock1['product_type'].iloc[i] if df_ag_stock1['category'].iloc[i] in [None]
                                   else df_ag_stock1['category'].iloc[i] for i in range(df_ag_stock1.shape[0]) ]
dfstocks = df_ag_stock1.groupby(['agent_id','agent','superviseur','Type de produit']).sum().reset_index()

stock_col = list(dfstocks.columns)
stock_col.remove('agent_id')
#======================================= Ventes mensuelles des Agents ====================================================

df_vente_agent = pd.merge(df_vente,df_ag_sup[['agent_id','superviseur','zone']], how= 'inner', on= 'agent_id')


def week_int2(x):
    y =x.year
    m = x.month
    d = x.day
    list_week = calendar.monthcalendar(y,m)
    for i in range(len(list_week)):
        if d in list_week[i]:
            return 'week_'+str(i+1)


df_vente_agent['Weeks'] = df_vente_agent['created_at'].apply(week_int2)
df_vente_agent['Month'] = df_vente_agent['created_at'].apply(lambda x: x.strftime('%b'))
df_vente_agent['Day'] = df_vente_agent['created_at'].apply(lambda x: x.strftime('%a'))
df_vente_agent['CA'] = df_vente_agent['deposit']*df_vente_agent['ventes']

feature = ['annee','mois','agent_id','agent','group_prix','superviseur','zone','Weeks','Month','Day','CA','ventes']
df_vente_agent_w = pd.pivot_table(df_vente_agent,values='ventes',index=['annee','mois','agent_id','agent','group_prix','superviseur'],columns='Weeks', aggfunc=np.sum).reset_index()
df_vente_agent_w.fillna(0,inplace=True)

df_vente_agent_d = pd.pivot_table(df_vente_agent,values='ventes',index=['annee','mois','agent_id','agent','group_prix','superviseur','Weeks'],columns='Day', aggfunc=np.sum).reset_index()
df_vente_agent_d.fillna(0,inplace=True)

week_sales = df_vente_agent_w[(df_vente_agent_w['annee'] == yesterday.year)&(df_vente_agent_w['mois']==yesterday.month)]
feat_sales = ['agent_id','group_prix','Mon','Tue', 'Wed','Thu', 'Sat', 'Sun']
df_daysales = df_vente_agent_d[feat_sales][(df_vente_agent_d['annee'] == yesterday.year)&(df_vente_agent_d['mois']==yesterday.month)&(df_vente_agent_d['Weeks']==week_int2(yesterday))]
print(df_daysales.columns)
df_vente = pd.merge(week_sales,df_daysales, how= 'left', on= ['agent_id','group_prix'])
df_vente.fillna(0,inplace=True)
# df_vente = df_vente.drop(week_int2(yesterday),axis=1)

sale_col = list(df_vente.columns)
print(sale_col)
sale_col.remove('superviseur')
sale_col.remove('agent')
sale_col.remove('agent_id')
sale_col.remove('annee')
sale_col.remove('mois')

def get_col(list_col):
    yesterday = date.today() - timedelta(days=1)
    week_n = week_int2(yesterday).split('_')[1]
    for i in range(8):
        if i>=int(week_n):
            dropweek = 'week_'+str(i)
            if dropweek in list_col:
                list_col.remove(dropweek)
    return list_col
sale_col = get_col(sale_col)


#-----------------------------------------------------------------------------------------------------------------------------------
#==================================================  Revenues  ====================================================================

week_ca =pd.pivot_table(df_vente_agent,values='CA',index=['annee','mois','agent_id','agent','superviseur'],columns='Weeks', aggfunc=np.sum).reset_index()
week_ca.fillna(0,inplace=True)
week_ca = week_ca[(week_ca['annee'] == yesterday.year)&(week_ca['mois']==yesterday.month)]

day_ca = pd.pivot_table(df_vente_agent,values='CA',index=['annee','mois','agent_id','agent','superviseur','Weeks'],columns='Day', aggfunc=np.sum).reset_index()
day_ca.fillna(0,inplace=True)

feat_sales = ['agent_id','Mon','Tue', 'Wed','Thu', 'Sat', 'Sun']
df_day_ca = day_ca[feat_sales][(day_ca['annee'] == yesterday.year)&(day_ca['mois']==yesterday.month)&(day_ca['Weeks']==week_int2(yesterday))]


df_ca = pd.merge(week_ca,df_day_ca, how= 'left', on= 'agent_id')
df_ca.fillna(0,inplace=True)
df_ca = df_ca.drop(week_int2(yesterday),axis=1)


week_ca_col = list(df_ca.columns)
if date.today().weekday() ==0:
    if  week_int2(date.today()) in week_ca_col:
        week_ca_col.remove(week_int2(date.today()))
else:
    pass
week_ca_col.remove('superviseur')
week_ca_col.remove('agent')


day_ca_col = list(day_ca.columns)
day_ca_col.remove('superviseur')
day_ca_col.remove('agent')
day_ca_col.remove('agent_id')

#========================================= Tickets ==================================================================================
df_ticket_sup = pd.merge(df_ticket,df_ag_sup[['agent_id','superviseur','zone']], how= 'inner', on= 'agent_id')
df_ticket_sup['Weeks'] = df_ticket_sup['closed_date'].apply(week_int2)
df_ticket_sup['Month'] = df_ticket_sup['closed_date'].apply(lambda x: x.strftime('%b'))
df_ticket_sup['Day'] = df_ticket_sup['closed_date'].apply(lambda x: x.strftime('%a'))

feature1 = ['closed_date','annee','mois','agent_id','agent','superviseur','status','type_ticket','zone','Weeks','Month','Day','ticket']

df_ticket_mois = df_ticket_sup[feature1][(df_ticket_sup['annee']==yesterday.year)&(df_ticket_sup['mois']==yesterday.month)]

week_ticket = pd.pivot_table(df_ticket_sup,values='ticket',index=['annee','mois','agent_id','agent','superviseur','type_ticket','status'],columns='Weeks', aggfunc=np.sum).reset_index()
week_ticket.fillna(0,inplace=True)
week_ticket =week_ticket[(week_ticket['annee']==yesterday.year)&(week_ticket['mois']==yesterday.month)]
feat_ticket = ['agent_id', 'type_ticket', 'status','Mon','Tue', 'Wed','Thu', 'Sat', 'Sun']
day_ticket = pd.pivot_table(df_ticket_sup,values='ticket',index=['annee','mois','agent_id','agent','superviseur','type_ticket','status','Weeks'],columns='Day', aggfunc=np.sum).reset_index()
day_ticket.fillna(0,inplace=True)
df_day_ticket = day_ticket[feat_ticket][(day_ticket['annee']==yesterday.year)&(day_ticket['mois']==yesterday.month)&(day_ticket['Weeks']==week_int2(yesterday))]


df_ticket = pd.merge(week_ticket,df_day_ticket, how= 'left', on= ['agent_id','type_ticket','status'])
df_ticket.fillna(0,inplace=True)
df_ticket = df_ticket.drop(week_int2(yesterday),axis=1)


week_ticket_col = list(df_ticket.columns)
if date.today().weekday() ==0:
    if week_int2(date.today()) in week_ticket_col:
        week_ticket_col.remove(week_int2(date.today()))
        print(week_int2(date.today()))
else:
    pass
print(week_ticket_col)
week_ticket_col.remove('superviseur')
week_ticket_col.remove('agent')
week_ticket_col.remove('agent_id')
week_ticket_col.remove('annee')
week_ticket_col.remove('mois')

week_ticket_col = get_col(week_ticket_col)


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([dcc.Location(id='url', refresh=False),
                        dbc.Row([
                            dbc.Col(
                                html.Div([
                                        html.Img(src=app.get_asset_url('oolulogo.png'),
                                             id='oolu-logo',
                                             style={
                                                 "height": "60px",
                                                 "width": "auto",
                                                 "margin-bottom": "25px",},
                                     )],className="one-third column",
                                ),width={'size': 1}),
                            dbc.Col([
                                html.H1('Rapport Agents',
                                        style={
                                            'font-size': '450%',
                                            'textAlign': 'center',
                                            'color': '#090808'
                                            }),
                            ],width={'size': 5, 'offset': 3},)
                        ]),
                        html.Br(),
                        html.Div([
                            dbc.Row([
                                dbc.Col(
                                    [html.H2(children='Overview Agent',
                                                style={
                                                    'textAlign': 'center',
                                                    'color': '#090808'}
                                                ),
                                    dt.DataTable(
                                        id = 'ag_rap',
                                        style_header=table_rapport_style,
                                        columns= [{'name':i, 'id': i} for i in stock_ag_col],
                                        style_table={'border-collapse': 'collapse','height': '150px', 'overflowY': 'auto',"font-family":"Montserrat"},
                                        style_cell={'minWidth': '0px', 'maxWidth': '90px','width':'30px','fontSize':11,
                                                    'textAlign': 'center',"font-family":"Montserrat"}

                                    )
                                ],width={'size': 9,'offset':2})
                            ]),
                            dbc.Row([
                                dbc.Col([html.Br(),
                                         html.H3(children='Stocks Agent',
                                                style={
                                                    'textAlign': 'center',
                                                    'color': '#090808'}
                                                ),
                                         html.Br(),
                                    dt.DataTable(
                                        id = 'ag_stock',
                                        style_header=table_header_style,
                                        columns=[{'name': i, 'id': i} for i in ['Type de produit','stock']],
                                        # data=df1.to_dict('records'),
                                        style_table={'height': '150px', 'overflowY': 'auto',"font-family":"Montserrat"},
                                        style_cell={'minWidth': '0px', 'maxWidth': '100px','width':'50px','fontSize': 11, 'textAlign': 'center',"font-family":"Montserrat"}
                                    )
                                ],width={'size': 3,'offset':0}),
                                dbc.Col([html.H3(children='Agent Sales',
                                                style={
                                                    'textAlign': 'center',
                                                    'color': '#090808'}
                                                ),
                                         html.H5(children='Semaine en cours',
                                                style={
                                                    'textAlign': 'right',
                                                    'color': '#090808'}
                                                ),
                                    dt.DataTable(
                                        id = 'week_sales',
                                        style_header=table_header_style,
                                        columns= [{'name':i, 'id': i} for i in sale_col],
                                        # data=df1.to_dict('records'),
                                        style_table={'height': '280px', 'overflowY': 'auto',"font-family":"Montserrat"},
                                        style_cell={'minWidth': '0px', 'maxWidth': '100px','width':'40px','fontSize':11,  'textAlign': 'center',"font-family":"Montserrat"},
                                        style_cell_conditional=[
                                                                    {
                                                                        'if': {'column_id': 'group_prix'},
                                                                        'maxWidth': '180px','width':'150px','fontSize':11,  'textAlign': 'center',"font-family":"Montserrat"
                                                                    }
                                                                ]

                                    ),
                                    html.H3(children='SAV',
                                                style={
                                                    'textAlign': 'center',
                                                    'color': '#090808'}
                                                ),
                                         html.H5(children='Semaine en cours',
                                                style={
                                                    'textAlign': 'right',
                                                    'color': '#090808'}
                                                ),
                                    dt.DataTable(
                                        id = 'week_rep',
                                        style_header=table_header_style,
                                        columns= [{'name':i, 'id': i} for i in week_ticket_col],
                                        # data=df1.to_dict('records'),
                                        style_table={'height': '150px', 'overflowY': 'auto',"font-family":"Montserrat"},
                                        style_cell={'minWidth': '0px', 'maxWidth': '100px','width':'40px','fontSize':11,"font-family":"Montserrat", 'textAlign': 'center'},
                                        style_cell_conditional=[
                                                                    {
                                                                        'if': {'column_id': 'type_ticket'},
                                                                        'maxWidth': '180px','width':'150px','fontSize':11,  'textAlign': 'center',"font-family":"Montserrat"
                                                                    }
                                                                ]
                                    )
                                ],width={'size': 9,'offset':0}),
                            ]),
                        ]),

])


@app.callback(
    Output('ag_rap', 'data'),
    Input('url', 'pathname'))
def update_table(pathname):
    agent = int(pathname.split('/')[1])
    data_table1 = stock_ag[stock_ag['agent_id'] == agent]
    data_table1 = data_table1[stock_ag_col]
    return data_table1.to_dict('records')


@app.callback(
    Output('week_sales', 'data'),
    Output('ag_stock', 'data'),
    Output('week_rep', 'data'),
    Input('url', 'pathname'))


def update_table(pathname):
    agent = int(pathname.split('/')[1])
    data_table1 = df_vente[df_vente['agent_id'] == agent]
    print(data_table1.head())
    data_table1 = data_table1[sale_col]
    total = data_table1.copy()
    total['group_prix'] = ['Total' for i in range(len(total))]
    total = total.groupby(['group_prix']).sum().reset_index()

    data_table3 = df_ca[df_ca['agent_id'] == agent]
    print(data_table3.head())
    data_table3 = data_table3[week_ca_col]
    data_table3['group_prix'] = ['Revenues Totale en CFA' for i in range(data_table3.shape[0])]
    data_table1 = pd.concat([data_table1, total,data_table3],sort=False)

    data_table5 = df_ticket[df_ticket['agent_id'] == agent]
    data_table5 = data_table5[week_ticket_col]

    data_table6 = dfstocks[dfstocks['agent_id'] == agent]

    return data_table1.to_dict('records'), data_table6.to_dict('records'), data_table5.to_dict('records')


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8083)