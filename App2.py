import calendar
import pandas as pd
import numpy as np
import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table as dt
import  psycopg2
from psycopg2 import Error
from sqlalchemy import text
from datetime import date
from datetime import timedelta
import collect
import atexit

from apscheduler.schedulers.background import BackgroundScheduler

collect.get_data()

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(func=collect.get_data, trigger="interval", minutes=60)
scheduler.start()


df_ag_sup= pd.read_csv('Data/ag_sup.csv')
df_stock= pd.read_csv('Data/stock.csv')
df_vente= pd.read_csv('Data/vente.csv')
df_sup = pd.read_csv('Data/superviseur.csv')
df_ticket= pd.read_csv('Data/ticket.csv')
df_backlog= pd.read_csv('Data/backlog.csv')

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
df_ag_stock1 = df_ag_stock1[stock_feat]
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

df_vente = pd.merge(week_sales,df_daysales, how= 'left', on= ['agent_id','group_prix'])
df_vente.fillna(0,inplace=True)

sale_col = list(df_vente.columns)
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


#==================================================  Revenues  ====================================================================

week_ca =pd.pivot_table(df_vente_agent,values='CA',index=['annee','mois','agent_id','agent','superviseur'],columns='Weeks', aggfunc=np.sum).reset_index()
week_ca.fillna(0,inplace=True)
week_ca = week_ca[(week_ca['annee'] == yesterday.year)&(week_ca['mois'] == yesterday.month)]

day_ca = pd.pivot_table(df_vente_agent,values='CA',index=['annee','mois','agent_id','agent','superviseur','Weeks'],columns='Day', aggfunc=np.sum).reset_index()
day_ca.fillna(0,inplace=True)

feat_sales = ['agent_id','Mon','Tue', 'Wed','Thu', 'Sat', 'Sun']
df_day_ca = day_ca[feat_sales][(day_ca['annee'] == yesterday.year)&(day_ca['mois'] == yesterday.month)&(day_ca['Weeks'] == week_int2(yesterday))]


df_ca = pd.merge(week_ca,df_day_ca, how= 'left', on= 'agent_id')
df_ca.fillna(0,inplace=True)


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

week_ticket_col = list(df_ticket.columns)

week_ticket_col.remove('superviseur')
week_ticket_col.remove('agent')
week_ticket_col.remove('agent_id')
week_ticket_col.remove('annee')
week_ticket_col.remove('mois')


week_ticket_col = get_col(week_ticket_col)
#=============================================== Backlog =====================================================================
backlog_col = list(df_backlog.columns)
backlog_col.remove('agent')
backlog_col.remove('agent_id')


sale_col1 = sale_col + ['Total du mois']
week_ticket_col1 = week_ticket_col + ['Total du mois']


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
                        dcc.Interval(
                                id='interval-component',
                                interval=120*1000, # in milliseconds
                                n_intervals=0
                            ),
                        dcc.Location(id='url', refresh=False),
                            dbc.Row([
                                html.Div([dbc.Col(
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
                                    html.H1('Rapport Agent',
                                            style={
                                                'font-size': '450%',
                                                'textAlign': 'center',
                                                'color': '#090808'
                                                }),
                                ],width={'size': 5, 'offset': 3},)
                                ], className="card_container twelve columns",)
                        ]),
                        html.Br(),
                        html.Div([
                            dbc.Row([
                                html.Div([dbc.Col(
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
                                        style_cell={'minWidth': '0px', 'maxWidth': '90px','width':'30px','fontSize':12,
                                                    'textAlign': 'center',"font-family":"Montserrat"}

                                    )
                                ],width={'size': 9,'offset':2})
                                ], className="card_container twelve columns",)
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
                                    ),
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
                                        columns= [{'name':i, 'id': i} for i in sale_col1],
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
                                ],width={'size': 9,'offset':0}),
                            ]),
                            html.Br(),
                            dbc.Row([
                                dbc.Col([
                                    html.Br(),
                                    html.H3(children='Backlog',
                                            style={
                                                'textAlign': 'center',
                                                'color': '#090808'}
                                            ),
                                    html.Br(),
                                    dt.DataTable(
                                        id = 'backlog',
                                        style_header=table_header_style,
                                        columns=[{'name': i, 'id': i} for i in backlog_col],
                                        style_table={'height': '150px', 'overflowY': 'auto',"font-family":"Montserrat"},
                                        style_cell={'minWidth': '0px', 'maxWidth': '100px','width':'50px','fontSize': 11, 'textAlign': 'center',"font-family":"Montserrat"}
                                    ),
                                ],width={'size': 3,'offset':0}),
                                dbc.Col([
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
                                        columns= [{'name':i, 'id': i} for i in week_ticket_col1],
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
                    html.Div(
                                id='update-connection'
                            ),
                    html.Div(html.A(html.Button('Retour',className='one columns'),href='http://212.47.246.218:8030/'))

])


@app.callback(
    Output('ag_rap', 'data'),
    Output('week_sales', 'data'),
    Output('ag_stock', 'data'),
    Output('week_rep', 'data'),
    Output('backlog', 'data'),
    Input('url', 'pathname'))


def update_table(pathname):
    agent = int(pathname.split('/')[1])
    data_rap = stock_ag[stock_ag['agent_id'] == agent]
    table_rap = data_rap[stock_ag_col]

    data_table1 = df_vente[df_vente['agent_id'] == agent]
    data_table1 = data_table1[sale_col]
    total = data_table1.copy()
    total.loc[:,'group_prix'] = 'Total'

    total = total.groupby(['group_prix']).sum().reset_index()

    data_table3 = df_ca[df_ca['agent_id'] == agent]

    data_table3.loc[:,'group_prix'] = 'Revenues Totale en CFA'
    data_table3 = data_table3[sale_col]
    table_sales = pd.concat([data_table1, total,data_table3],sort=False)
    table_sales.loc[:, 'Total du mois'] = table_sales.sum(numeric_only=True, axis=1)

    data_table5 = df_ticket[df_ticket['agent_id'] == agent]
    table_sav = data_table5[week_ticket_col]
    table_sav.loc[:, 'Total du mois'] = table_sav.sum(numeric_only=True, axis=1)

    table_stock = dfstocks[dfstocks['agent_id'] == agent]
    backlog_table = df_backlog[df_backlog['agent_id'] == agent]

    return table_rap.to_dict('records'), table_sales.to_dict('records'), table_stock.to_dict('records'), table_sav.to_dict('records'), backlog_table.to_dict('records')


@app.callback(
    Output('update-connection', 'children'),
    Input('interval-component', 'n_intervals'))


def update_connection(n):

    global sale_col
    if n > 0:
        collect.get_data()
        sale_col = get_col(sale_col)
        print('data have been updated')
        return ''

if __name__ == '__main__':
    app.run_server(debug=True,dev_tools_ui=False, host='0.0.0.0', port=8083)
