import calendar
import pandas as pd
import numpy as np
import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import dash_table as dt
import test2
import collect
from apscheduler.schedulers.background import BackgroundScheduler

collect.get_data()

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(func=collect.get_data, trigger="interval", minutes=60)
scheduler.start()


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
                                        columns= [{'name':i, 'id': i} for i in test2.stock_ag_col],
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
                                        columns= [{'name':i, 'id': i} for i in test2.sale_col1],
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
                                        columns=[{'name': i, 'id': i} for i in test2.backlog_col],
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
                                        columns= [{'name':i, 'id': i} for i in test2.week_ticket_col1],
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
    data_rap = test2.stock_ag[test2.stock_ag['agent_id'] == agent]
    table_rap = data_rap[test2.stock_ag_col]

    data_table1 = test2.df_vente[test2.df_vente['agent_id'] == agent]
    data_table1 = data_table1[test2.sale_col]
    total = data_table1.copy()
    total.loc[:,'group_prix'] = 'Total'

    total = total.groupby(['group_prix']).sum().reset_index()

    data_table3 = test2.df_ca[test2.df_ca['agent_id'] == agent]

    data_table3.loc[:,'group_prix'] = 'Revenues Totale en CFA'
    data_table3 = data_table3[test2.sale_col]
    table_sales = pd.concat([data_table1, total,data_table3],sort=False)
    table_sales.loc[:, 'Total du mois'] = table_sales.sum(numeric_only=True, axis=1)

    data_table5 = test2.df_ticket[test2.df_ticket['agent_id'] == agent]
    table_sav = data_table5[test2.week_ticket_col]
    table_sav.loc[:, 'Total du mois'] = table_sav.sum(numeric_only=True, axis=1)

    table_stock = test2.dfstocks[test2.dfstocks['agent_id'] == agent]
    backlog_table = test2.df_backlog[test2.df_backlog['agent_id'] == agent]

    return table_rap.to_dict('records'), table_sales.to_dict('records'), table_stock.to_dict('records'), table_sav.to_dict('records'), backlog_table.to_dict('records')


@app.callback(
    Output('update-connection', 'children'),
    Input('interval-component', 'n_intervals'))


def update_connection(n):

    if n > 0:
        collect.get_data()
        test2.sale_col = test2.get_col(test2.sale_col)
        print('data have been updated')
        return ''

if __name__ == '__main__':
    app.run_server(debug=True,dev_tools_ui=False, host='0.0.0.0', port=8083)
