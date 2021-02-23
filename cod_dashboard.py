import dash
import dash_core_components as dcc 
import dash_html_components as html 
from dash.dependencies import Input, Output
import plotly.graph_objs as go 
import pandas as pd

stats_df = pd.read_csv('./datasets/players_stats.csv')
list_of_stats = ['wins', 'kd_ratio', 'kills', 'deaths', 'downs', 'damage', 'level']

app = dash.Dash()

app.layout = html.Div([
    html.H1('Cod/WZ Leaderboard Dashboard', style={'textAlign':'center'}),
    html.Div([
        dcc.Graph(id='graph'),
        dcc.Dropdown(id='stat_selector',
                    options=[{'label':stat_name.capitalize(),'value':stat_name} for stat_name in list_of_stats],
                    value='wins')
    ])
])

@app.callback(Output(component_id='graph', component_property='figure'),
            [Input(component_id='stat_selector', component_property='value')])
def update_figure(selected_stat):
    df_by_stat = stats_df[['player', selected_stat]]
    
    data = [go.Bar(x=df_by_stat['player'],
                    y=df_by_stat[selected_stat]
    )]

    layout = go.Layout(title=selected_stat.capitalize() + ' ' + 'in Warzone BR Mode by player')

    return {'data': data, 'layout': layout}


if __name__ == '__main__':
    app.run_server()