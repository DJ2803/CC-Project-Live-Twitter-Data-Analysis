#CarCompany Feature Key Words  
import os
from json import load
import pandas as pd
import plotly.express as px
import warnings
import dash
from dash import dcc, html
import pandas as pd
import plotly.express as px
from dash.dependencies import Input, Output
import warnings
import boto3
import os
import json
from json import JSONEncoder

masterDf = None
warnings.filterwarnings('ignore')

listCarCompanies = ['Chevrolet', 'Honda', 'Hyundai', 'Kia', 'Mazda', 'Nissan']
features = ['speed', 'safety', 'comfort', 'security']
featureLables = ['Speed', 'Safety', 'Comfort', 'Security']

session = boto3.Session(
                         aws_access_key_id='',
                         aws_secret_access_key='')

s3 = session.resource('s3')

object = s3.Object(
        bucket_name='ccfinalproject01', 
        key='CARData/CARData.json' #folder
)
resultGet = object.get()

datatest = resultGet['Body'].read().decode('utf-8') 
json_data = json.loads(datatest)

df = pd.json_normalize(json_data)
df = df.groupby(['authorId'], as_index=False).agg({'text': ' '.join})

import re
out = df.merge(pd.Series(features, name='feature'), how='cross')

out['text'] = out['text'].apply(str.lower)

Occurances = []
for index, row in out.iterrows():
    Occurances.append(out["text"][index].count(out['feature'][index], re.I))
    
out["count"] = Occurances 

test1 = out.replace([66395780, 43430484, 26007726, 23689478, 88803528, 33645850],['Chevrolet', 'Honda', 'Hyundai', 'Kia', 'Mazda', 'Nissan'])

#Visualization

pdf = test1

feature = pdf['feature'].unique()

app = dash.Dash(__name__)
application = app.server

app.layout = html.Div([
    html.Div([
        html.Label(['Test']),
        dcc.Dropdown(
            id='my_dropdown',
            options=[{'label': y, 'value': y} for y in feature],
            value=feature[0],
            multi=False,
            clearable=False,
            style={"width": "50%"}
        )
    ]),

    html.Div([
        dcc.Graph(id='the_graph')
        
    ])

])


@app.callback(
    Output(component_id='the_graph', component_property='figure'),
    [Input(component_id='my_dropdown', component_property='value')]
)
def update_graph(feature):
    piechart = px.pie(
        pdf[pdf['feature'] == feature],
        values='count',
        names='authorId'
    )
    return piechart
    
    

if __name__ == '__main__':
    application.run(port=8080)
