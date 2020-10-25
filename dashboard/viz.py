import plotly.graph_objects as go
import datetime


import dash
import dash_html_components as html 
import dash_core_components as dcc 
from dash.dependencies import Input, Output 

import pandas as pd 

from cassandra.cluster import Cluster 


#-------------CASSANDRA Connection-------------------------------------

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('credit')
session.execute("use credit")

#------------------ DASH APP with PLOTLY-----------------------------

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
app.config.suppress_callback_exceptions = True

app.layout = html.Div(
    
    children = [

     	html.Div([

     		html.Div([
     			html.H1(html.B(children="Credit Card Fraud Detection - Dashboard"))
     			], className="row")
     		], style={"textAlign": "center"},id="titleText"),

	        html.Div(className = 'row',
	                    children = [
	                    			html.Div(className='four columns',
	                    					children = [
	                                        dcc.Graph(id='predicted-bar', config={'displayModeBar': False}),
	                                        dcc.Interval(
	                                            id = 'predicted-bar-interval',
	                                            interval = 3000, # in milliseconds
	                                            n_intervals = 0
	                                        )
	                                    ]),
	                    			html.Div(className='four columns',
	                    					children = [
	                                        dcc.Graph(id='label-bar', config={'displayModeBar': False}),
	                                        dcc.Interval(
	                                            id = 'label-bar-interval',
	                                            interval = 3000, # in milliseconds
	                                            n_intervals = 0
	                                        )
	                                    ]),
	                    			html.Div(className='four columns',
	                    					children = [
	                                        dcc.Graph(id='ratio-pred-lab', config={'displayModeBar': False}),
	                                        dcc.Interval(
	                                            id = 'ratio-pred-lab-interval',
	                                            interval = 3000, # in milliseconds
	                                            n_intervals = 0
	                                        )
	                                    ])
	                                ])
	        	]
)


@app.callback(Output('predicted-bar', 'figure'),
				[Input('predicted-bar-interval', 'n_intervals')])
def update_pred_bar(n):

	print("Refresh Prediction Bar")

	query = 'select * from transactions'
	query_selection = session.execute(query)

	df = pd.DataFrame(list(query_selection))

	prediction_list = df['prediction'].to_list()
	fraud_count = prediction_list.count(1)
	non_fraud_count = prediction_list.count(0)

	print("fraud_count " + str(fraud_count))
	print("non_fraud_count " + str(non_fraud_count))

	fig = go.Figure()

	fig.add_trace(go.Bar(x = ['Fraud'], y = [fraud_count],
						name = 'Pred Fraud Bar',
						marker_color =  'rgb(209,68,72)',
						marker_opacity=1
					)
				)

	fig.add_trace(go.Bar(x = ['Non Fraud'], y = [non_fraud_count],
						name = 'Pred Non Fraud Bar',
						marker_color =  'rgb(73,138,93)',
						marker_opacity=1
					)
				)


	fig.update_layout(
		title = "Prediction Comparison",
		title_x = 0.5, 
		xaxis_title = "Transactions' type",
		yaxis_title = "Number of Transactions",
		showlegend = False
	)

	return fig


@app.callback(Output('label-bar', 'figure'),
				[Input('label-bar-interval', 'n_intervals')])
def update_label_bar(n):

	print("Refresh Label Bar")

	query = 'select * from transactions'
	query_selection = session.execute(query)

	df = pd.DataFrame(list(query_selection))
	
	label_list = df['label'].to_list()

	fraud_count = label_list.count(1)
	non_fraud_count = label_list.count(0)

	#print("fraud_count " + str(fraud_count))
	#print("non_fraud_count " + str(non_fraud_count))

	x_list = ['fraud', 'non fraud']

	fig = go.Figure()

	fig.add_trace(go.Bar(x = ['Fraud'], y = [fraud_count],
					name = 'Pred Fraud Bar',
					marker_color =  'rgb(209,68,72)',
					marker_opacity=1
				)
			)

	fig.add_trace(go.Bar(x = ['Non Fraud'], y = [non_fraud_count],
					name = 'Pred Non Fraud Bar',
					marker_color =  'rgb(73,138,93)',
					marker_opacity=1
				)
			)


	fig.update_layout(
		title = "Label Comparison",
		title_x = 0.5, 
		xaxis_title = "Transactions' type",
		yaxis_title = "Number of transactions",
		showlegend = False

	)

	return fig


@app.callback(Output('ratio-pred-lab', 'figure'),
				[Input('ratio-pred-lab-interval', 'n_intervals')])
def update_ratio_pie(n):

	print("Refresh Pie Chart")

	query = 'select * from transactions'
	query_selection = session.execute(query)

	df = pd.DataFrame(list(query_selection))

	prediction_list = df['prediction'].to_list()
	label_list = df['label'].to_list()

	number = len(prediction_list)

	values = []
	correct = 0
	errors = 0

	for i in range(number):
		if(prediction_list[i] == label_list[i]): 
			correct += 1
		else: errors += 1 

	values.append(correct)
	values.append(errors)

	labels = ['Correct', 'Non Correct']
	colors = ['rgb(33,66,82)','rgb(175,45,45)']

	fig = go.Figure()

	fig.add_trace(go.Pie(labels=labels, values=values, hole=.5,
						name = 'Pie Fraud',
					)
				)

	fig.update_traces(marker=dict(colors=colors))

	fig.update_layout(
		title="Prediction Correctness",
		title_x=0.25)

	return fig

 
#----------_APP Localhost run-----------------------

if __name__ == '__main__':
	app.run_server(debug = True)
