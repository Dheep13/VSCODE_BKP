# Imports
import plotly.graph_objs as go
import pandas as pd
import numpy as np


print ('Hello!!')
# data
df = pd.DataFrame({'date': {0: '2019-11-17',
                          1: '2019-10-27',
                          2: '2019-11-03',
                          3: '2019-11-10',
                          4: '2019-11-17',
                          5: '2019-10-27',
                          6: '2019-11-03',
                          7: '2019-11-17',
                          8: '2019-10-27',
                          9: '2019-11-03',
                          10: '2019-11-10',
                          11: '2019-11-17'},
                         'Reason': {0: 'AI',
                          1: 'RANDOM',
                          2: 'RANDOM',
                          3: 'RANDOM',
                          4: 'RANDOM',
                          5: 'AI',
                          6: 'AI',
                          7: 'AI',
                          8: 'RANDOM',
                          9: 'RANDOM',
                          10: 'RANDOM',
                          11: 'RANDOM'},
                         'name': {0: 'CHRISTOPHE',
                          1: 'CHRISTOPHE',
                          2: 'CHRISTOPHE',
                          3: 'CHRISTOPHE',
                          4: 'CHRISTOPHE',
                          5: 'FERRANTE',
                          6: 'FERRANTE',
                          7: 'FERRANTE',
                          8: 'FERRANTE',
                          9: 'FERRANTE',
                          10: 'FERRANTE',
                          11: 'FERRANTE'},
                         'Task': {0: 3,
                          1: 19,
                          2: 19,
                          3: 49,
                          4: 26,
                          5: 29,
                          6: 40,
                          7: 26,
                          8: 1,
                          9: 4,
                          10: 2,
                          11: 2}})

# split df by names
names = df['name'].unique().tolist()
dates = df['date'].unique().tolist()

dfs = {}

# dataframe collection grouped by names
for name in names:
    #print(name)
    dfs[name]=pd.pivot_table(df[df['name']==name],
                             values='Task',
                             index=['date'],
                             columns=['Reason'],
                             aggfunc=np.sum)

# plotly start 
fig = go.Figure()

# get column names from first dataframe in the dict
colNames = list(dfs[list(dfs.keys())[0]].columns)
#xValues=

# one trace for each column per dataframe: AI and RANDOM
for col in colNames:
    fig.add_trace(go.Bar(x=dates,
                             visible=True,
                             #name=col
                  )
             )

# menu setup    
updatemenu= []

# buttons for menu 1, names
buttons=[]

# create traces for each Reason: AI or RANDOM
for df in dfs.keys():
    buttons.append(dict(method='update',
                        label=df,
                        visible=True,
                        args=[#{'visible':True},
                              #{'x':[dfs[df]['AI'].index, dfs[df]['RANDOM'].index]},
                              {'y':[dfs[df]['AI'].values, dfs[df]['RANDOM'].values]}])
                  )

# buttons for menu 2, reasons
b2_labels = colNames

# matrix too feed all visible arguments for all traces
# so that they can be shown or hidden by choice
b2_show = [list(b) for b in [e==1 for e in np.eye(len(b2_labels))]]
buttons2=[]
buttons2.append({'method': 'update',
                 'label': 'All',
                 'args': [{'visible': [True]*4}]})

# create buttons to show or hide
for i in range(0, len(b2_labels)):
    buttons2.append(dict(method='update',
                        label=b2_labels[i],
                        args=[{'visible':b2_show[i]}]
                        )
                   )

# add option for button two to hide all
buttons2.append(dict(method='update',
                        label='None',
                        args=[{'visible':[False]*4}]
                        )
                   )

# some adjustments to the updatemenus
updatemenu=[]
your_menu=dict()
updatemenu.append(your_menu)
your_menu2=dict()
updatemenu.append(your_menu2)
updatemenu[1]
updatemenu[0]['buttons']=buttons
updatemenu[0]['direction']='down'
updatemenu[0]['showactive']=True
updatemenu[1]['buttons']=buttons2
updatemenu[1]['y']=0.6

fig.update_layout(showlegend=False, updatemenus=updatemenu)
fig.show()