from flask import Flask, request, jsonify
import os
import json
import requests 

app = Flask(__name__)
cf_port = os.getenv("PORT")
@app.route('/')
def hello():
    return '<h1>SAP Conversational AI</h1><body>The Commissions GetTitle webhook for use in SAP Conversational AI chatbots</body>'

@app.route('/PolicyInfo',methods=['POST','GET'])
def bot():
    #Edit the url, username and password before deploying to SAP BTP
    url = "https://0509.callidusondemand.com/"
    Username='Deepan'
    Password='Msd183$$'
    ##########      GET SLUG        ##############
    data = json.loads(request.get_data())
    skill = data['conversation']['skill']
    if skill == 'getPolicyInfo':
        final_slug='initial'
        nlp = data['nlp']
        intents = nlp['intents']
        slug = intents[0]
        #print(slug)
        final_slug=slug['slug']

    ##########      GET TITLE        ##############
    if final_slug == 'getPolicyInfo':
        position_value = data['nlp']['entities']['policyno']
        position = position_value[0]
        final_position=position['raw'].capitalize()
        print(final_position)
        base_url = url+"api/v2/positions?$filter=name eq "+final_position+" &select=name,title&expand=title"
        r = requests.get(base_url , auth=(Username,Password))
        print(r.text)
        pos_data = json.loads(r.text)
        for x in pos_data['positions']:
            #positionName=x['name']
            #print(x['name']) 
            y = (x['title'])
            TitleName=y['displayName']
            print(y['displayName'])

        return jsonify(
        status=200,
        replies=[{
        'type': 'text',
        'content': 'The title name is %s' % (TitleName)
        }]
        ) 

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)