from flask import send_from_directory , abort, Flask, send_file, request, jsonify, session
import os
import json
import requests


app = Flask(__name__)
cf_port = os.getenv("PORT")

@app.route('/Title',methods=['POST','GET'])
def bot():
   
    if (request.method == 'POST'):
        ##########      GET SLUG        ##############
        data = json.loads(request.get_data())
        with open('data.json', 'w') as outfile:
            json.dump(data, outfile)
            skill = data['conversation']['skill']
            with open('skill.json', 'w') as outfile:
                json.dump(skill, outfile)
        ##########      GET TITLE        ##############
        if final_slug == 'gettitle':
            position_value = data['conversation']['memory']['position']['raw']
            with open('pos_value.json', 'w') as outfile:
                json.dump(position_value, outfile)
            base_url = "https://0509.callidusondemand.com/api/v2/positions?$filter=name eq "+position_value+" &select=name,title&expand=title"
            r = requests.get(base_url , auth=(CREDENTIALS.api_login,CREDENTIALS.api_password))
            pos_data = json.loads(r.text)

            with open('pos_data.json', 'w') as outfile:
                json.dump(pos_data, outfile)

            for x in pos_data['positions']:
                positionName=x['name']
                print(x['name']) 
                y = (x['title'])
                TitleName=y['displayName']
                print(y['displayName'])

            return jsonify(
            status=200,
            replies=[{
            'type': 'text',
            'content': 'The title name is %s' % (TitleName)
            #   'content': 'The title name is %s ' %(base_url)
            }]
            ) 

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)


url = "https://57d85ec3trial.it-cpitrial05-rt.cfapps.us10-001.hana.ondemand.com/http/sftpFile"

user="sb-022b1ff0-ce16-41c7-9794-47f7408086d5!b68848|it-rt-57d85ec3trial!b26655"
key="a556c05c-ea2d-4684-a4a4-ff4b3ceeac08$XDMcsZKApc-JbVXFJFw2V5DaASS95WM-fBVnHGcWPPk="

xml = '<ns1:Order_MT xmlns:ns1= "http://cpi.sap.demo"> <orderNumber>1234</orderNumber> <supplierName>Deepan</supplierName> <productName>Tobelerone</productName></ns1:Order_MT>'

response = requests.post(url, data=xml,  auth=(user,key))
print("Status Code", response.status_code)
