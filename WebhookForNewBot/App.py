from flask import send_from_directory , abort, Flask, send_file, request, jsonify, session,render_template, url_for
import os
import json
import requests
import comm_credentials as cred
import base64


app = Flask(__name__)
cf_port = os.getenv("PORT")

user="Deepan"
key="Msd183$$"
api_url="https://0509.callidusondemand.com/api/v2/"























@app.route('/GetCreditedInvoices',methods=['POST','GET'])
def GetCreditedInvoices():
    
    if (request.method == 'POST'):
        print('Inside Post')
    ##########      GET SLUG        ##############
        botdata = json.loads(request.get_data())
        skill = botdata['conversation']['skill']
        positionseq=botdata['conversation']['memory']['positionseq']
        periodseq=botdata['conversation']['memory']['periodseq']
        # salesorderseq = botdata['conversation']['memory']['salesorderseq']
        print('positionseq is ' + str(positionseq))
                
        base_url = api_url+"credits?$filter=(position eq "+positionseq +" and period eq "+periodseq +" )"
        print(base_url)
        r = requests.get(base_url, auth=(user,key))
        print('this is the output' + str(r.text))
        credits = json.loads(r.text)
        
        salestransactionseq=[]
        for x in credits['credits']:
            if(x["salesTransaction"] not in salestransactionseq):
                salestransactionseq.append(x["salesTransaction"])

        print(salestransactionseq)

        invoices=[]
        for x in salestransactionseq:
            base_url = api_url+"salesTransactions?$filter=(salesTransactionSeq eq "+x+" )"
            print(base_url)
            r = requests.get(base_url, auth=(user,key))
            # print('this is the output' + str(r.text))
            invoice_data = json.loads(r.text)
            
            for x in invoice_data['salesTransactions']:
                print(x["genericAttribute1"])
                if(x["genericAttribute1"] not in invoices):
                    invoices.append(x["genericAttribute1"])

        invoices_str='\n\n'       
        for x in invoices:
            invoices_str+=x
            invoices_str+='\n'

   
    return jsonify(
        status=200,
        replies=[{
        'type': 'text',
        'content': 'Below are the Invoices that contributed to your Commissions %s' %(invoices_str)
        }]
        ) 

@app.route('/AllInvoices',methods=['POST','GET'])
def AllInvoices():
    
    if (request.method == 'POST'):
        print('Inside Post')
    ##########      GET SLUG        ##############
        botdata = json.loads(request.get_data())
        skill = botdata['conversation']['skill']
        positionseq=botdata['conversation']['memory']['positionseq']
        salesorderseq = botdata['conversation']['memory']['salesorderseq']
        print('positionseq is ' + str(positionseq))
        
        base_url = api_url+"salesTransactions?$filter=(salesOrder eq "+salesorderseq +" )"
        print(base_url)
        r = requests.get(base_url, auth=(user,key))
        # print('this is the output' + str(r.text))
        invoice_data = json.loads(r.text)
        
        invoices=[]
        for x in invoice_data['salesTransactions']:
            if(x["genericAttribute1"] not in invoices):
                invoices.append(x["genericAttribute1"])
        invoices_str='\n'       
        for x in invoices:
            invoices_str+=x
            invoices_str+='\n'
   
    return jsonify(
        status=200,
        replies=[{
        'type': 'text',
        'content': 'All invoices part of the order are %s' %(invoices_str)
        }]
        ) 


@app.route('/MissingInvoice',methods=['POST','GET'])
def MissingInvoice():
    
    if (request.method == 'POST'):
        botdata = json.loads(request.get_data())
        # if botdata['conversation']['memory']['positionseq']:
        #     positionseq=botdata['conversation']['memory']['positionseq']
        missingInvoice=botdata['conversation']['memory']['missingInvoice']['raw']
        missingInvoice=missingInvoice.upper()
        # print('positionseq is ' + str(positionseq))
        
        base_url = api_url+"salesTransactions?$filter=(genericAttribute1 eq "+missingInvoice +")"
        print(base_url)
        r = requests.get(base_url, auth=(user,key))        
        invoice_data = json.loads(r.text)
        
        reason_code=[]
        salestransactionseq_lst=[]
        if not invoice_data['salesTransactions']:
                return jsonify(
        status=200,
        replies=[{
        'type': 'text',
        'content': 'The Invoice %s is not available in the commissions system' %(missingInvoice)
        }]
        ) 
        
        elif invoice_data['salesTransactions']:
            for x in invoice_data['salesTransactions']:
                salestransactionseq_lst.append(x["salesTransactionSeq"])
                if botdata['nlp']['intents']:
                    slug=botdata['nlp']['intents'][0]['slug']
                    if slug == 'invoiceincluded' :
                        for x in salestransactionseq_lst:
                            base_url = api_url+"credits?$filter=(salesTransaction eq "+x+" )"
                            r = requests.get(base_url, auth=(user,key))
                # print('this is the output' + str(r.text))
                            credits = json.loads(r.text)
                            if not credits['credits']:
                                for x in invoice_data['salesTransactions']:
                                    reason_code.append(x["genericAttribute2"])
                                    for x in reason_code:
                                        base_url = api_url+"products?$filter=(classifierId eq "+x+")"
                                        r = requests.get(base_url, auth=(user,key))
                                        response = json.loads(r.text)
                                        
                                        if not response['products'][0]['description']:
                                            return jsonify(
                                            status=200,
                                            replies=[{
                                            'type': 'text',
                                            'content': 'The Invoice %s is available in system, but no commissions generated. Please check with Comp Admin for more details' %(missingInvoice)
                                            }]
                                    ) 
                                        reason=response['products'][0]['description']
                                    
                                #if invoice is present and reason code is available
                                    return jsonify(
                                        status=200,
                                        replies=[{
                                        'type': 'text',
                                        'content': 'The Invoice %s did not generate commission due to the following reason : %s' %(missingInvoice, reason)
                                        }]
                                        ) 
                                
                                
            #                     return jsonify(
            # status=200,
            # replies=[{
            # 'type': 'text',
            # 'content': 'The Invoice %s has not been considered for commissions' %(missingInvoice)
            # }]
            # )               
                            elif credits['credits']:
                                return jsonify(
            status=200,
            replies=[{
            'type': 'text',
            'content': 'The Invoice %s has been considered for commissions' %(missingInvoice)
            }]
            ) 
                    
            #if invoice is present and no reason code is available
                for x in salestransactionseq_lst:
                    base_url = api_url+"credits?$filter=(salesTransaction eq "+x+" )"
                    r = requests.get(base_url, auth=(user,key))
                # print('this is the output' + str(r.text))
                    credits = json.loads(r.text)
                if credits['credits']:
                                return jsonify(
            status=200,
            replies=[{
            'type': 'text',
            'content': 'The Invoice %s has been considered for commissions' %(missingInvoice)
            }]
            ) 
                
                elif not credits['credits']:
                    for x in invoice_data['salesTransactions']:
                        reason_code.append(x["genericAttribute2"])
                        
                    for x in reason_code:
                        base_url = api_url+"products?$filter=(classifierId eq "+x+")"
                        r = requests.get(base_url, auth=(user,key))
                        response = json.loads(r.text)
                        
                        if not response['products'][0]['description']:
                            return jsonify(
                            status=200,
                            replies=[{
                            'type': 'text',
                            'content': 'The Invoice %s is available in system, but no commissions generated. Please check with Comp Admin for more details' %(missingInvoice)
                            }]
                    ) 
                        reason=response['products'][0]['description']
                    
                #if invoice is present and reason code is available
                    return jsonify(
                        status=200,
                        replies=[{
                        'type': 'text',
                        'content': 'The Invoice %s did not generate commission due to the following reason : %s' %(missingInvoice, reason)
                        }]
                        ) 
        # I could not find the reason for the missing invoice. Please contact Comp Admin


@app.route('/paymentTotal',methods=['POST','GET'])
def paymentTotal():
    
    if (request.method == 'POST'):
        print('Inside Post')
        botdata = json.loads(request.get_data())
        skill = botdata['conversation']['skill']
        # if skill == 'paymentquery':
        positionseq=botdata['conversation']['memory']['positionseq']
        periodseq=botdata['conversation']['memory']['periodseq']
        print('positionseq is ' + str(positionseq))
        
        base_url = api_url+"payments?$filter=(position eq "+positionseq+" and period eq "+periodseq+ ")"
        headers = {'Accept': '*/*', 'Accept': 'application/json'}
        r = requests.get(base_url, headers=headers, auth=(user,key))
        payments = json.loads(r.text)
        
        payment_values=[]
        total=0.0
        payment_values=payments["payments"]
        for x in payment_values:
            total+=x["value"]["value"]
        trialPipelineRunDate=payments["payments"][0]["trialPipelineRunDate"]
        trialPipelineRunDate=trialPipelineRunDate[0:10]

        return jsonify(
            status=200,
            replies=[{
 "type": "card",
 "content": {
 "title": "Total Commissions Information",
 "subtitle": 'Your total commissions as of %s is $%s' %(trialPipelineRunDate, total),
 "imageUrl": "https://i.ibb.co/6b6Tqb6/282887-Cash-Compensation-R-blue.png",
 "buttons": []
 }
 }])

@app.route('/getAchievement',methods=['POST','GET'])
def getAchievement():
    
    if (request.method == 'POST'):
        print('Inside Post')
        botdata = json.loads(request.get_data())
        positionseq=botdata['conversation']['memory']['positionseq']
        periodseq=botdata['conversation']['memory']['periodseq']
        
        base_url = api_url+"incentives?$filter=(position eq "+positionseq+" and period eq "+periodseq+ ")&select=genericAttribute1,genericNumber1,genericNumber2"
        headers = {'Accept': '*/*', 'Accept': 'application/json'}
        r = requests.get(base_url, headers=headers, auth=(user,key))
        incentives = json.loads(r.text)
        achievement_list=[]
        incentive_values=incentives["incentives"]
        for x in incentive_values:
            a_string = x["genericAttribute1"].upper()
            partitioned_string = a_string.partition(' ')
            print(round(x["genericNumber2"]["value"],2))
            achievement_list.append (partitioned_string[0] + ' ACHIEVEMENT is ' + str(round(x["genericNumber2"]["value"],2))+'%')

        achievement_str='\n'
        for x in achievement_list:
                achievement_str+=str(x)
                achievement_str+='\n'

    return jsonify(
        status=200,
        replies=[{
        'type': 'text',
        'content': 'Below are the Achievement Details %s' %(achievement_str)
        }]
        ) 

@app.route('/getRate',methods=['POST','GET'])
def getRate():
    
    if (request.method == 'POST'):
        print('Inside Post')
        botdata = json.loads(request.get_data())
        print ('bot data working')
        # skill = botdata['conversation']['skill']
        positionseq=botdata['conversation']['memory']['positionseq']
        periodseq=botdata['conversation']['memory']['periodseq']
                
        base_url = api_url+"incentives?$filter=(position eq "+positionseq+" and period eq "+periodseq+ ")&select=genericAttribute1,genericNumber1,genericNumber2"
        headers = {'Accept': '*/*', 'Accept': 'application/json'}
        r = requests.get(base_url, headers=headers, auth=(user,key))
        incentives = json.loads(r.text)
        print(incentives)
        keys = ["genericNumber1", "genericAttribute1"]  
        rate_name=[]
        incentive_values=incentives["incentives"]
        for x in incentive_values:
            a_string = x["genericAttribute1"].upper()
            partitioned_string = a_string.partition(' ')
            if(x["genericAttribute1"].upper() + ':' + str(x["genericNumber1"]["value"]) not in rate_name):
                rate_name.append('For Achievement of ' +  str(round(x["genericNumber2"]["value"],2))+'% ' + 'the ' + x["genericAttribute1"].upper() + ' is ' + str(x["genericNumber1"]["value"]).replace('.0','%'))
        print(rate_name)

        rate_str='\n'
        for x in rate_name:
                rate_str+=x
                rate_str+='\n'
                
    return jsonify(
            status=200,
            replies=[{
            'type': 'text',
            'content': 'Below are the Commission Rates %s' %(rate_str)
            }]
            ) 


@app.route('/getImage',methods=['POST','GET'])
def getImage():
    if (request.method == 'POST'):
        # image_url="https://image.shutterstock.com/image-photo/colorful-flower-on-dark-tropical-260nw-721703848.jpg"
        # image = requests.get(url = image_url)
        # # print(image.format)
        # encoded_string = base64.b64encode(image)
        print('In get Image')
        # return jsonify({ "type":"card", 
        # #         "content": {
        # # "title": "This is from webhook",
        # # "subtitle": "Wowwww",
        # # "description": "",
        # # "imageUrl":"https://image.shutterstock.com/image-photo/colorful-flower-on-dark-tropical-260nw-721703848.jpg"},
        # # "buttons": [],
        # # "sections": []
        # # }) 
        # img_file='\uploads\commission.jpg'
        # img = open(img_file, 'rb').read()
        # response = requests.post(URL, data=img, headers=headers)
        # output = [dI for dI in os.listdir('static') if os.path.isdir(os.path.join('static',dI))]
        # print(output)
        filename='static/Commissions.png'
        with open(filename, "rb") as image_file:
            b64_string = base64.b64encode(image_file.read())
            # image=(image_file.read())
    #     return jsonify(
    #         status=200,
    #         replies=[{
    #     "type": "list",
    #     "content": {
    #         "elements": [
    #             {
    #                 "title": "Title1",
    #                 "imageUrl": "https://previews.123rf.com/images/artistbandung/artistbandung2006/artistbandung200600743/153093157-bag-with-money-logo-suck-money-logo.jpg",
    #                 "subtitle": "Subtitle1",
    #                 "buttons": []
    
    #             }
    #         ]
    #     }
    # }]) 
    
        # url=url_for('static',filename='Commissions.png')
        # print(url)
        
        
    # img_file='\uploads\Commissions.png'
    # img = open(img_file, 'rb').read()
    # response.headers.set('Content-Type', 'image/png')
    # response.headers.set(
    #     'Content-Disposition', 'attachment', filename='%s.jpg' % pid)
    # return response
        return jsonify(
            status=200,
            replies=[{
 "type": "card",
 "content": {
 "title": "Commissions Information",
 "subtitle": "Details",
 "imageUrl": b64_string.decode(),
 "buttons": []
 }
 }])

        render_template('index.html') # You have to save the html files# inside of a 'templates' folder.
  
        # filename = os.getcwd()+"\uploads\commission.jpg"
        # filename = 'uploads/commission.jpg'
        # return send_file(filename, mimetype='image/jpg')

        # return {send_file(filename, mimetype='image/jpg')}
        # # return {"type":"picture", "content":send_file(encoded_string, mimetype='image/jpeg')}

if __name__ == '__main__':
    # app.run(host="localhost", port=4000, debug=True)
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)