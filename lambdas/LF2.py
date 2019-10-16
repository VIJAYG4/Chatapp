import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/540692873434/dining_reservations'

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    
    if (response and 'Messages' in response):
        
        # restaurant original host
        # host = 'search-es-yelp-555toxbazyu56cgn5sgplxwgqy.us-west-2.es.amazonaws.com'
        
        # extra credit new host
        host = 'search-restaurantrecommendation-dh6emscrx4hi4qgzmk3atzpogm.us-east-1.es.amazonaws.com'
        
        region = 'us-east-1'
        service = 'es'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
        
        es = Elasticsearch(
            hosts = [{'host': host, 'port': 443}],
            # http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )
        
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('yelp-restaurants')
        
        for each_message in response['Messages']:
        
            message = each_message
            receipt_handle = message['ReceiptHandle']
            req_attributes = message['MessageAttributes']
            
            res_category = req_attributes['Categories']['StringValue']
            # old query
            searchData = es.search(index="restaurants", body={
                                                            "query": {
                                                                "match": {
                                                                    "categories.title": res_category
                                                                }}})
           
            #   new query
            # searchData = es.search(index="predictions", body={
            #                                     "sort" :[{'score': 'desc'}],
            #                                     "query": {
            #                                         "match": {
            #                                             "cuisine": "American"#res_category
            #                                         }}})
            # print (res_category)
            # print (searchData)
           
            #print("Got %d Hits:" % searchData['hits']['total'])
            print("searchData", searchData['hits']['hits'])
            businessIds = []
            for hit in searchData['hits']['hits']:
                businessIds.append(hit['_source']['Id'])
            
            # Call the dynemoDB
            resultData = getDynamoDbData(table, req_attributes, businessIds[:3])
            print (resultData)
            print ('req_attributes----', req_attributes)
            
            # send the email
            #sendMailToUser(req_attributes, resultData)
            
            #send text message
            sendTextToUser(req_attributes, resultData)
            

            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def getDynamoDbData(table, requestData, businessIds):
    
    if len(businessIds) <= 0:
        return 'We can not find any restaurant under this description, please try again.'
    
    textString = "Hello! Here are my " + requestData['Categories']['StringValue'] + " restaurant suggestions for " + requestData['PeopleNum']['StringValue'] +" people, for " + " at " + requestData['DiningTime']['StringValue'] + ". "
    count = 1
    
    for business in businessIds:
        responseData = table.query(KeyConditionExpression=Key('Id').eq(business))
        
        if (responseData and len(responseData['Items']) >= 1 and responseData['Items'][0]['info']):
            responseData = responseData['Items'][0]['info']
            display_address = ', '.join(responseData['display_address'])
            
            textString = textString + " " + str(count) + "." + responseData['name'] + ", located at " + display_address + " "
            count += 1
    
    textString = textString + " Enjoy your meal!"
    return textString

def sendMailToUser(requestData, resultData):
    
    SENDER = "vijayg.ece.4@gmail.com"
    RECIPIENT = requestData['EmailId']['StringValue']
    AWS_REGION = "us-west-2"
    
    SUBJECT = "Your Dining Suggestions"
    
    BODY_TEXT = ("Amazon project (Python)")
            
    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>Restaurant Suggestions</h1>
      <p>Hi User, Following are your restaurant suggestions</p>
      <p>""" + resultData + """</p>
    </body>
    </html>
                """         
    
    # The character encoding for the email.
    CHARSET = "UTF-8"
    
    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION)
    
    # return true
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    # 'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # # If you are not using a configuration set, comment or delete the
            # # following line
            # ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong. 
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def sendTextToUser(requestData, resultData):

    #credentials = boto3.Session().get_credentials()
    RECIPIENT = requestData['PhoneNumber']['StringValue']

    print("RECIPIENT", RECIPIENT)
    print("resultData", resultData)
    # Create an SNS client
    client = boto3.client(
        "sns",
        #aws_access_key_id= str(credentials.access_key),
        #aws_secret_access_key=str(credentials.secret_key),
        region_name="us-east-1"
    )

    # Send your sms message. 
    client.publish(
        PhoneNumber= RECIPIENT,
        Message= resultData
    )

  