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
        
        
        
        #host
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
            #query
            searchData = es.search(index="restaurants", body={
                                                            "query": {
                                                                "match": {
                                                                    "categories.title": res_category
                                                                }}})
           
            print("searchData", searchData['hits']['hits'])
            businessIds = []
            for hit in searchData['hits']['hits']:
                businessIds.append(hit['_source']['Id'])
            
            # Call the dynamoDB
            resultData = getDynamoDbData(table, req_attributes, businessIds[:3])
            print (resultData)
            print ('req_attributes----', req_attributes)
            
            #send text message
            #sendTextToUser(req_attributes, resultData)
            
            #uncomment to send mail to user
            sendMailToUser(req_attributes, resultData)

            # Delete message received from queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Restaurant recommendation sent to user!')
    }

def getDynamoDbData(table, requestData, businessIds):
    
    if len(businessIds) <= 0:
        return 'Number of businessIds cannot be less than zero.'
    
    textString = "Hi there! My " + requestData['Categories']['StringValue'] + " restaurant suggestions for " + requestData['PeopleNum']['StringValue'] +" people," + " at " + requestData['DiningTime']['StringValue'] + " on" + requestData['DiningDate']['StringValue'] 
    count = 1
    
    for business in businessIds:
        responseData = table.query(KeyConditionExpression=Key('Id').eq(business))
        
        if (responseData and len(responseData['Items']) >= 1 and responseData['Items'][0]['info']):
            responseData = responseData['Items'][0]['info']
            display_address = ', '.join(responseData['display_address'])
            
            textString = textString + " " + str(count) + "." + responseData['name'] + ", located at " + display_address + " "
            count += 1
    
    textString = textString + " Have a great day!"
    return textString

# def sendTextToUser(requestData, resultData):

#     #credentials = boto3.Session().get_credentials()
#     RECIPIENT = requestData['PhoneNumber']['StringValue']

#     print("RECIPIENT", RECIPIENT)
#     print("resultData", resultData)
    
#     # Create an SNS client
#     sns = boto3.client(
#         "sns",
#         #aws_access_key_id= str(credentials.access_key),
#         #aws_secret_access_key=str(credentials.secret_key),
#         region_name="us-east-1"
#     )

#     # Send your sms message. 
#     try:
#         response = sns.publish(
#             PhoneNumber= RECIPIENT,
#             Message= resultData
#         )
#     except ClientError as e:
#         print(e.response['Error']['Message'])
#     else:
#         print("text message sent")
#         print(response['MessageId'])       

def sendMailToUser(requestData, resultData):
    
    SENDER = "vijayg.ece.4@gmail.com"
    RECIPIENT = requestData['EmailId']['StringValue']
    
    AWS_REGION = "us-east-1"
    
    SUBJECT = "Restaurant Recommendation"
    
    BODY_TEXT = ("Amazon project (Python)")
            
    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>Restaurant Recommendation</h1>
      <p>Hi there, Here are my restaurant suggestions</p>
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
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong. 
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent"),
 
  