import json
import boto3

def lambda_handler(event, context):
    uid="User1"   
    client = boto3.client('lex-runtime')
    response = client.post_text(
    botName='BookRestaurant',
    botAlias='$LATEST',
    userId=uid,
    inputText=event['message']
    )
    return {'statusCode': 200,'body': json.dumps(response['message'])}
    