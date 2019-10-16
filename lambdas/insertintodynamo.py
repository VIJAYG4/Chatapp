import json
import boto3
import datetime
from botocore.vendored import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import csv
from io import BytesIO

def lambda_handler(event, context):
    # TODO implement
    resultData = []
    
    #if event['data_origin'] == 'yelp':
    totalRestaurantCount = 15
    for cuisine in ['INDIAN', 'CHINESE', 'MEDITERANEAN', 'MEXICAN', 'JAPANESE']:#, 'indian', 'mexican', 'chinese', 'thai', 'french']:japanese
        for i in range(totalRestaurantCount):
            requestData = {
                            "term": cuisine + " restaurants",
                            "location": "manhattan",
                            "limit": 50,
                            "offset": 50*i
                        }
            result = yelpApiCall(requestData)
            resultData = resultData + result

    print("Fetched data from Yelp API endpoint")

    # Add data to the dynamodDB
    #dynamoInsert(resultData)
    #print("Inserted Data into Dynamodb")
    
    # Add index data to the ElasticSearch
    elasticIndex(resultData)
    print("added index data to ElasticSearch")
    # else:
        
    #     # get data from s3
    #     resultData = getDataFromS3()
        
    #     # Add index data to the ElasticSearch
    #     elasticIndexForPrediction(resultData)
    
    return {
        'statusCode': 200,
        'body': json.dumps('success'),
        'total': 1#len(resultData)
    }

def yelpApiCall(requestData):
    
    url = "https://api.yelp.com/v3/businesses/search"
    
    querystring = requestData
    
    payload = ""
    headers = {
        'Authorization': "Bearer 7NjA90modOdaeEn6bLkfmOoy3YIibnWpCUki41GqXzW5z0nlxHPwRpjXeAJ1ycwHd0qeiipWT-9vtxH-WcSdM5zj9TcHhJrEzW6n_4tuQCS5YHUOnmvg_Ax4KYmKXHYx",
        'cache-control': "no-cache",
        'Postman-Token': "d1b24c2d-4f0d-4a67-b5fa-48f40f6fa447"
        }
    
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    message = json.loads(response.text)
    
    if len(message['businesses']) <= 0:
        return []
    
    return message['businesses']

def dynamoInsert(restaurants):
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('yelp-restaurants')
    
    
    for each_restaurants in restaurants:
        
        dataObject = {
            'Id': each_restaurants['id'],
            'alias': each_restaurants['alias'],
            'name': each_restaurants['name'],
            'is_closed': each_restaurants['is_closed'],
            'categories': each_restaurants['categories'],
            'rating': int(each_restaurants['rating']),
            'review_count': each_restaurants['review_count'],
            # 'transactions': each_restaurants['transactions'],
            # 'zip_code': each_restaurants['location']['zip_code'],
            'display_address': each_restaurants['location']['display_address']
        }
        
        if (each_restaurants['image_url']):
            dataObject['image_url'] = each_restaurants['image_url']
        
        if (each_restaurants['coordinates'] and each_restaurants['coordinates']['latitude'] and each_restaurants['coordinates']['longitude']):
            dataObject['latitude'] = str(each_restaurants['coordinates']['latitude'])
            dataObject['longitude'] = str(each_restaurants['coordinates']['longitude'])
            
        if (each_restaurants['phone']):
            dataObject['phone'] = each_restaurants['phone']
        
        if (each_restaurants['location']['zip_code']):
            dataObject['zip_code'] = each_restaurants['location']['zip_code']
        
        
        print ('dataObject', dataObject)
        table.put_item(
               Item={
                   'insertedAtTimestamp': str(datetime.datetime.now()),
                   'info': dataObject,
                   'Id': dataObject['Id']
               }
            )
    

def elasticIndex(restaurants):
    host = 'search-restaurantrecommendation-dh6emscrx4hi4qgzmk3atzpogm.us-east-1.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
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
    
    for each_restaurants in restaurants:
        
        dataObject = {
            'Id': each_restaurants['id'],
            'alias': each_restaurants['alias'],
            'name': each_restaurants['name'],
            'categories': each_restaurants['categories']
        }
        
        # alreadyExists = es.indices.exists(index="restaurants")
                            
        print ('dataObject', dataObject)
        
        # if alreadyExists:
        es.index(index="restaurants", doc_type="Restaurant", id=each_restaurants['id'], body=dataObject, refresh=True)
        # else:
        #     es.create(index="restaurants", doc_type="Restaurant", body=dataObject)

# def getDataFromS3():
    
#     data_key = 'FILE_3.csv'
#     bucket = 'sagemaker-yelp-data' 
#     data_location = 'https://s3-us-west-2.amazonaws.com/sagemaker-yelp-data/FILE_3.csv'

#     s3 = boto3.resource(u's3')

#     # get a handle on the bucket that holds your file
#     bucket = s3.Bucket(u'sagemaker-yelp-data')
    
#     # get a handle on the object you want (i.e. your file)
#     obj = bucket.Object(key=u'FILE_3.csv')
    
#     # get the object
#     response = obj.get()
    
#     lines = response['Body'].read().splitlines(True)
#     final_list = []
#     for line in lines:
#         final_list.append(line.decode('UTF-8'))
        
#     reader = csv.reader(final_list)
    
#     result = []
#     i = 0
#     for row in reader:
#         if i != 0:
#             if float(row[-1]) == 1.0:
#                 dataObject = {
#                     'id': row[1],
#                     'cuisine': row[2],
#                     'rating': int(row[3]),
#                     'review_count': int(row[4]),
#                     'score': float(row[5])
#                 }
#                 result.append(dataObject)
#         i = i+1 
    
    
#     return result

# def elasticIndexForPrediction(restaurants):
#     host = 'search-predictions-lmr33sl6iq3l4on4tmmsmzbf4a.us-west-2.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com
#     region = 'us-west-2'
    
#     service = 'es'
#     credentials = boto3.Session().get_credentials()
#     awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
    
#     es = Elasticsearch(
#         hosts = [{'host': host, 'port': 443}],
#         # http_auth = awsauth,
#         use_ssl = True,
#         verify_certs = True,
#         connection_class = RequestsHttpConnection
#     )
    
#     for each_restaurants in restaurants:
        
#         dataObject = {
#             'id': each_restaurants['id'],
#             'cuisine': each_restaurants['cuisine'],
#             'rating': each_restaurants['rating'],
#             'review_count': each_restaurants['review_count'],
#             'score': each_restaurants['score']
#         }
        
#         # alreadyExists = es.indices.exists(index="restaurants")
                            
#         print ('dataObject', dataObject)
        
#         # if alreadyExists:
#         es.index(index="predictions", doc_type="Prediction", id=each_restaurants['id'], body=dataObject, refresh=True)
#         # else:
#         #     es.create(index="restaurants", doc_type="Restaurant", body=dataObject)
#     