import json
import os
import math
import dateutil.parser
import datetime
import time
import logging
import boto3
from botocore.vendored import requests
import string

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Helper Functions:

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    
    return response
    
def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }
    
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


""" --- Functions that control the bot's behavior --- """

def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def isvalid_location(location):
    locations = ['new york', 'manhattan', 'brooklyn', 'bronx', 'queens', 'staten island']
    if location.lower() not in locations:
        return build_validation_result(False,
                                       'Location',
                                       'Please enter correct location')

def isvalid_cuisine(cuisine):
    cuisines = ['indian','chinese', 'mediterranean', 'mexican', 'american', 'japanese']
    #cuisines = ['nothing','chinese', 'indian', 'mediterranean', 'mexican', 'american', 'japanese'] #victor
    if cuisine.lower() not in cuisines:
        return build_validation_result(False,
                                       'Cuisine',
                                       'Please enter correct Cuisine')

def isvalid_people(num_people):
    print("num people", num_people)
    num_people = int(num_people)
    if num_people > 20 or num_people < 0:
        return build_validation_result(False,
                                  'People',
                                  'Maximum of only 20 people allowed')

def isvalid_phonenum(phone_num):
    if len(phone_num)!= 12:
        return build_validation_result(False, 'PhoneNumber', 'Phone Number must be 12 digits')
    


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# Action Functions

def greeting_intent(intent_request):
    
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText', 
                'content': 'Hi there, how can I help?'}
        }
    }

def thank_you_intent(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText', 
                'content': 'Glad I can help!!'}
        }
    }

def validate_dining_suggestion(location, cuisine, num_people, given_time, phone_num):
    
    
    if location is not None: 
        notValidLocation = isvalid_location(location)
        if notValidLocation:
            return notValidLocation

    if cuisine is not None:
        notValidCuisine = isvalid_cuisine(cuisine)
        if notValidCuisine:
            return notValidCuisine
        
                                       
    if num_people is not None:
        notValidPeople = isvalid_people(num_people)
        if notValidPeople:
            return notValidPeople
        
    
    if given_time is not None:
        print("given_time", given_time)
        # if len(given_time) != 5:
        #     # Not a valid time; use a prompt defined on the build-time model.
        #     return build_validation_result(False, 'Time', None)

        # hour, minute = given_time.split(':')
        # hour = parse_int(hour)
        # minute = parse_int(minute)
        # print("hour", hour, "minute", minute)
        # if math.isnan(hour) or math.isnan(minute):
        #     # Not a valid time; use a prompt defined on the build-time model.
        #     return build_validation_result(False, 'Time', 'Not a valid time')

        # if hour < 10 or hour > 16:
        #     # Outside of business hours
        #     return build_validation_result(False, 'Time', 'Our business hours are from ten a m. to five p m. Can you specify a time during this range?')


    if phone_num is not None:
        notValidPhonenum = isvalid_phonenum(phone_num)
        if notValidPhonenum:
            return notValidPhonenum

    #return True if all the slots are valid
    return build_validation_result(True, None, None)

def dining_suggestion_intent(intent_request):
    
    location = get_slots(intent_request)["Location"]
    cuisine = get_slots(intent_request)["Cuisine"]
    given_time = get_slots(intent_request)["DiningTime"]
    num_people = get_slots(intent_request)["NumPeople"]
    phone_num = get_slots(intent_request)["PhoneNumber"]
    
    source = intent_request['invocationSource']
    
    
    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        
        validation_result = validate_dining_suggestion(location, cuisine, num_people, given_time, phone_num)
        print ("validation_result", validation_result)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])
                               
        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

        print("output_session_attributes", output_session_attributes)
        
        print ('here')
        print ('uncomment below to get the ')
        return delegate(output_session_attributes, get_slots(intent_request))
      
    # time to Unix time conversion for the yelp API  
    # req_date = datetime.datetime.strptime(date, '%Y-%m-%d')
    # req_hour, req_minute = given_time.split(':')
    # req_hour = parse_int(req_hour)
    # req_minute = parse_int(req_minute)
    
    # dt = datetime.datetime(req_date.year, req_date.month, req_date.day, req_hour, req_minute)
    # dt_unix = time.mktime(dt.timetuple())
    # dt_unix = parse_int(dt_unix)
    
        
    # Add Yelp API endpoint to get the data
    requestData = {
                    "term":cuisine+", restaurants",
                    "location":location,
                    "categories":cuisine,
                    "limit":"3",
                    "peoplenum": num_people,
                    "Time": given_time,
                    "PhoneNumber" : phone_num
                }
                
    print (requestData)
    
    # This is for the yelp API
    #resultData = restaurantApiCall(requestData)
    
    messageId = restaurantSQSRequest(requestData)
    print (messageId)

    return close(intent_request['sessionAttributes'],
             'Fulfilled',
             {'contentType': 'PlainText',
              'content': 'I received your request and will send my recommendation soon. Until then, Sit back and relax :)'})


def restaurantSQSRequest(requestData):
    
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/540692873434/dining_reservations'
    delaySeconds = 5
    messageAttributes = {
        'Term': {
            'DataType': 'String',
            'StringValue': requestData['term']
        },
        'Location': {
            'DataType': 'String',
            'StringValue': requestData['location']
        },
        'Categories': {
            'DataType': 'String',
            'StringValue': requestData['categories']
        },
        "DiningTime": {
            'DataType': "String",
            'StringValue': requestData['Time']
        },
        # "DiningDate": {
        #     'DataType': "String",
        #     'StringValue': requestData['Date']
        # },
        'PeopleNum': {
            'DataType': 'Number',
            'StringValue': requestData['peoplenum']
        },
        'PhoneNumber': {
            'DataType' : 'String',
            'StringValue' : requestData['PhoneNumber']
        }
        # 'EmailId': {
        #     'DataType': 'String',
        #     'StringValue': requestData['EmailId']
        # }
    }
    messageBody=('Recommendation for the food')
    
    response = sqs.send_message(
        QueueUrl = queue_url,
        DelaySeconds = delaySeconds,
        MessageAttributes = messageAttributes,
        MessageBody = messageBody
        )

    print("response", response)
    
    print ('send data to queue')
    print(response['MessageId'])
    
    return response['MessageId']
    
    

def restaurantApiCall(requestData):
    
    url = "https://api.yelp.com/v3/businesses/search"
    
    querystring = requestData
    
    payload = ""
    headers = {
        'Authorization': "Bearer NJkSpKWmlW-gVx9vlMha96c5IxdgmdVYW0GJGP9gkoqr9I7Y6buwPmj2SGZF6JS6ryu01WAR_p8dMiN_LBKRkFYhXbZdRAGz4Z-V-M9MfdOPov87w-6K_e34CFfuXHYx",
        'cache-control': "no-cache",
        'Postman-Token': "d1b24c2d-4f0d-4a67-b5fa-48f40f6fa447"
        }
    
    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    message = json.loads(response.text)
    
    print("message['businesses']", message['businesses'])
    
    if len(message['businesses']) <= 0:
        return 'We can not find any restaurant under this description, please try again.'
    
    textString = "Hello! Here are my " + requestData['categories'] + " restaurant suggestions for " + requestData['peoplenum'] +" people, for " + " at " + requestData['Time'] + ". "
    count = 1
    for business in message['businesses']:
        textString = textString + " " + str(count) + "." + business['name'] + ", located at " + business['location']['address1'] + " "
        count += 1
    
    textString = textString + " Enjoy your meal!"
    print("textString", textString)
    return textString


""" --- Intents --- """


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    print(intent_name)

    # Dispatch to your bot's intent handlers
    if intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return dining_suggestion_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')
    

""" --- Main handler --- """

def lambda_handler(event, context):
    # TODO implement
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Hello from Lambda!')
    # }
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
