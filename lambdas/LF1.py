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
    cuisines = ['japanese', 'mexican', 'mediterranean', 'american']
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

# def isvalid_phonenum(phone_num):
#     if len(phone_num)!= 12:
#         return build_validation_result(False, 'PhoneNumber', 'Phone Number must be 12 digits')
    


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

def validate_dining_suggestion(location, cuisine, num_people, date, given_time, email):
    
    
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

    if date is not None:      
        if not isvalid_date(date):
            return build_validation_result(False, 'Date', 'Please enter correct date')

        if datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'Date', 'Please enter date from today')
        
        
    
    if given_time is not None:
        print("given_time", given_time)

    # if phone_num is not None:
    #     notValidPhonenum = isvalid_phonenum(phone_num)
    #     if notValidPhonenum:
    #         return notValidPhonenum

    if email is not None:
    	email_list = ['vg1203@nyu.edu', 'vijayg.ece.4@gmail.com', 'mza240@nyu.edu']
    	if email not in email_list:
    		return build_validation_result(False, 'Email', 'Please enter valid email')



    #return True if all the slots are valid
    return build_validation_result(True, None, None)

def dining_suggestion_intent(intent_request):
    
    location = get_slots(intent_request)["Location"]
    cuisine = get_slots(intent_request)["Cuisine"]
    given_time = get_slots(intent_request)["DiningTime"]
    date = get_slots(intent_request)["Date"]
    num_people = get_slots(intent_request)["NumPeople"]
    email = get_slots(intent_request)["Email"]
    
    source = intent_request['invocationSource']
    
    
    if source == 'DialogCodeHook':
        slots = get_slots(intent_request)
        
        validation_result = validate_dining_suggestion(location, cuisine, num_people, date, given_time, email)
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
        
        return delegate(output_session_attributes, get_slots(intent_request))
      
    
        
    # Add Yelp API endpoint to get the data
    requestData = {
                    "term":cuisine+", restaurants",
                    "location":location,
                    "categories":cuisine,
                    "limit":"3",
                    "peoplenum": num_people,
                    "Date" : date,
                    "Time": given_time,
                    "Email" : email
                }
                
    print (requestData)
    
    
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
        "DiningDate": {
            'DataType': "String",
            'StringValue': requestData['Date']
        },
        'PeopleNum': {
            'DataType': 'Number',
            'StringValue': requestData['peoplenum']
        },
        # 'PhoneNumber': {
        #     'DataType' : 'String',
        #     'StringValue' : requestData['PhoneNumber']
        # }
        'EmailId': {
            'DataType': 'String',
            'StringValue': requestData['Email']
        }
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
