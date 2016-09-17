from __future__ import print_function

import boto3
from decimal import *


# Helper for quering DynamoDB table
def dynamo_request(table_obj, key_name, key_value):
    resp = table_obj.get_item(Key={key_name: key_value})
    if resp.has_key('Item') and resp['Item']:
        return resp['Item']
    else:
        return None

# Constance for calculating of grid
LAT_SHIFT = Decimal("0.001")
LON_SHIFT = Decimal("0.0015")

NUM_LAT = 180 / LAT_SHIFT
NUM_LON = 360 / LON_SHIFT


# Function for calculating lower left corner of sqare as a Decimal
def calculate_sqare(lat, lon):
    grid_lat = ((Decimal(str(lat)) + 90) // LAT_SHIFT) * LAT_SHIFT - 90
    grid_lon = ((Decimal(str(lon)) + 180) // LON_SHIFT) * LON_SHIFT - 180
    return (grid_lat, grid_lon)


# Function for calculating id of the square
def calculate_id(grid_lat, grid_lon):
    return (grid_lat + 90) / LAT_SHIFT * NUM_LON + (grid_lon + 180) / LON_SHIFT


# Function for getting lat lon from square id
def calculate_grid(id):
    lat = (id // NUM_LON) * LAT_SHIFT - 90
    lon = (id % NUM_LON) * LON_SHIFT - 180
    return (lat, lon)


def lambda_handler(event, context):
    '''Provide an event that contains the following keys:

      - token: user token
      - id: user id
      - lat: lat
      - lon: lon
      - timestamp: event timestamp 
    '''

    dyn_users = boto3.resource('dynamodb').Table('mapexplorer-users')
    dyn_history = boto3.resource('dynamodb').Table('mapexplorer-history')

    user = dynamo_request(dyn_users, 'id', event['id'])
    if user:
        if user['token'] == event['token']:
            try:
                resp = dyn_history.put_item(Item={'id': event['id'],
                                                  'lat': Decimal(str(event['lat'])),
                                                  'lon': Decimal(str(event['lon'])),
                                                  'timestamp': event['timestamp']})
                if resp:
                    if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                        raise Exception("Error: Error adding new item to history")
            except:
                raise Exception("Error: Error adding new item to history")
    else:
        raise Exception("Not Found: User not found")
