from __future__ import print_function

import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import *


# Helper for quering DynamoDB table
def dynamo_request(table_obj, request_dict):
    resp = table_obj.get_item(Key=request_dict)
    if resp.has_key('Item') and resp['Item']:
        return resp['Item']
    else:
        return None


# Helper for put new item to DynamoDB table
def dynamo_put(table_obj, item_dict):
    try:
        resp = table_obj.put_item(Item=item_dict)
        if resp:
            if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                return None
    except:
        return None
    return True


# Helper for updating item in DynamoDB table
def dynamo_update(table_obj, request_dict, value_dict):
    resp = table_obj.get_item(Key=request_dict)
    if resp.has_key('Item') and resp['Item']:
        item = resp['Item']
        for key in value_dict.keys():
            item[key] = value_dict[key]
        try:
            resp = table_obj.put_item(Item=item)
            if resp:
                if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                    return None
        except:
            return None

    else:
        return None

    return True

# Constance for gamification
NEW_SQUARE = 10
NEW_POI = 20

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


# Dummy function, in future filtration of abuse usage
def filter(user, square_id):
    return True


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
    dyn_grid = boto3.resource('dynamodb').Table('mapexplorer-grid')
    dyn_poi = boto3.resource('dynamodb').Table('mapexplorer-poi')

    user = dynamo_request(dyn_users, {'id': event['id']})
    if user:
        if user['token'] == event['token']:
            update_resp = dynamo_put(dyn_history, {'id': event['id'],
                                                   'lat': Decimal(str(event['lat'])),
                                                   'lon': Decimal(str(event['lon'])),
                                                   'timestamp': event['timestamp']})
            if not update_resp:
                raise Exception("Bad Request: Error adding new item to history")

            grid_lat, grid_lon = calculate_sqare(event['lat'], event['lon'])
            square_id = calculate_id(grid_lat, grid_lon)

            if filter(user, square_id):
                if square_id != user['lastLocation']:
                    # request and update grid table
                    grid_resp = dynamo_request(dyn_grid, {'squareId': square_id, 'userId': user['id']})
                    if grid_resp:
                        # user was here, update
                        update_resp = dynamo_update(dyn_grid, {'squareId': square_id,
                                                               'userId': user['id']}, {'heat': grid_resp['heat'] + 1})
                        if not update_resp:
                            raise Exception("Bad Request: Error updating square")

                        return {'xp_square': 0, 'xp_poi': 0}
                    else:
                        #new location add to grid, xp +10
                        put_resp = dynamo_put(dyn_grid, {'squareId': square_id, 'userId': user['id'], 'heat': 1})
                        if not put_resp:
                            raise Exception("Bad Request: Error adding new square")

                        resp = dyn_poi.query(KeyConditionExpression=Key('squareId').eq(square_id))
                        num_poi = 0
                        if resp.has_key('Items') and resp['Items']:
                            num_poi = len(resp['Items'])

                        # Update XP in DynamoDB
                        new_xp = user['xp'] + NEW_SQUARE + NEW_POI * num_poi
                        update_xp = dynamo_update(dyn_users, {'id': user['id']}, {'xp': new_xp})

                        return {'xp_square': NEW_SQUARE, 'xp_poi': NEW_POI * num_poi, 'square_id': square_id}
                else:
                    return {'xp_square': 0, 'xp_poi': 0}
        else:
            raise Exception("Unauthorized: Wrong token")
    else:
        raise Exception("Not Found: User not found")
