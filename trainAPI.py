from pymongo import MongoClient
import time
from math import radians, cos, sin, asin, sqrt
collection_pairs = [{'id': 'hk', 'node': 'node_hk', 'way': 'way_hk', 'street': 'street_hk', 'result': 'result_hk'},
                    {'id': 'singapore', 'node': 'node_singapore', 'way': 'way_singapore', 'street': 'street_singapore', 'result': 'result_singapore'},
                    {'id': 'london', 'node': 'node_london', 'way': 'way_london', 'street': 'street_london', 'result': 'result_london'},
                    {'id': 'nyc', 'node': 'node_nyc', 'way': 'way_nyc', 'street': 'street_nyc', 'result': 'result_nyc'}]
def test_sort_by_node_len(HOST, PORT, c_name):
    print("Modify collection", c_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    way_collection = db[c_name]
    number = 0
    for record in way_collection.find().sort('attr.len', -1).skip(5):
        if number > 10:
            break
        print('record', record['attr']['len'])
        number += 1
    client.close()

def query_street(HOST, PORT, c_name, start_index, number):
    """
    This function querys specific number in the street collections
    :param HOST: Host
    :param PORT: Port
    :param c_name: collection name
    :param start_index: the index of the first record
    :param number: the total number start from the first index
    :return:
    """

    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    way_collection = db[c_name]
    records = []
    total_number = 0
    for record in way_collection.find().sort('attr.len', -1).skip(start_index - 1):
        if total_number > number:
            break
        records.append(record)
        print('record', record['attr']['len'])
        number += 1
    client.close()
    return records

def collect_key_type(HOST, PORT, c_name):
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    way_collection = db[c_name]
    key_type = {

    }
    number = 0
    total_number = 0
    hightWay_list = []
    for record in way_collection.find():
        if number % 10000 == 0:
            print(number)
        if 'list' not in record:
            continue
        all_list = record['list']
        tag_list = [r for r in all_list if r['tag'] == 'tag']
        _num = 0
        for tagObj in tag_list:
            if tagObj['k'] == 'highway':
                if _num != 0:
                    print(_num)
                _num += 1
                value = tagObj['v']
                if value not in key_type:
                    key_type[value] = 0

                key_type[value] += 1

                total_number += 1
                break
        number += 1
    print('record', key_type)
    print(total_number)

def find_near_image_points(result_name, position, distance):

    client = MongoClient('127.0.0.1', 27017)
    db = client['sv_analysis']
    collection = db[result_name]
    arr = []
    for record in collection.find({
        'location': {
            '$near': {
                '$geometry': {'type': 'Points', 'coordinates': position},
                '$maxDistance': distance
            }
        }
    }):
        arr.append(record)
    return arr

def get_one_street(city_id, distance):
    street_name = None
    result_name = None
    for city_obj in collection_pairs:
        if city_obj['id'] == city_id:
            street_name = city_obj['street']
            result_name = city_obj['result']

    client = MongoClient('127.0.0.1', 27017)
    db = client['sv_analysis']
    collection = db[street_name]
    number = 0

    for record in collection.find():
        image_list = []
        if number > 5:
            break
        for node in record['node_list']:
            _list = find_near_image_points(result_name, node['location'], distance)
            image_list += _list
        number += 1
        origin_num = len(image_list)
        image_list = remove_duplicate(image_list, 'index')
        print(origin_num, len(image_list))
        # print(image_list.len)

def remove_duplicate(arr, select_attr):
    index_obj = {}
    arr_with_no_duplicate = []
    for image_node in arr:
        if 'index' not in image_node:
            continue
        index = image_node[select_attr]
        if index not in index_obj:
            index_obj[index] = image_node
            arr_with_no_duplicate.append(image_node)
    return arr_with_no_duplicate

def calculate_distance(nodes):
    total_distance = 0

    pass

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km
if __name__ == '__main__':
    start_time = time.time()
    # collect_key_type('localhost', 27017, 'way_nyc')
    # find_near_image_points('result_nyc', [-73.8177738587209, 40.7190726787152], 20);
    get_one_street('hk', 50)
    print('time', time.time() - start_time)