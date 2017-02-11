from pymongo import MongoClient
import time
import pymongo
from math import radians, cos, sin, asin, sqrt
collection_pairs = [{'id': 'hk', 'node': 'node_hk', 'way': 'way_hk', 'street': 'street_hk', 'result': 'result_hk'},
                    {'id': 'singapore', 'node': 'node_singapore', 'way': 'way_singapore', 'street': 'street_singapore', 'result': 'result_singapore'},
                    {'id': 'london', 'node': 'node_london', 'way': 'way_london', 'street': 'street_london', 'result': 'result_london'},
                    {'id': 'nyc', 'node': 'node_nyc', 'way': 'way_nyc', 'street': 'street_nyc', 'result': 'result_nyc'}]
# 1 Add attribute of location to documents
def modefiy_nw_collections(c_name):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['node']:
            modify_single_node_collection('localhost', 27017, r_pair['node'])

def modify_single_node_collections(c_name):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['node']:
            modify_single_node_collection('localhost', 27017, r_pair['node'])

def modify_single_node_collection(HOST, PORT, c_name):
    print("Modify collection", c_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db[c_name]
    number = 0
    count = collection.count()
    for record in collection.find():
        if number % 10000 == 0:
            print(number, "records has been parsed!", c_name, 'total', count)
        collection.update(
            {'_id': record['_id']},
            {'$set': {'location': [float(record['lon']), float(record['lat'])]}}
        )
        number += 1

# 1 Finished

# 2 Create index
def create_index_for_all_node_collections(c_name, attr):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['node']:
            create_index_for_single_node_collection('localhost', 27017, r_pair['node'], attr)


def create_index_for_single_node_collection(HOST, PORT, c_name, attr):
    print('Start indexing', c_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db[c_name]
    collection.create_index(attr)
    client.close()
    print('Index', c_name,' finished!')
# 2 Create finish

# 3 Create new attributes node_list, with each elements has the an array of nodes, and the nodes has the detail locations

def modify_all_way_collections(c_name):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['node']:
            modify_single_way_collection('localhost', 27017, r_pair['way'], r_pair['node'])


def modify_single_way_collection(HOST, PORT, way_name, node_name):
    print("Modify collection", way_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    way_collection = db[way_name]
    node_collection = db[node_name]
    number = 0
    count = way_collection.count()
    for record in way_collection.find():
        if number % 1000 == 0:
            # Sometimes the number will be larger than count. Why?
            print(number, "records has been parsed!", way_name, 'total', count)
        number += 1

        # if 'node_list' in record:
        #     continue
        if 'list' not in record:
            print('except', record)
            continue
        temp_nodelist = record['list']
        new_node_list = []
        new_tag_list = []
        for node in temp_nodelist:
            if 'tag' in node:
                if ('node_list' not in record) and (node['tag'] == 'nd') and ('ref' in node):
                    node_id = node['ref']
                    collection_node = node_collection.find_one({'id': node_id})
                    new_node_list.append({
                        'ref': node['ref'],
                        'location': collection_node['location']
                    })
                if (node['tag'] == 'tag'): #('tag_list' not in record) and
                    new_tag_list.append(node)
        if 'node_list' not in record:
            way_collection.update(
                {'_id': record['_id']},
                {'$set': {'node_list': new_node_list}}
            )

        # if 'tag_list' not in record:
        way_collection.update(
            {'_id': record['_id']},
            {'$set': {'tag_list': new_tag_list}}
        )


# 3 finish

# 4 Count Length
def set_all_way_nodelist_length(c_name):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['way']:
            set_single_nodelist_length('localhost', 27017, r_pair['way'])

def set_single_nodelist_length(HOST, PORT, c_name):
    print("update collection", c_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db[c_name]
    number = 0
    count = collection.count()
    for record in collection.find():
        if number % 10000 == 0:
            print(number, "records has been parsed!", c_name, 'total', count)
        number += 1
        if 'attr' in record:
            if 'len' in record['attr']:
                continue
        if 'node_list' in record:
            collection.update(
                {'_id': record['_id']},
                {'$set': {'attr.len': len(record['node_list'])}}
            )
        else:
            print(record)



# 4 count finish


# 5 Create way index
def create_index_for_all_way_collections(c_name, attr):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['way']:
            create_index_for_single_node_collection('localhost', 27017, r_pair['street'], attr)


def create_index_for_single_node_collection(HOST, PORT, c_name, attr):
    print('Start indexing', c_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db[c_name]
    collection.create_index(attr)
    client.close()
    print('Index', c_name,' finished!')
# 5 Create finish

# 6 Init Street Collection

def create_street_collections_from_way(c_name):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['node']:
            create_single_street_collection_from_way('localhost', 27017, r_pair['way'], r_pair['street'])


def create_single_street_collection_from_way(HOST, PORT, way_name, street_name):
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    way_collection = db[way_name]
    street_collection = db[street_name]

    street_collection.remove({})
    street_number = 0
    count = way_collection.count()
    for way in way_collection.find():
        if(street_number % 10000 == 0):
            print(street_number,' streets in', way_name, 'has been parsed! Total number is: ', count)
        if 'tag_list' not in way:
            continue
        tags = way['list']
        for tag in tags:
            if tag['tag'] == 'tag' and tag['k'] == 'highway':
                del way['_id']
                street_collection.insert(way)
                street_number += 1
                break
    print(street_number, 'street parsed!')
# Init Street Finished

# 7 Parse_tags_of_Street
def set_all_street_tag_info(c_name):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['node']:
            set_single_street_tag('localhost', 27017, r_pair['way'])

def set_single_street_tag(HOST, PORT, c_name):
    print("update collection", c_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db[c_name]
    number = 0
    count = collection.count()
    for record in collection.find():
        if number % 10000 == 0:
            print(number, "records has been parsed!", c_name, 'total', count)
        number += 1
        # if 'attr' in record:
        #     if 'len' in record['attr']:
        #         continue

        record_tag = None
        if 'tag_list' not in record:
            print("Not tag list", record)
            continue

        for tag in record['tag_list']:
            if tag['k'] == 'highway':
                record_tag = tag['v']
                break

        collection.update(
            {'_id': record['_id']},
            {'$set': {'attr.streetType': record_tag}}
        )
# 7 Parse_Finish

# 8 Update distance
def set_all_total_distance(HOST, PORT, c_name):
    for r_pair in collection_pairs:
        if c_name == None or c_name == r_pair['node']:
            set_single_street_tag('localhost', 27017, r_pair['way'])
#

# 9 Create Index for result collections
def create_position_index_for_all_result_collection(c_id):
    for r_pair in collection_pairs:
        if c_id == None or c_id == r_pair['id']:
            create_position_index_for_single_result_colelction('localhost', 27017, r_pair['result'], 'location', pymongo.GEOSPHERE)

def create_position_index_for_single_result_colelction(HOST, PORT, c_name, attr, index_type):
    print('Start indexing', c_name)
    start_time = time.time()
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db[c_name]
    if index_type != None:
        collection.create_index([(attr, index_type)])
    else:
        collection.create_index(attr)
    client.close()
    print('Create index for ', attr, ' in ', c_name,' finish! With time', time.time() - start_time)

# 9 create finished

# 10 Create image list attributes

def create_image_list_attr_for_all_street_collection(c_id):
    for r_pair in collection_pairs:
        if c_id == None or c_id == r_pair['id']:
            create_image_list_attr_for_single_street_collection('localhost', 27017, r_pair['street'],  r_pair['result'], 20)

def create_image_list_attr_for_single_street_collection(HOST, PORT, street_name, result_name, distance):
    print("update collection", street_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    street_collection = db[street_name]
    result_collection = db[result_name]
    number = 0
    count = street_collection.count()
    for record in street_collection.find():
        if number % 1000 == 0:
            print(number, "records has been parsed!", street_name, 'total', count)
        number += 1
        image_list = []
        for node in record['node_list']:
            _list = find_near_image_points(result_collection, node['location'], distance)
            image_list += _list
        image_list = remove_duplicate(image_list, 'index')
        street_collection.update(
            {'_id': record['_id']},
            {'$set': {'image_list': image_list,
                      'attr.img_len': len(image_list)}}
        )

def find_near_image_points(result_collection, position, distance):
    arr = []
    for record in result_collection.find({
        'location': {
            '$near': {
                '$geometry': {'type': 'Points', 'coordinates': position},
                '$maxDistance': distance
            }
        }
    }):
        del record['_id']
        arr.append(record)
    return arr

def remove_duplicate(arr, select_attr):
    index_obj = {}
    arr_with_no_duplicate = []
    for image_node in arr:
        if select_attr not in image_node:
            continue
        index = image_node[select_attr]
        if index not in index_obj:
            index_obj[index] = image_node
            arr_with_no_duplicate.append(image_node)
    return arr_with_no_duplicate

def distance_self(d1, d2):
    [lat1, lon1] = d1
    [lat2, lon2] = d2
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c * 1000
    return km

# Test part
def test_find(HOST, PORT, c_name, attr, value):
    print("Modify collection", c_name)
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db[c_name]
    number = 0
    count = collection.count()
    print(collection.find_one({'id': value}))
    # for record in collection.find_one({'id': value}):
    #     print(record)
    #     number += 1

if __name__ == '__main__':
    start_time = time.time()
    # create_index_for_all_node_collections(None, 'id')
    # modify_all_way_collections('node_nyc')
    # test_find('localhost', 27017, 'node_nyc', 'id', '2860865914')

    # modify_all_way_collections('node_nyc')
    # create_index_for_all_way_collections(None, 'attr.len')


    # set_all_street_tag_info(None)


    # modify_all_way_collections(None)

    # create_street_collections_from_way(None)

    # create_image_list_attr_for_all_street_collection(None)

    create_index_for_all_way_collections(None, 'attr.img_len')
    print(time.time() - start_time)
