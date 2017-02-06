from pymongo import MongoClient
import time
collection_pairs = [{'node': 'node_hk', 'way': 'way_hk'},
                    {'node': 'node_singapore', 'way': 'way_singapore'},
                    {'node': 'node_london', 'way': 'way_london'},
                    {'node': 'node_nyc', 'way': 'way_nyc'}]
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
        if 'list' not in record:
            print('except', record)
            continue
        temp_nodelist = record['list']
        new_node_list = []

        for node in temp_nodelist:
            if 'tag' in node:
                if node['tag'] == 'nd' and 'ref' in node:
                    node_id = node['ref']
                    collection_node = node_collection.find_one({'id': node_id})
                    new_node_list.append({
                        'ref': node['ref'],
                        'location': collection_node['location']
                    })
        way_collection.update(
            {'_id': record['_id']},
            {'$set': {'node_list': new_node_list}}
        )

        number += 1

# 3 finish


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
    modify_all_way_collections('node_nyc')
    # test_find('localhost', 27017, 'node_nyc', 'id', '2860865914')
    print(time.time() - start_time)