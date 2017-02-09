from pymongo import MongoClient
import time

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
    for record in way_collection.find().sort('attr.len', -1).skip(start_index -1):
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

if __name__ == '__main__':
    start_time = time.time()
    collect_key_type('localhost', 27017, 'way_nyc')
    print('time', time.time() - start_time)