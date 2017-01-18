import json
import datetime
from pymongo import MongoClient
from Configuration import Configuration
import time

HOST = "127.0.0.1"
PORT = 27017

CONFIG_FOLDER = 'output'
DBNAME = 'sv_analysis'
RESULT_COLLECTION_NAME = 'result_c'
RESULT_FILE = 'result_file'
FOLDER = 'folder'
NAME = 'name'

def parse_data_into_mongo():
    conf = Configuration()
    cities = conf.read_configuration(CONFIG_FOLDER)
    d_name = DBNAME

    for city in cities:
        c_name = city[RESULT_COLLECTION_NAME]

        whole_path = city[FOLDER] + '/' + city[RESULT_FILE]
        if c_name == '':
            continue
        import_csv_to_mongo(whole_path, HOST, PORT, d_name, c_name)

def import_csv_to_mongo(filename, HOST, PORT, d_name, c_name):
    client = MongoClient(HOST, PORT)
    db = client[d_name]
    collection = db[c_name]
    collection.remove({})
    with open(filename, 'r') as inputfile:
        schema_line = inputfile.readline()
        index2schema = {}
        schema_arr = schema_line.split(',')
        for index in range(0, len(schema_arr)):
            index2schema[index] = schema_arr[index].strip()

        data_line = inputfile.readline()

        parsed_number = 0

        while data_line:
            items = data_line.split(',')
            item_record = {}
            location = [None, None]
            max_attr = None
            max_value = -1
            for index in range(0, len(items)):
                key = index2schema[index]
                value = items[index].strip()
                if key == 'longitude':
                    location[0] = float(value)
                elif key == 'latitude':
                    location[1] = float(value)
                elif key == 'index':
                    item_record[key] = int(value)
                else:
                    item_record[key] = float(value)
                    if max_value < item_record[key]:
                        max_value = item_record[key]
                        max_attr = index2schema[index]
                        item_record['max_attr'] = {
                            'attr': max_attr,
                            'value': max_value
                        }

            if location[0] == None or location[1] == None:
                continue
            item_record['location'] = location
            collection.insert(item_record)
            parsed_number += 1

            if parsed_number % 10000 == 0:
                print parsed_number,' lines in ', filename, ' are parsed!'
            data_line = inputfile.readline()
        print 'Import', filename, 'finished!'

def count_number_of_lines(filename):
    with open(filename, 'r') as readfile:
        line = readfile.readline()
        number = 0
        while line:
            number += 1
            line = readfile.readline()
        print number

def test_query():
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db['result_london']
    num = 0
    for record in collection.find({
        'location': {
            '$geoWithin': {
                # '$polygon': [
                #     [113.713128, 22.603702],
                #     [114.427537, 22.603702],
                #     [114.427537, 22.153432],
                #     [113.713128, 22.153432]
                # ]
                '$box': [
                    [-0.187225, 51.544570],
                    [-0.080424, 51.476434]
                ]
                # '$box':[
                #     [-0.243790, 51.571068],
                #     [0.191129, 51.142103]
                # ]
                # '$box': [
                #     [-4.833587, 53.915931],
                #     [1.433694, 50.415793]
                # ]
            }
        }
    }):
        num += 1

    print num


def create_geo_index():
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db['result_london']
    num = 3
    collection.createIndex( { 'location' : "2d" } )

if __name__ == '__main__':
    parse_data_into_mongo()
    # file_name = 'data/results_hk.csv'
    # count_number_of_lines(file_name)

    # create_geo_index()
    # start_time = time.time()
    # test_query()
    # create_geo_index()
    # end_time = time.time()
    # print end_time - start_time
    pass