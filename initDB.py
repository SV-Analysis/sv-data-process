import json
import datetime
from pymongo import MongoClient
from Configuration import Configuration
import time
from ParseOSM import OSMParser
from initOSMDB import parse_leaf_2_obj

HOST = "127.0.0.1"
PORT = 27017

CONFIG_FOLDER = 'output'
DBNAME = 'sv_analysis'
RESULT_COLLECTION_NAME = 'result_c'
RESULT_FILE = 'result_file'
IMAGE_FILE = 'img_file'
FOLDER = 'folder'
NAME = 'name'
OVERALLNAME = 'overall_result'

OSM_FOLDER = 'osm_folder'
OSM_FILE = 'osm_file'
OSM_NODE_COLLECTION = 'osm_node_collection'
OSM_WAY_COLLECTION = 'osm_way_collection'

def parse_image_line(line):
    first_string = line.split(' ')[0]
    arr = first_string.split('/')
    return '/'.join(arr[7:])



def parse_result_data_into_mongo(city_id):
    conf = Configuration()
    cityObj = conf.read_configuration(CONFIG_FOLDER)
    d_name = DBNAME
    cities = cityObj['conf']
    for city in cities:
        if city_id == None or city_id == city['id']:
            c_name = city[RESULT_COLLECTION_NAME]
            whole_path = city[FOLDER] + '/' + city[RESULT_FILE]
            whole_img_path = city[FOLDER] + '/' + city[IMAGE_FILE]
            overall_result_name = city[OVERALLNAME]

            osm_path = city[OSM_FOLDER] + '/' + city[OSM_FILE]
            osm_node_c_name = city[OSM_NODE_COLLECTION]
            osm_way_c_name = city[OSM_WAY_COLLECTION]

            if c_name == '':
                continue
            # parse google street view into db
            records = import_csv_to_mongo(whole_path, HOST, PORT, d_name, c_name, whole_img_path)
            # all the records are storage with 50000 size block, need import_csv_to_mongo
            import_overall_result_to_mongo(HOST, PORT, d_name, overall_result_name, records)

            # parse osm file into db, please run the NYC and London seperately(data too large)
            import_osm_to_mongo(osm_path, HOST, PORT, d_name, osm_node_c_name, osm_way_c_name)


def import_osm_to_mongo(osm_path, HOST, PORT, d_name, osm_node_c_name, osm_way_c_name):
    print('Insert', osm_path)
    client = MongoClient(HOST, PORT)
    print('2nodes',d_name)
    db = client[d_name]
    node_collection = db[osm_node_c_name]

    node_collection.remove({})
    print('2nodes')

    # Init info
    OSMHandler = OSMParser(osm_path)
    nodes = OSMHandler.get_nodes()

    number = 0
    print('123nodes')
    for type, node in nodes:
        if number % 10000 == 0:
            print(number, 'nodes of', osm_node_c_name ,'has been parsed!')
        node_collection.insert(parse_leaf_2_obj(node))
        number += 1


    ways = OSMHandler.get_ways()
    way_collection = db[osm_way_c_name]
    way_collection.remove({})
    number = 0
    for type, way in ways:
        if number % 10000 == 0:
            print(number, 'ways of', osm_way_c_name ,'has been parsed!')
        way_collection.insert(parse_leaf_2_obj(way))
        number += 1

    client.close()
    print(osm_path, HOST, PORT, d_name, osm_node_c_name, osm_way_c_name)

def import_overall_result_to_mongo(HOST, PORT, d_name, overall_name, records):
    client = MongoClient(HOST, PORT)
    db = client[d_name]
    collection = db[overall_name]
    collection.remove({})
    segment_len = 50000
    i = 0
    split_records = split_list_block(records, segment_len)

    for record in split_records:
        collection.insert({'seg': record})
        i += 1


def import_csv_to_mongo(filename, HOST, PORT, d_name, c_name, whole_img_path):
    client = MongoClient(HOST, PORT)
    db = client[d_name]
    collection = db[c_name]
    collection.remove({})
    with open(filename, 'r') as inputfile, open(whole_img_path, 'r') as imageFile:
        img_lines = imageFile.readlines()
        schema_line = inputfile.readline()
        index2schema = {}
        schema_arr = schema_line.split(',')
        for index in range(0, len(schema_arr)):
            index2schema[index] = schema_arr[index].strip()

        data_line = inputfile.readline()

        parsed_number = 0
        records = []
        overall_records = []
        while data_line:
            items = data_line.split(',')
            item_record = {}
            location = [None, None]
            max_attr = None
            max_value = -1
            image_id = None
            for index in range(0, len(items)):
                key = index2schema[index]
                value = items[index].strip()
                if key == 'longitude':
                    location[0] = float(value)
                elif key == 'latitude':
                    location[1] = float(value)
                elif key == 'index':
                    image_id = int(value)
                    item_record[key] = image_id
                    item_record['img_path'] = parse_image_line(img_lines[image_id])
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
            records.append(item_record)
            parsed_number += 1

            if parsed_number % 10000 == 0:
                print(parsed_number,' lines in ', filename, ' are parsed!')
            data_line = inputfile.readline()

            overall_records.append([location[1], location[0], max_attr])
        print('Import', filename, 'finished!')
        return overall_records

def count_number_of_lines(filename):
    with open(filename, 'r') as readfile:
        line = readfile.readline()
        number = 0
        while line:
            number += 1
            line = readfile.readline()
        print(number)

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

    print(num)


def create_geo_index():
    client = MongoClient(HOST, PORT)
    db = client['sv_analysis']
    collection = db['result_london']
    collection.createIndex({ 'location' : "2d" })

def split_list(alist, wanted_parts=1):
    length = len(alist)

    return [ alist[int(i*length // wanted_parts): int((i+1)*length // wanted_parts)] for i in range(wanted_parts)]

def split_list_block(alist, block_size = 1):
    return split_list(alist, int(len(alist) / block_size))


if __name__ == '__main__':
    parse_result_data_into_mongo('nyc')

    # with open('data/hongkong_caffe.txt', 'r') as inputfile:
    #     lines = inputfile.readlines()
    #     line = lines[100]
    #     print(parse_image_line(line))
    # temp_arr = [1,2,3,4,5,6,7,8,10,12,11,2,3,4,1,2,2,3,5]
    # print(split_list_block(temp_arr, 5))