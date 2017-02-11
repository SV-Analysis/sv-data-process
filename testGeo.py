from geopy.distance import great_circle
from geopy.distance import vincenty
from math import radians, cos, sin, asin, sqrt

import time
def all_distance(node_list):
    distance = 0
    for i in range(0, len(node_list)):
        if i == 0:
            first_node = node_list[i]
        second_node = node_list[i]
        distance += distance_geopy_vincenty(first_node, second_node)
        first_node = second_node;
    return distance;

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

def distance_geopy_vincenty(d1, d2):
    return vincenty(d1, d2).kilometers

def distance_geopy_great_circle(d1, d2):
    return great_circle(d1, d2).kilometers

if __name__ =='__main__':
    # Lat Lon
    newport_ri = (41.49008, -71.312796)
    cleveland_oh = (41.499498, -81.695391)

    node_list = [(114.323817, 22.400054), (114.328456, 22.460481)]
    # node_list = [[22.460475,114.323817],[22.454103, 114.328431]]
    print(all_distance(node_list))

