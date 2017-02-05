from lxml import etree
import time

class OSMParser:
    def __init__(self, path):
        self.nodes = None
        self.ways = None
        self._init_elements(path)

    def _init_elements(self, path):
        self.nodes = etree.iterparse(path, events=('end',), tag='node')
        self.ways = etree.iterparse(path, events=('end',), tag='way')
    def _count_number(self, collection):
        number = 0
        for event, ele in collection:
            number += 1
        return number

    def get_nodes(self):
        return self.nodes

    def get_ways(self):
        return self.ways
if __name__ == '__main__':
    start_time = time.time()
    handler = OSMParser('osm/NYC.osm')
    end_time = time.time()
    print(end_time - start_time)
