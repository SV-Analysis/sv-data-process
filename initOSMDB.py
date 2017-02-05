from ParseOSM import OSMParser
from lxml import etree

# def parseOSM2DB(file_path):
#     handler = OSMParser(file_path)
#     nodes = handler.get_ways()
#     number = 0
#     for type, node in nodes:
#         if number > 3:
#             return
#         print(parse_leaf_2_obj(node))
#
#         number += 1

def get_children(ele):
    return [child for child in ele]


def get_attr(ele):
    attrs = {}
    attrs['tag'] = ele.tag
    for key in ele.attrib:
        attrs[key] = ele.attrib[key]

    return attrs

def parse_leaf_2_obj(ele):
    ret = {}
    if get_attr(ele): ret.update(get_attr(ele))
    if ele.text and ele.text.lstrip() != '':
        ret['content'] = ele.text
    children = get_children(ele)
    if len(children) != 0:
        ret['list'] = []
        for element in children:
            if element.tag is not etree.Comment:
                ret['list'].append(parse_leaf_2_obj(element))
    return ret

if __name__ == '__main__':
    pass
    # parseOSM2DB('osm/HongKong.osm')
    # dic = {1:1,2:2}
    # dic2 = [{'a': 'a', 'b': 'b'}, {'x': 'a', 't': 'b'}]
    # dic.update(dic2)
    # print(dic)
