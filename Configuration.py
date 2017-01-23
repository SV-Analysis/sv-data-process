import json

CONFIG_PATH = 'conf/conf.csv'
OUTPUT_PATH = 'output'
class Configuration():
    def __init__(self):
        pass

    def generate_conf_json(self, conf_path, out_path):
        """
        Generate configuration json file from csv
        :param path:
        :return:
        """
        confs = []
        with open(conf_path) as f:
            schema = f.readline()
            index2schema = {}
            schema_arr = schema.split(',')
            length = len(schema_arr)
            for index in range(0, length):
                index2schema[index] = schema_arr[index]
            conf_line = f.readline()

            while conf_line:
                conf_arr = conf_line.split(',')
                record = {}
                for index in range(0, len(conf_arr)):
                    record[index2schema[index].strip()] = conf_arr[index].strip()
                confs.append(record)
                conf_line = f.readline()

        conf_obj = {'overall_c': 'overall_result_c', 'conf': confs}

        with open(out_path + '/configuration.json', 'w') as outfile:
            json.dump(conf_obj, outfile)

    def read_configuration(self, path):
        """
        Read configurations from json file
        :param path:
        :return:
        """
        with open(path + '/configuration.json', 'r') as readfile:
            conf = json.load(readfile)
            return conf


if __name__ == "__main__":
    conf = Configuration()
    conf.generate_conf_json(CONFIG_PATH, OUTPUT_PATH)
    print(conf.read_configuration(OUTPUT_PATH))
