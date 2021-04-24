import json
def read_config(param):
    if param == 'db_config':
        with open('../config/db_config.json','r') as f:
            config_param = json.load(f)
        return config_param
if __name__ == '__main__':
    config_param = read_config('db_config')
    print(config_param)