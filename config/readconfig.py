import json

def read_config(file_path):
    with open(file_path,'r') as f:
        config_param = json.load(f)
    return config_param

if __name__ == '__main__':
    config_param = read_config('db_config.json')
    print(config_param)