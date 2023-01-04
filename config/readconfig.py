import json
def read_config(param):
    if param == 'db_config':
        with open('../config/db_config.json','r') as f:
            config_param = json.load(f)
        return config_param
db_config= {
  "host":"192.168.1.99",
  "user": "user1",
  "password": "Zzl08382020",
  "database": "stockdb"
}
if __name__ == '__main__':
    config_param = read_config('db_config')
    print(config_param)