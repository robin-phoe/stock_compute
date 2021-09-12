import pub_uti_a
import pandas as pd
import json
import re



def deal_wave():
    sql = "SELECT * FROM boxin_data limit 1"
    wave_init_df = pub_uti_a.creat_df(sql)

    data = {'stock_id': [],
            'trade_date': [],
            'wave_price': [],
            }
    wave_df = pd.DataFrame(data)

    # 可使用二维列表转df优化，目前存储为三维[((),()),]
    def wave_list_2_dict(raw):
        wave_dict = {}
        wave_list_str = raw['boxin_list']
        data_list = re.sub("\(","[",wave_list_str)
        data_list = re.sub("\)", "]", data_list)
        wave_list = json.loads(data_list)
        for group_tuple in wave_list:
            for day in group_tuple:
                wave_df.loc[len(wave_df)] = [raw['stock_id'], day[0], day[1]]
        return raw

    wave_init_df.apply(wave_list_2_dict, axis=1)
    print('wave_df',wave_df)

if __name__ == '__main__':
    print(pub_uti_a.creat_df)