# coding: utf-8
#
import uiautomator2 as u2
from time import sleep as sleep
import pymysql
import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),"strategy"))
import pub_uti


# d = u2.connect('192.168.1.5')
d=u2.connect_usb()
def init():
    d(resourceId="com.miui.home:id/icon_icon", description="新浪财经").click()
    # sleep(5)
    # 启动沉睡
    #关闭广告（可能没有）
    try:
        d(resourceId="cn.com.sina.finance:id/acCloseIv").click()
    except:
        pass
    #进入自选
    d(resourceId="cn.com.sina.finance:id/tab_item_tv", text="自选").click()
    #下拉自选列表
    d(resourceId="cn.com.sina.finance:id/optional_manage_img").click()
    #点击管理分组
    d(text="管理分组").click()
    #点击删除（多个）
    for i in range(1,10):
        try:
            d.xpath('//*[@resource-id="cn.com.sina.finance:id/Delete_Item_Img"]/android.widget.ImageView[1]').click()
        except:
            break
        #确定
        d(resourceId="cn.com.sina.finance:id/id_dialog_right_btn").click()
    groups = ['庄线','小波形','回撤','单涨停','波形']
    for group in groups:
        #新建分组
        d(text="新建分组").click()
        #输入名字
        d(resourceId="cn.com.sina.finance:id/id_simple_two_btn_content").click()
        #输入分组名字
        d.send_keys(group, clear=True)
        #确定
        d(resourceId="cn.com.sina.finance:id/id_dialog_right_btn").click()
    #分组管理完成
    d(resourceId="cn.com.sina.finance:id/optional_manage_left_btn").click()
    # sleep(1)
def fill_stock(stock_dict):
    #分类中stock_id不能重复
    # stock_dict = {'庄线':["002218",'603456'],'小波形':['605299'],'回撤':[],'单涨停':[],'波形':[]} //test
    #查询个股
    d(resourceId="cn.com.sina.finance:id/optional_right_img").click()
    count = 0
    for group in stock_dict:
        for id in stock_dict[group]:
            try:
                #点击输入框
                d(resourceId="cn.com.sina.finance:id/EditText_Search_Input").click()
                # sleep(1)
                #输入id 优化：id前需要加上SZ SH
                d.send_keys(id, clear=True)
                sleep(0.5)
                #点击自选 复时按钮为编辑
                try:
                    d.xpath('//*[@resource-id="cn.com.sina.finance:id/search_all_stock_list"]/android.widget.LinearLayout[1]/android.widget.RelativeLayout[1]/android.widget.FrameLayout[1]/android.widget.TextView[1]').click()
                except:
                    d(resourceId="cn.com.sina.finance:id/SearchStockItem_Edit").click()
                #选择分组
                d(resourceId="cn.com.sina.finance:id/tv_stock_group", text=group).click()
                sleep(0.3)
                #点击确定
                d(resourceId="cn.com.sina.finance:id/id_dialog_ok_btn").click()
                # sleep(1)
                count += 1
                print(count ,' ',id)
            except Exception as err:
                print('err:{},id:{},group:{}'.format(err,id,group))
                continue
def sel_data_from_db(date):
    if date == None:
        sql = "select DATE_FORMAT(max(trade_date),'%Y-%m-%d') as last_date from monitor "
        date = pub_uti.select_from_db(sql=sql)[0][0]
    print('date:',date)
    sql = "select stock_id,monitor_type from monitor where trade_date = '{}'".format(date)
    type_dic = {'zhuang':'庄线','remen_xiaoboxin':'小波形','remen_retra':'回撤','single_limit_retra':'单涨停','remen_boxin':'波形'}
    stock_dict = {}
    for type in type_dic:
        stock_dict[type_dic[type]] = []
    df = pub_uti.creat_df(sql= sql)
    df.apply(lambda raw:stock_dict[type_dic[raw['monitor_type']]].append(raw['stock_id']),axis = 1)
    print(stock_dict)


    return stock_dict
def main(date):
    init()
    stock_dict = sel_data_from_db(date)
    fill_stock(stock_dict)
    print('completed.')
if __name__ == '__main__':
    date = None
    main(date)
