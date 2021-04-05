#coding=utf-8
import os
import re
with open ('history_js1.txt','r',encoding='UTF-8') as f:
    data=f.read()
    text=data
    #text=data.decode('UTF-8').encode('gbk')
    #print(text)
#url_list=re.findall('"(.*? http|https .*?)"',text)
url_list=re.findall('(http.*?)"',text)
#url_list=re.findall('http',text)
print(url_list)
url_str='\n'
for url in url_list:
    url_str+=url+'\n'

with open ('history_js1.txt','a',encoding='UTF-8') as f:
    f.write(url_str)
