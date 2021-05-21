import redis
r = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
# r.hset("dic_name","a1",'a')
# a = r.hget('aa','b')
# r.lpush('price',1)
# r.lpush('price',2)
# r.lpush('price',3)
# print(r.lrange('price',0,-1))
# print(r.lindex('price',0))
dic={"a1":"aa","b1":"bb"}
r.hset("dic_name",dic)
# print(r.hget("dic_name","b1"))#输出:bb
print(r.hmget("dic_name",["a1","b1","c1"]))
if __name__ == '__main__':
    pass