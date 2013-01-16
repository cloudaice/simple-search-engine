#-*-coding:utf-8-*-
import redis
r = redis.Redis()
keys = r.keys('*')

for key in keys:
   r.delete(key)
