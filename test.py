#-*-coding:utf-8-*-
import searchengine
import redis
import time
import sys

def testcrawl(pages = ['http://kiwitobes.com/wiki/Categorical_list_of_programming_languages.html']):
   crawler = searchengine.crawler()
   crawler.crawl(pages)

def testquery(q = 'functional programming'):
   search = searchengine.searcher()
   search.query(q)

def testpagerank():
   r = redis.Redis()
   result = r.hgetall('pagerank_value')
   result = sorted(result.iteritems(),key = lambda d:d[1],reverse = True)
   for key,value in result:
      #print value,key
      if 'Main' in key:
         print value,key
   
if __name__ == '__main__':
   if sys.argv[1] not in ['crawl','query']:
      print 'use crawl or query'
      exit()
   if sys.argv[1] == 'crawl':
      if len(sys.argv) > 2:
         testcrawl(pages = sys.argv[2].split(':'))
      else:
         testcrawl()
   else:
      if len(sys.argv) > 2:
         testquery(q = ' '.join(sys.argv[2].split(':')))
      else:
         testquery()
