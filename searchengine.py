#-*-coding:utf-8-*-
#filename: searchengine.py
import urllib
import urllib2
import redis
import re
from BeautifulSoup import *
from BeautifulSoup import BeautifulSoup
from urlparse import urljoin
from gevent.pool import Pool
from gevent import monkey;monkey.patch_socket()
import threading
import networkx as nx

ignorewords = set(['the','of','to','and','a','in','is','it'])
class crawler(object):
   def __init__(self):
      self.r = redis.Redis()
      self.g = nx.DiGraph()

   def __del__(self):
      pass

   def add_to_index(self,url,soup):
      if self.is_indexed(url):
         return
      print 'Indexing %s' % url
      text = self.get_text_only(soup)
      words = self.separatewords(text)
      self.r.sadd('urllist',url)
      for i,word in enumerate(words):
         if word in ignorewords:continue
         if word == '': continue
         try:
            self.r.sadd('wordlist',word)
            self.r.sadd(word,url)
            self.r.sadd(word+";"+url,i)
         except:
            print "word:",word
            print 'url:',url

   def separatewords(self,text):
      splitter = re.compile('\\W*')
      return [s.lower() for s in splitter.split(text) if s !='']

   #从一个html页面中提取文字
   def get_text_only(self,soup):
      v = soup.string
      if v == None:
         c = soup.contents
         resulttext = ''
         for t in c:
            subtext = self.get_text_only(t)
            resulttext += subtext + '\n'
         return resulttext
      else:
         return v.strip()
      return None

   def is_indexed(self,url):
      if self.r.sismember('urllist',url):
         return True
      else:
         return False

   def add_linkref(self,urlFrom,urlTo,linkText):
      self.g.add_edge(urlFrom,urlTo)
      
   def download(self,url,pagehtmls):
      try:
         c = urllib2.urlopen(url)
         pagehtmls.append((url,c.read()))
      except:
         print 'Could not open %s' % url
         pagehtmls.append((url,None))

   def calculaterpagerank(self,iterations):
      pagerank = nx.pagerank(self.g,max_iter = iterations)
      for key,value in pagerank.items():
         self.r.hset('pagerank_value',key,value)

   def crawl(self,pages,depth=2):
      self.g = nx.DiGraph()
      for i in range(depth):
         newpages = set()
         pagehtmls = []
         pool = Pool(50)
         for page in pages:
            pool.apply_async(self.download,args=(page,pagehtmls))
         pool.join()
         for page,html in pagehtmls:
            if not html:
               continue
            soup = BeautifulSoup(html)
            self.add_to_index(page,soup)
            links = soup('a')
            for link in links:
               if 'href' in dict(link.attrs):
                  url = urljoin(page,link['href'])
                  if url.find("'") != -1:
                     continue
                  url = url.split('#')[0]
                  if url[0:4]=='http' and not self.is_indexed(url):
                     newpages.add(url)
                  linkText = self.get_text_only(link)
                  self.add_linkref(page,url,linkText)
            pages = newpages
      self.calculaterpagerank(20)
      

class searcher(object):
   def __init__(self):
      self.r = redis.Redis()
      self.words = []
      self.urllist = []

   def __del__(self):
      pass

   def getmatchrows(self,q):
      words = q.split()
      all_sets = [set(self.r.smembers(word)) for word in words]
      result_set = reduce(lambda x,y:x.intersection(y),all_sets)
      return words,result_set

   def get_scored_list(self):
      totalscores = dict([(url,0) for url in self.urllist])
      weights = [(1.0,self.frequency_score()),
                 (1.5,self.location_score()),
                 (2.0,self.distance_score()),
                 (2.5,self.pagerank_score())
                ]
      for (weight,scores) in weights:
         for url in totalscores:
            totalscores[url] += weight * scores[url]
      return totalscores
   
   def normalizescores(self,scores,smallIsBetter = 0):
      vsmall = 0.00001
      if smallIsBetter:
         minscore = min(scores.values())
         return dict([(u,float(minscore)/max(vsmall,l)) 
                         for (u,l) in scores.items()])
      else:
         maxscore = max(scores.values())
         if maxscore == 0:
            maxscore = vsmall
         return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])

    
   def frequency_score(self):
      counts = {}
      for url in self.urllist:
         counts[url] = 0
         for word in self.words:
            counts[url] += len(self.r.smembers(word+';'+url))
      return self.normalizescores(counts)

   def location_score(self):
      locations = {}
      for url in self.urllist:
         locations[url] = 1000000
         loc = 0
         for word in self.words:
            loc += sum([int(local) for local in self.r.smembers(word+';'+url)])
         if loc < locations[url]:
            locations[url] = loc
      return self.normalizescores(locations,smallIsBetter = 1)
            
   def two_word_distance(self,list1,list2):
      list1 = [int(value) for value in list1]
      list2 = [int(value) for value in list2]
      list1.sort()
      list2.sort()
      min = 1000000
      for i in list1:
         for j in list2:
            if abs(i-j) < min:
               min = abs(i-j)
      return min

   def distance_score(self):
      if len(self.words) == 1:
         return dict([(url,1.0) for url in self.urllist])
      else:
         mindistance = dict([(url,1000000) for url in self.urllist])
         for url in self.urllist:
            wordslist = [self.r.smembers(word+';'+url) for word in self.words]
            dist = sum([self.two_word_distance(wordslist[i],wordslist[i-1])
                       for i in range(2,len(wordslist)) ])
            if dist < mindistance[url]:mindistance[url] = dist
         return self.normalizescores(mindistance,smallIsBetter = 1)

   def pagerank_score(self):
       pageranks = [ float(self.r.hget('pagerank_value',url)) for url in self.urllist]
       return self.normalizescores(dict(zip(self.urllist,pageranks)))
            
   def view_top(self,result,top):
      i = 0 
      for key,value in result:
         print value,key
         i += 1
         if i == top:
            break

   def query(self,q):
      words,urllist =  self.getmatchrows(q)
      self.urllist = list(urllist)
      self.words = words
      result = self.get_scored_list()
      result = sorted(result.iteritems(),key = lambda d:d[1],reverse = True)
      self.view_top(result,100)

