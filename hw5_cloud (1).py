#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
from pyspark import SparkContext, SparkConf
conf = SparkConf()
sc = SparkContext.getOrCreate(conf=conf)
file_rdd = sc.wholeTextFiles("hw5/Data/*/*")


# In[2]:


flat_test = file_rdd.flatMap(lambda x: [((word, x[0]), 1) for word in x[1].split()])


# In[3]:





# In[4]:


stopWord = {"they", "she", "he", "it", "the", "as", "is", "and"}
filtered = flat_test.filter(lambda x: x[0][0].lower() not in stopWord)
result_reduced = filtered.reduceByKey(lambda x, y: x + y)


# In[8]:


output_map = result_reduced.map(lambda x: (x[0][0], [(x[0][1], x[1])]))
# output_map = result_reduced.map(lambda x: (x[0][0], [(x[0][1], x[1])]))
# output_map = result_reduced.map(lambda x: (x[1], [(x[0][1], x[0][0])]))
output_reduced = output_map.reduceByKey(lambda x, y: x + y)
output_reduced.coalesce(1).saveAsTextFile("output")


# In[6]:





# In[7]:





# In[ ]:




