#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install confluent-kafka')


# In[2]:


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


# In[3]:


from confluent_kafka import Consumer
import random

props = read_ccloud_config("client.properties")
props["group.id"] = "python-group-"+str(random.randint(0,8))
props["auto.offset.reset"] = "earliest"

consumer = Consumer(props)


# In[4]:


import json
js = None
consumer.subscribe(["topic_2"])
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            print(type(msg.value().decode('utf-8')))
            js = json.loads(msg.value().decode('utf-8'))
        #print("key = {key:12} value = {value:12}".format(key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            #print(msg)
            #print(type(msg))
        else:
            print("NONE")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()


# In[ ]:





# In[ ]:




