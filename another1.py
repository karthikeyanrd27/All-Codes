#!/usr/bin/env python
'''
A helper class that can connect schema registry URL and will get the schema ID.
Based on the given Server name , topic name , port number , this is class will give the schema_id of the respective Topic. 
'''
import requests

def getting_schema(var_server_name, var_topic_name,var_port_no):
    var_http ='http://'
    var_vers = '/versions/1'
    var_subj = '/subjects/'
    var_topic_val = var_topic_name + '-value'
    var_url = var_http+var_server_name+':'+var_port_no+var_subj+var_topic_val+var_vers
    r= requests.get(var_url)
    r.raise_for_status()
    b = r.json()
    c=b.get('id')
    return c


