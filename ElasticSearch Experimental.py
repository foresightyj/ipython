# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

#!pip install elasticsearch

# <codecell>

from sqlalchemy import *
from pprint import pprint as pp
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Enum

# <codecell>

#nanjing_sql_server_engine = create_engine("mssql+pyodbc://fht360:fht#^)2014@180.96.21.242,4219/fht360", echo=False)
#conn = nanjing_sql_server_engine.connect()

# <codecell>

test_server_engine = create_engine("mssql+pyodbc://sa:123456@192.168.0.110/20140730fht0", echo=False)
conn = test_server_engine.connect()

# <codecell>

cols = ["CompanyId", "CompanyName", "Contact", "Mobile", "CreateTime", "UpdateTime",  "FanCount", "AttenCount", "Longitude", "Latitude", "Tel", "Fax", "Email", "BusinessModel", "CompanyContent", "Address"]

# <codecell>

from datetime import datetime
import json
import requests
from elasticsearch import Elasticsearch, client
es = Elasticsearch()
es.info()
ec = es.indices

# <markdowncell>

# # Compare ik and mmseg

# <markdowncell>

# You cannot use a custom analyzer until it is referenced by an index. You would need to create a mapping that uses the analyzer and then use that index in the analyzer call. There is no need to index any documents to that index.
# 
# curl -XGET 'localhost:9200/SOMEINDEX/_analyze?analyzer=angram'

# <codecell>

#create a dummy index for testing analyzers.
requests.put('http://localhost:9200/test_analyzer_index')
def seg_chinese(data, seg_lib):
    json_result = requests.get('http://localhost:9200/test_analyzer_index/_analyze?analyzer=%s&pretty=true' % seg_lib,data=data.encode('utf-8')).json()
    return '  '.join(token['token'] for token in json_result['tokens'])

# <codecell>

paragraph = u"""江苏火火网络科技有限公司创立于中国江苏,主要为中小企业提供网络整合营销方案；推动并帮助中小企业认识互联网，应用互联网，推广互联网，提高中小企业市场核心竞争力。

公司业务涉及互联网广告业务、互联网基础业务服务、企业营销软件开发、软件服务外包、 信息技术外包、软件出口业务外包。软件定制项目：进销存软件，财务软件，办公软件、CRM软件、物流软件、酒店智能化控制管理系统、金融结算系统等。

江苏火火网络科技有限公司，致力于打造中国领先的企业营销社区。自主知识产权烽火台产品经过近十年积累和沉淀，充分运用团队多年企业网络营销执行与推广经验，在细致分析国内成长型企业在网络营销领域需求与瓶颈的基础上，利用互联网传播速度快、覆盖面广的特点，整合社区、移动、搜索精确导向的优势，历经四大版本，为广大成长企业带来高效完整的烽火台系列网络营销解决方案。

“创新，开放，担当，分享，忠诚，诚信”是烽火台的企业精神和核心价值观，是公司多年来奋斗历程和集体智慧的总结，也是未来烽火台发展的精神基石。
"""

# <codecell>

print seg_chinese(paragraph, seg_lib='ik')

# <codecell>

print seg_chinese(paragraph, seg_lib='mmseg')

# <markdowncell>

# # Conclusion: use mmseg!!!

# <markdowncell>

# #Index Data from SQL Server into ElasticSearch

# <codecell>

!curl -XDELETE localhost:9200/fht360/

# <codecell>

with open("./mapping.json") as inf:
    mapping = inf.read()

# <codecell>

print requests.put('http://localhost:9200/fht360', data=mapping).json()

# <markdowncell>

# ## Test if Chinese words segmentation works

# <codecell>

for row in conn.execute("select top 1000 %s from CM_Company;" % ','.join(cols)):
    doc = dict(row)
    company_id = doc['CompanyId']
    es.index(index="fht360", doc_type="company", id=company_id, body=doc)

# <codecell>

json_result = requests.get('http://localhost:9200/fht360/_analyze?field=CompanyName&pretty=true', data=u"江苏火火是成立在南京国家软件园的一家公司".encode('utf-8')).json()
print '  '.join(token['token'] for token in json_result['tokens'])

# <codecell>

#print json.dumps(ec.get_field_mapping(index="fht360", doc_type="company", field=','.join(cols)), indent=2)

# <codecell>

response = requests.get('http://localhost:9200/fht360/_mapping/company?pretty=true')
# print response.text

# <markdowncell>

# ## Check if we can retrieve one document

# <codecell>

print es.get(index="fht360", doc_type="company", id=1)['_source']['CompanyName']

# <markdowncell>

# ## Find the total count

# <codecell>

es.count(index='fht360', doc_type='company')

# <codecell>

requests.get('http://localhost:9200/fht360/_stats').json()['_all']['primaries']['docs']

# <markdowncell>

# ## Search by CompanyName

# <codecell>

res = es.search(index='fht360', doc_type='company', q="CompanyName:火火")
#print json.dumps(res)

# <codecell>

def search_by_company_name(name):
    q = """{
       "query" : {
           "match" : {
                "CompanyName": "%s"
          }
        },
        "highlight": {
        "fields":{
          "CompanyName": {}
        }
      }
    }""" % name
    return requests.get('http://localhost:9200/fht360/company/_search?_source=CompanyName,CompanyId', data=q).text

# <codecell>

print search_by_company_name("火火")

# <markdowncell>

# # Search by Multi-match

# <codecell>

def search_multi_match(query):
    q = '''
    {
      "query": {
        "multi_match": {
          "query": "%s",
          "fields": [
              "CompanyName^2",
              "BusinessModel^1.5",
              "Address",
              "CompanyContent"
          ]
        }
      }
    }
    ''' % query
    #return requests.get('http://localhost:9200/fht360/_search?explain', data=q).text
    return requests.get('http://localhost:9200/fht360/_search?_source=CompanyName,CompanyId,BusinessModel', data=q).text

# <codecell>

print search_multi_match("江苏火火")

# <codecell>

print search_multi_match("火火")

# <codecell>

print search_multi_match("女装")

# <codecell>

print search_multi_match("化工")

# <codecell>


