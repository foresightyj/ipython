# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <headingcell level=2>

# Intelligently parse routing rules I noted a few months ago and turn then into regex matches

# <codecell>

routing_rules_path = "E:/NutStore/Notes/fht360_study/routing_rules.cs"
    
def _get_rules():
    with open(routing_rules_path) as f:
        for line in f:
            if '{' in line and '}' in line and 'currpage' in line.lower():
                yield line.strip().lower().strip(",").strip('"')
                
def _get_parse_rules():
    controller_patt = r'(product|company)'
    integer_patt = r'(?P<{kw}>\d+)'

    keyword_patterns = {kw: integer_patt.format(kw=kw) for kw in ["realcategoryid", "categoryid", "currpage", "industryid", "region", "tagid"]}

    for rule in _get_rules():
        try:
            rule = rule.format(controller=controller_patt, **keyword_patterns)
            yield rule
        except KeyError as e:
            print 'Don\'t care about this token %s yet' % e
            pass

parse_rules = list(_get_parse_rules())

for parse_rule in parse_rules:
    print parse_rule

# <headingcell level=2>

# Group all matches by some kind of hash of all other fields except for currpage

# <codecell>

!wc -l C://Users//yj//Desktop//fht360urls//urls//*currpage*.url

# <codecell>

currpage_stats = !wc -l C://Users//yj//Desktop//fht360urls//urls//*currpage*.url
currpage_total = int(currpage_stats[-1].split()[0])
print 'There are', currpage_total, 'links which contains currpage in total'

# <codecell>

import os
import re
import requests
import itertools
from collections import defaultdict

url_folder = u"C:/Users/yj/Desktop/fht360urls/urls"

urls_files =  os.listdir(url_folder)

def all_links_from_files(*files):
    for fn in files:
        full_path = os.path.join(url_folder, fn)
        with open(full_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield line

url_group_matches = defaultdict(list)

def try_parse_by_all_rules(url):
    url = url.lower()
    for rule in parse_rules:
        m = re.search(rule, url)
        if m:
            return m

success = 0
ignored = 0

for url in all_links_from_files(*urls_files):
    m = try_parse_by_all_rules(url)
    if m:
        groups = m.groupdict()
        currpage = groups.pop("currpage") # get rid of currpage and group the rest of keys and values into some kind of url_group
        url_group_identifier = ';'.join(sorted([str(k)+'='+v for k, v in groups.items()]))
        url_group_matches[url_group_identifier].append(m)
        success+=1
    else:
        assert 'companylist' not in url and 'productlist' not in url, "we should not miss any productlist or companylist links"
        assert 'newsinfo' in url or 'productinfo' in url or 'more' in url, "we can ignore newsinfo or productinfo or more pages for now"
        ignored += 1

print 'success', success, 'ignored', ignored

assert success >= currpage_total, "You must have missed some links that contains currpage"

# <codecell>

from operator import itemgetter

last_page_matches = []
total = 0
no_of_multipages = 0
for url_group, matches in url_group_matches.items():
    total+=1
    if len(matches) > 1:
        no_of_multipages += 1
        # find the url with largest page no
        last_page_match = sorted(matches, key=lambda match: int(match.groupdict()["currpage"]))[-1]
        #print len(matches), last_page_url
        last_page_matches.append(last_page_match)

print "%d of %d pages are multipages" %(no_of_multipages, total)

# <markdowncell>

# # Use SQLite3 to cache results

# <codecell>

error = """
(ProgrammingError) ('42000', '[42000] [Microsoft][ODBC SQL Server Driver][SQL Server]\xce\xde\xb7\xa8\xb4\xf2\xbf\xaa\xb5\xc7\xc2\xbc\xcb\xf9\xc7\xeb\xc7\xf3\xb5\xc4\xca\xfd\xbe\xdd\xbf\xe2 "urls_cache"\xa1\xa3\xb5\xc7\xc2\xbc\xca\xa7\xb0\xdc\xa1\xa3 (4060) (SQLDriverConnect); [42000] [Microsoft][ODBC SQL Server Driver][SQL Server]\xce\xde\xb7\xa8\xb4\xf2\xbf\xaa\xb5\xc7\xc2\xbc\xcb\xf9\xc7\xeb\xc7\xf3\xb5\xc4\xca\xfd\xbe\xdd\xbf\xe2 "urls_cache"\xa1\xa3\xb5\xc7\xc2\xbc\xca\xa7\xb0\xdc\xa1\xa3 (4060)') None None
"""
print error.decode('gb2312')

# <codecell>

from sqlalchemy import *
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, Enum

from sqlalchemy.dialects.mssql import NVARCHAR


#engine = create_engine("mssql+pyodbc://yuanjian:xixinannan@localhost/urls_cache")
engine = create_engine("mssql+pyodbc://sa:123456@192.168.0.110/urls_cache")
engine.echo = False

Base = declarative_base()

class Url(Base):
    __tablename__ = "urls"
    id = Column(Integer, primary_key = True)
    url = Column(String(100), unique=True, nullable=False)# whether we are checking this url as the last possible page or as last+1 page.
    page_type = Column(Enum('LAST', 'BEYONDLAST', name='page_type'), nullable=False)
    status_code = Column(Integer, nullable=False)
    response_time = Column(Float, nullable=False)
    last_checked = Column(DateTime, nullable=False, default=datetime.utcnow)
    html = Column(NVARCHAR, nullable=False)
        
    def __repr__(self):
        return "<Url(url='%s', page_type='%s', status_code='%d', last_checked='%s', response_time='%d')" % (self.url, self.page_type, self.status_code, self.last_checked, self.response_time)
    
Base.metadata.create_all(engine)

# <codecell>

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()

# <markdowncell>

# # Assert all last pages we found return 200 OK

# <codecell>

page_div_pattern = re.compile(r'<</a><span><em>(\d+)</em>/(\d+)</span>')

def should_have_been_404(html):
    """They made all links return 200 even if it corresponds to a resource shouldn't exist
    The only workaround now is to parse htmls to assert if a url should be non-existent
    """
    # here are the rules
    # 1. if 暂无新闻 shows up in the news pages, then it is not found
    # 2. if 没有找到满足条件的数据 shows up in company pages, then it is not found
    # 3. if <</a><span><em>412</em>/2</span> where 412 is a particular page no while 2 is the total number of pages
    if u"暂无新闻" in html:
        return True
    elif u"没有找到满足条件的数据" in html:
        return True
    else:
        found = page_div_pattern.search(html)
        if found:
            page, total = map(int, found.groups())
            if page > total:
                return True
    return False

# <headingcell level=2>

# #Assert that next page will not be found on server

# <codecell>

def next_page_of_matched_url(match):
    url = match.string
    start, end = match.span('currpage')
    return url[0:start]+str(int(url[start:end])+1)+url[end:]
    
for match in last_page_matches:
    # find next page of matched url
    url = next_page_of_matched_url(match)
    
    found = session.query(Url).filter_by(url=url).first()
    if found:
        continue
    else:
        try:
            res = requests.get(url)
            new_url = Url(url=url, status_code=res.status_code, response_time=res.elapsed.total_seconds(), html=res.text, page_type='BEYONDLAST')
            session.add(new_url)
            session.commit()
            assert res.status_code == 404 or should_have_been_404(res.text), res.url + " should have been 404"
        except AssertionError as e:
            print 'Error:', e
        finally:
            print '.',

# <codecell>

for match in last_page_matches:
    url = match.string
    try:
        url_result = session.query(Url).filter_by(url=url).first()
        if not url_result:
            res = requests.get(url)
            url_result = Url(url=url, status_code=res.status_code, response_time=res.elapsed.total_seconds(), html=res.text, page_type='LAST')
            session.add(url_result)
            session.commit()
        assert url_result.status_code == 200, url_result.url + " should have returned 200."
        assert not should_have_been_404(url_result.html), url_result.url + " is practically 404, which it shouldn't be."
    except AssertionError as e:
        print e
    finally:
        print '.',

# <markdowncell>

# # Double Check

# <codecell>

for url in session.query(Url).filter_by(page_type='LAST').all():
    try:
        assert url.status_code == 200, url.url + " should have returned 200."
        assert not should_have_been_404(url.html), url.url + " is practically 404, which it shouldn't be."
    except AssertionError as e:
        #session.delete(url) # we try it again
        #session.commit()
        print e
    finally:
        print '.',

# <codecell>

for url in session.query(Url).filter_by(page_type='BEYONDLAST').all():
    try:
        assert url.status_code == 404 or should_have_been_404(url.html), url.url + " should have been 404"
    except AssertionError as e:
        print e

# <markdowncell>

# # Restart Some Urls If Necessary

# <codecell>

restart_urls = !cat C:\Users\yj\Desktop\restart_urls.txt
restart_urls = [url.strip() for url in restart_urls if 'http' in url]

# <codecell>

for url in restart_urls:
    try:
        res = requests.get(url)
        tobemodified = session.query(Url).filter_by(url=url).one()
        tobemodified.status_code = res.status_code
        tobemodified.response_time = res.elapsed.total_seconds()
        tobemodified.html = res.text
        session.add(tobemodified)
        session.commit()
        assert tobemodified.status_code == 200, tobemodified.url + " should have returned 200."
        assert not should_have_been_404(tobemodified.html), tobemodified.url + " is practically 404, which it shouldn't be."

    except AssertionError as e:
        print 'Error:', e
    finally:
        print '.',

# <codecell>


# <codecell>


# <codecell>


# <codecell>


