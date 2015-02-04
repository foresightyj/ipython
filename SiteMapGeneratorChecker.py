# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <headingcell level=2>

# Intelligently parse routing rules I noted a few months ago and turn then into regex matches

# <codecell>

routing_rules_path = "E:/OneDrive/Notes/fht360_study/routing_rules.cs"

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

# <headingcell level=2>

# Assert all last pages we found return 200 OK

# <codecell>

import requests
from contextlib import contextmanager
import time

@contextmanager
def timed_context(work_description):
    start = time.time()
    try:
        yield
    except Exception as e:
        raise
    else:
        delta = time.time() - start
        with open("urls_tested.log", "a") as outfile:
            print>>outfile, '%.2f' % delta, ':', work_description
    

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

# <codecell>

with open("all_multipage_urls.log", 'w') as txt:
    for match in last_page_matches:
        txt.write(match.string)
        txt.write('\n')
print '%d of urls in total' % len(last_page_matches)

# <codecell>

raw_urls_times = !cat urls_tested.log

urls_times = {}
for line in raw_urls_times:
    _time, url = line.split(":", 1)
    _time = float(_time)
    url = url.strip()
    urls_times[url] = _time

failed = []

# this is rather long, so we need to
for match in last_page_matches:
    url = match.string
    if url in urls_times:
        continue # skip this
    try:
        with timed_context(url):
            req = requests.get(url)
            assert req.status_code == 200
            assert not should_have_been_404(req.text), req.url + " is practically 404, which it shouldn't be."
    except AssertionError as e:
        print e

# <headingcell level=2>

# #Assert that next page will not be found on server

# <codecell>

def next_page_of_matched_url(match):
    url = match.string
    start, end = match.span('currpage')
    return url[0:start]+str(int(url[start:end])+1)+url[end:]
    
for match in last_page_matches:
    # find next page of matched url
    next_page = next_page_of_matched_url(match)
    req = requests.get(next_page)
    print req.status_code, next_page
    assert should_have_been_404(req.text)

# <codecell>


