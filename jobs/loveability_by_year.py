#!/usr/bin/env python
# Script that shows (year, num_songs_with_"love"_in_title)
# conclusion: 1999 is the most romantic year

import sys
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings

def map(line, params):
    import StringIO
    output = StringIO.StringIO(line)
    
    import csv
    recordReader = csv.reader(output, delimiter=',', quotechar='"')
    for line in recordReader:
        title = line[-4]
        year = line[-1]
        yield year, title 

def reduce(iter, params):
    from disco.util import kvgroup
    for year, titles in kvgroup(sorted(iter)):
        romantic_titles = [title for title in titles if "love" in title.lower()]
        yield year, len(romantic_titles)
        
        
disco = Disco(DiscoSettings()['DISCO_MASTER'])
print "Starting Disco job.."
print "Go to %s to see status of the job." % disco.master
results = disco.new_job(name="song-titles",
                        input=["tag://hackreduce:millionsongs:subset"],
                        map=map,
                        reduce=reduce,
                        save=True).wait()
print "Job done. Results:"

chart_url = "http://chart.apis.google.com/chart?chxr=0,0,15&chxt=y&chbh=a,4,10&chs=738x220&cht=bvs&chco=4D89F9&chds=0,15&chd=t:"
res_list = []
# Print result to user
for year, titles in result_iterator(results):
    res_list.append(str(titles))

chart_url += ",".join(res_list)
chart_url += "&chdl=Songs+with+%22Love%22+in+their+titles&chtt=Most+Romantic+Year+by+Song+Titles"
print chart_url
