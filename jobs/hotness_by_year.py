#!/usr/bin/env python
# Script that shows a

import sys
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings

def map(line, params):
    import StringIO
    output = StringIO.StringIO(line)
    
    import csv
    recordReader = csv.reader(output, delimiter=',', quotechar='"')
    for line in recordReader:
        hotness = line[-10]
        year = line[-1]
        yield year, float(hotness) 

def reduce(iter, params):
    from disco.util import kvgroup
    for year, hotness in kvgroup(sorted(iter)):
        l_hotness = list(hotness)
        length = float(len(l_hotness))
        yield year, sum(l_hotness) / length

        
        
disco = Disco(DiscoSettings()['DISCO_MASTER'])
print "Starting Disco job.."
print "Go to %s to see status of the job." % disco.master
results = disco.new_job(name="song-hotness",
                        input=["tag://hackreduce:millionsongs:subset"],
                        map=map,
                        reduce=reduce,
                        save=True).wait()
print "Job done. Results:"

# Print result to user
for year, titles in result_iterator(results):
    print year, titles
