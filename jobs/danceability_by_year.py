import sys
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings

def map(line, params):
    import StringIO
    output = StringIO.StringIO(line)
    
    import csv
    recordReader = csv.reader(output, delimiter=',', quotechar='"')
    for line in recordReader:
        danceability = line[12]
        year = line[-1]
        yield year, float(danceability)

def reduce(iter, params):
    from disco.util import kvgroup
    for year, dance_values in kvgroup(sorted(iter)):
        l_dance_values = list(dance_values)
        length = float(len(l_dance_values))
        #yield year, sum([float(x) for x in l_dance_values]) / length
        #yield sum(l_dance_values), length
        yield year, str(l_dance_values)
        #yield year, length
        
        
disco = Disco(DiscoSettings()['DISCO_MASTER'])
print "Starting Disco job.."
print "Go to %s to see status of the job." % disco.master
results = disco.new_job(name="danceability",
                        input=["tag://hackreduce:millionsongs:subset"],
                        map=map,
                        reduce=reduce,
                        save=True).wait()
print "Job done. Results:"

# Print result to user
for year, avg in result_iterator(results):
    print year, avg
