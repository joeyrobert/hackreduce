#!/usr/bin/env python
# conclusion: only about 100 unique words make up song titles of 10,000 songs
# top 10 most common words:
'''
the 1255
you 479
of 447
a 429
in 406
i 370
me 341
love 309
to 304
my 292
it 230
on 210
de 202
and 189

...

tonight 20
than 19
us 18
ya 17
walk 16
white 15
wind 14
young 13
what's 12
wrong 11
you'll 10
words 9
yellow 8
yourself 7

...
'''

import numpy as np
from disco.core import Disco, result_iterator
from disco.settings import DiscoSettings

def map(line, params):
    import StringIO
    output = StringIO.StringIO(line)
    lookup = {
        'get_danceability': 12,
        'get_song_id': 25,
        'get_release': 22,
        'get_artist_hotttnesss': 3,
        'get_title': 30,
        'get_artist_longitude': 7,
        'get_end_of_fade_in': 14,
        'get_time_signature': 28,
        'get_artist_id': 4,
        'get_artist_7digitalid': 1,
        'get_track_7digitalid': 31,
        'get_mode_confidence': 20,
        'get_artist_familiarity': 2,
        'get_num_songs': 21,
        'get_time_signature_confidence': 29,
        'get_artist_name': 9,
        'get_key': 16,
        'get_artist_playmeid': 10,
        'get_analysis_sample_rate': 0,
        'get_year': 33,
        'get_key_confidence': 17,
        'get_artist_location': 6,
        'get_audio_md5': 11,
        'get_artist_mbid': 8,
        'get_mode': 19,
        'get_loudness': 18,
        'get_energy': 15,
        'get_duration': 13,
        'get_release_7digitalid': 23,
        'get_track_id': 32,
        'get_tempo': 27,
        'get_start_of_fade_out': 26,
        'get_song_hotttnesss': 24,
        'get_artist_latitude': 5
    }
    
    import csv
    recordReader = csv.reader(output, delimiter=',', quotechar='"')
    for line in recordReader:
        title = line[lookup['get_title']]
        for word in title.split(" "):
            if '(' in word or ')' in word:
                continue
            yield word.lower(), 1

def reduce(iter, params):
    from disco.util import kvgroup
    for x, y in kvgroup(sorted(iter)):
        yield x, sum(y)

disco = Disco(DiscoSettings()['DISCO_MASTER'])
print "Starting Disco job.."
print "Go to %s to see status of the job." % disco.master
results = disco.new_job(name="song-titles",
                        input=["tag://hackreduce:millionsongs:subset"],
                        map=map,
                        reduce=reduce,
                        save=True).wait()
print "Job done. Results:"

# Print result to user
xs = []
ys = []
d = {}

for x, y in result_iterator(results):
    d[y] = x
    
keys = d.keys()
keys.sort()
keys.reverse()
for key in keys:
    print d[key], key
