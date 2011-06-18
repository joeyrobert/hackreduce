#!/usr/bin/env python
# conclusion: lots of one hit wonders and {'hits': 13, 'artist': 'Mario Rosenstock'} Mario Rosenstock is the most active artist

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
        artist = line[lookup['get_artist_name']]
        yield artist, 1

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
laziest = {'artist' : None, 'hits' : 10000}
active = {'artist' : None, 'hits' : 0}
for x, y in result_iterator(results):
    print x, y
    if laziest.get('hits') > y:
        laziest['hits'] = y
        laziest['artist'] = x
    if active.get('hits') < y:
        active['hits'] = y
        active['artist'] = x

print laziest
print active
