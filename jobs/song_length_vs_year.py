#!/usr/bin/env python
# Script that shows (year, num_songs_with_"love"_in_title)
# conclusion: 1999 is the most romantic year

import numpy as np
import matplotlib.pyplot as plt
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
        x = int(line[lookup['get_year']])
        y = float(line[lookup['get_duration']])
        if x != 0:
            yield x, y

def reduce(iter, params):
    from disco.util import kvgroup
    for x, y in kvgroup(sorted(iter)):
        ys = list(y)
        length = float(len(ys))
        yield x, sum(ys)/length

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

for x, y in result_iterator(results):
    xs.append(x)
    ys.append(y)

print xs
print ys

fig = plt.figure(figsize=(10, 8))
ax = fig.add_subplot(111)
z = np.polyfit(xs, ys, 4)
p = np.poly1d(z)
plt.plot(xs, p(xs), "r--")
plt.scatter(xs, ys)


ax.annotate('LSD', xy=(1966, 235), xytext=(1956, 225),
            arrowprops=dict(facecolor='black', shrink=0.05),
            )

fig.suptitle("Average Song Length by Year")
plt.xlabel("Year")
plt.ylabel("Song Length (seconds)")
fig.savefig("song_length_vs_year.png")
