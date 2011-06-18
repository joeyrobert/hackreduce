import hdf5_getters
import os
import csv

files = [i for i in os.popen('find /home/joey/Data/MillionSongSubset/data -name "*.h5"').read().split('\n') if i != '']
csv_file = open("results.csv", 'wb')
wr = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
counter = 0

for file in files:
    counter += 1
    info = []
    h5 = hdf5_getters.open_h5_file_read(file)
    attributes = [i for i in dir(hdf5_getters) if i[0:3] == "get"]
    working_attributes = []

    print "Reading file %d" % counter
    for attribute in attributes:
        result = eval("hdf5_getters." + attribute + "(h5)")
        if(str(result.__class__) == "<type 'numpy.ndarray'>"):
            continue
        info.append(result)
        working_attributes.append(attribute)
    #print working_attributes
    wr.writerow(info)

h5.close()
