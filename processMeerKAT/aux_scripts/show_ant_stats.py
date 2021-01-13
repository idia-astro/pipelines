#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

#!/usr/bin/env python3
import sys, os
import numpy as np
import matplotlib.pyplot as plt

args=sys.argv
fname = 'ant_stats.txt'
bins = 20

#Cheaps man's argparse
if len(args) > 1:
    if sys.argv[1] == '-h':
        print('Usage: {0} filename bins'.format(os.path.split(sys.argv[0])[1]))
    else:
        fname = sys.argv[1]
        bins = int(sys.argv[2])

dat=open(fname).read()
lines = dat.split('\n')[1:-1] #skip header and remove last line
ants=np.empty(len(lines),dtype=np.int)
flags=np.zeros(len(lines))

for i,line in enumerate(lines):
    split = line.split()
    ants[i] = split[0]
    flags[i] = split[-1]

refant = ants[np.argmin(flags)]
mask = (flags < 1) & (flags > 0)
label='Flagged % over {0} antennas\n({1} flagged out)'.format(flags[mask].size,flags[~mask].size)
plt.hist(flags[mask]*100,bins=bins,label=label)
refant_label='Reference antenna ({0})'.format(refant)
plt.hist(flags[refant]*100,label=refant_label,color='r')
plt.xlabel('Flagged Percentage')
plt.ylabel('N')
plt.legend()
plt.savefig('{0}_hist.png'.format(os.path.splitext(fname)[0]))
plt.close()

plt.scatter(ants[mask],flags[mask]*100,label=label)
plt.scatter(ants[refant],flags[refant]*100,label=refant_label,color='r')
plt.xlabel('Antenna')
plt.ylabel('Flagged Percentage')
plt.legend()
plt.savefig('{0}_plot.png'.format(os.path.splitext(fname)[0]))
