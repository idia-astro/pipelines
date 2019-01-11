#!/usr/bin/env python2.7

from matplotlib import use
use('Agg', warn=False)
import matplotlib.pyplot as plt
import numpy as np
import os
import sys
import time
import config_parser
from config_parser import validate_args as va
from cal_scripts import bookkeeping
from casacore import tables as tb ###REQUIRES KERN2 container###

def get_axis(axis,data,times):

    """Plot data from a measurement set or calibration table.

    Arguments:
    ----------
    axis : str
        Axis to extract ['Amp','Phase','Real','Imag'].
    data : Numpy array
        Data from which to extract axis.
    times : Numpy array
        The timestamps."""

    if axis in ['Amplitude','Amp']:
        data = np.absolute(data)
    elif axis == 'Phase':
        data = np.angle(data,deg=True)
    elif axis in ['Imag','Imaginary']:
        data = np.imag(data)
    elif axis == 'Real':
        data = np.real(data)
    elif axis in ['Chan','Channel']:
        data = np.arange(0,data.shape[2])
    elif axis == 'Time':
        data = times
    else:
        print "Unknown axis - '{0}'".format(axis)
        plt.close()
        sys.exit()

    return data

def fastplot(MS, col='DATA', field='', xaxis='Chan', yaxis='Amp', fname='fastplot.png', logy=False, markersize=1, extent=0.1):

    """Plot data from a measurement set or calibration table.

    Arguments:
    ----------
    MS : str
        Path to measurement set or calibration table.
    col : str, optional
        Plot data of this column. Use 'DATA' for MS and 'CPARAM' for calibration table.
    field : str, optional
        Plot data for this field.
    xaxis : str, optional
        X-axis to plot ['Chan','Time','Amp','Phase','Real','Imag'].
    yaxis : str, optional
        Y-axis to plot ['Chan','Time','Amp','Phase','Real','Imag'].
    fname : str, optional
        Write plot with this filename.
    logy : bool, optional
        Log the y axis.
    markersize : int, optional
        Use markers of this size.
    extent : float, optional
        Scale the limits of the x-axis by this fraction its maximum value."""

    start = time.clock()
    xaxis = xaxis.title()
    yaxis = yaxis.title()

    #Use time as last axis if being used, otherwise channel
    roll = 1
    if 'Time' in [xaxis,yaxis]:
        roll = 0

    try:
        if field == '':
            query = '1 == 1'
        else:
            query = 'FIELD_ID == {0}'.format(field)

        table = tb.table(MS)
        dat = table.query(query, columns='{0},TIME'.format(col))
        data = dat.getcol(col)
        if data is None:
            print "Field '{0}' may not exist".format(field)
            sys.exit()

        #put channel as last axis, and get antennas and timestamps
        data = np.rollaxis(data,roll,start=3)
        times = dat.getcol('TIME')
        loadtime = time.clock()
    except (RuntimeError,AttributeError),e:
        print "Column '{0}' may not exist. Use 'CPARAM' for calibration tables and 'DATA' for MSs.".format(col,field)
        print "Columns are {0}.".format(table.colnames())
        print e
        sys.exit()

    table.close()

    print 'Extracted data with shape {0} in {1:.0f} seconds.'.format(data.shape,loadtime-start)

    fig = plt.figure()

    x = get_axis(xaxis,data,times)
    y = get_axis(yaxis,data,times)

    if x.size < y.size:
        x = np.tile(x,data.shape[0]*data.shape[1])
    elif y.size < x.size:
        y = np.tile(y,data.shape[0]*data.shape[1])

    if len(y.shape) > 1:
        y = y.flatten()
    if len(x.shape) > 1:
        x = x.flatten()

    plt.plot(x,y,'.',markersize=markersize)

    plt.xlabel(xaxis)
    plt.ylabel(yaxis)

    if extent != 0.0:
        xmax = np.max(x) + np.max(x)*extent
        xmin = np.min(x) - np.max(x)*extent
        plt.xlim(xmin,xmax)
    if logy:
        plt.yscale('log')

    plottime = time.clock()
    print 'Plotted {0} points in {1:.0f} seconds.'.format(data.size,plottime - loadtime)

    plt.savefig(fname)
    savetime = time.clock()
    print "Wrote figure '{0}' in {1:.0f} seconds.".format(fname,savetime - plottime)
    plt.close()


def main():

    config = config_parser.parse_args()['config']

    # Parse config file
    taskvals, config = config_parser.parse_config(config)

    visname = va(taskvals, 'data', 'vis', str)
    visname = os.path.split(visname.replace('.ms', '.mms'))[1]

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    #Plot solutions for bandpass calibrator
    fastplot(calfiles.bpassfile, col='CPARAM', xaxis='Real', yaxis='Imag', fname='bpass_real_imag.png')
    fastplot(calfiles.bpassfile, col='CPARAM', xaxis='chan', yaxis='Amp', logy=True, fname='bpass_chan_amp.png')
    fastplot(calfiles.bpassfile, col='CPARAM', xaxis='chan', yaxis='Phase', fname='bpass_chan_phase.png')

    #Plot solutions for phase calibrator
    fastplot(calfiles.gainfile, col='CPARAM', xaxis='Amp', yaxis='Time', fname='phasecal_time_amp.png', markersize=2, extent=1e-8)
    fastplot(calfiles.gainfile, col='CPARAM', xaxis='Phase', yaxis='Time', fname='phasecal_time_phase.png', markersize=2, extent=1e-8)

if __name__ == "__main__":
    main()
