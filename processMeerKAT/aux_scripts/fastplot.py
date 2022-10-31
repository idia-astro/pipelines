#!/usr/bin/env python3

import argparse
from matplotlib import use
use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import os,sys,time

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))

from casatools import table,msmetadata
tb = table()
msmd = msmetadata()

import logging
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def get_axis(axis,data,flags,times,spw,startchan,freq_unit='MHz'):

    """Plot data from a measurement set or calibration table.

    Arguments:
    ----------
    axis : str
        Axis to extract ['Freq','Chan','Time','Amp','Phase','Real','Imag'].
    data : Numpy array
        Data from which to extract axis.
    flags : Numpy array
        Flags associated with data.
    times : Numpy array
        The timestamps associated with data.
    spw : int
        The spectral window, for extracting axis 'Freq' in units of freq_unit.
    startchan : int
        The starting channel, for extracting axis 'Chan'.
    freq_unit : str
        The frequency unit, for extracting axis 'Freq'. Default is 'MHz'."""

    #Data shape: (corrs,chans,rows)
    ncorrs,nchans,nrows = data.shape

    if axis in ['Amplitude','Amp']:
        data = np.ma.masked_array(np.absolute(data),flags)
    elif axis == 'Phase':
        data = np.ma.masked_array(np.angle(data,deg=True),flags)
    elif axis in ['Imag','Imaginary']:
        data = np.ma.masked_array(np.imag(data),flags)
    elif axis == 'Real':
        data = np.ma.masked_array(np.real(data),flags)
    elif axis in ['Chan','Channel','Freq','Frequency']:
        if axis in ['Chan','Channel']:
            data = np.arange(startchan,startchan+nchans)
        elif axis in ['Freq','Frequency']:
            try:
                data = msmd.chanfreqs(spw,unit=freq_unit)
            except RuntimeError:
                logger.error("Can't use 'Freq' for caltables. Use 'Chan'.")
                sys.exit(1)
        #TODO: iterate over these to reduce data size
        data = np.rollaxis(np.tile(data,(ncorrs,nrows,1)),2,1)

    elif axis == 'Time':
        data = times
        data = np.tile(data,(ncorrs,nchans,1))
    else:
        logger.error("Unknown axis - '{0}'".format(axis))
        logger.error('Use one of the following: {0}'.format(['Freq','Chan','Time','Amp','Phase','Real','Imag']))
        plt.close()
        sys.exit(1)

    return data

def fastplot(MS, col='DATA', field='', antenna='', xaxis='Chan', yaxis='Amp', freq_unit='MHz', fname='plot.png', logy=False, markersize=1, extent=0.1):

    """Plot data from a measurement set or calibration table.

    Arguments:
    ----------
    MS : str
        Path to measurement set or calibration table.
    col : str, optional
        Plot data of this column. Use 'DATA' for MS and 'CPARAM' for calibration table.
    field : str, optional
        Plot data for this field.
    antenna : str, optional
        Plot data for this antenna.
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

    ext = os.path.splitext(fname)[1]
    if ext in ['pdf','eps','ps']:
        logger.warning('"{0}" format will take some time to save and then open for large numbers of data points. "png" or "jpg" recommended.'.format(ext))

    try:
        msmd.open(MS)
        nspw = msmd.nspw()
    except RuntimeError:
        logger.info("Assuming '{0}' is a caltable and setting column as 'CPARAM'.".format(MS))
        nspw = 1
        col = 'CPARAM'

    start = time.process_time()
    xaxis = xaxis.title()
    yaxis = yaxis.title()
    startchan = 0

    fig = plt.figure(figsize=(15,12))
    tab = tb.open(MS)

    for spw in range(nspw):
        spwstart = time.process_time()

        try:
            if nspw > 1:
                query = 'DATA_DESC_ID=={0}'.format(spw)
            else:
                query = ''
            if field != '':
                if query != '':
                    query += ' AND '
                query += 'FIELD_ID == {0}'.format(field)
            if antenna != '':
                if query != '':
                    query += ' AND '
                query += 'ANTENNA1 == {0}'.format(antenna)
            dat = tb.query(query, columns='{0},FLAG,TIME'.format(col))
            data = dat.getcol(col)
            nchans = data.shape[1]

            if data is None:
                logger.info("Field '{0}' may not exist".format(field))
                sys.exit()

            flags = dat.getcol('FLAG')
            times = dat.getcol('TIME')

            loadtime = time.process_time()
        except (RuntimeError,AttributeError) as e:
            logger.info("Column '{0}' may not exist. Use 'CPARAM' for calibration tables and 'DATA' for MSs.".format(col,field))
    #        logger.info("Columns are {0}.".format(tb.colnames()))
            logger.info(e)
            sys.exit()

        logger.info('Extracted data with shape {0} in {1:.0f} seconds.'.format(data.shape,loadtime-spwstart))

        x = get_axis(xaxis,data,flags,times,spw,startchan,freq_unit=freq_unit)
        y = get_axis(yaxis,data,flags,times,spw,startchan,freq_unit=freq_unit)

        if len(y.shape) > 1:
            y = y.flatten()
        if len(x.shape) > 1:
            x = x.flatten()

        plt.plot(x,y,'.',markersize=markersize)

        xmax = max(plt.gca().get_xlim()[0],np.max(x))
        xmin = min(plt.gca().get_xlim()[1],np.min(x))

        plottime = time.process_time()
        logger.info('Plotted {0} points from SPW {1} in {2:.0f} seconds.'.format(data.size,spw,plottime - loadtime))

        if 'Chan' in xaxis or 'Chan' in yaxis:
            startchan += nchans

        #Garbage collection
        del x,y,data,flags,times

    if extent != 0.0:
        xmax = xmax + xmax*extent
        xmin = xmin - xmax*extent
        plt.xlim(xmin,xmax)

    if 'Freq' in xaxis:
        xaxis += ' ({0})'.format(freq_unit)
    if 'Amp' in yaxis:
        yaxis += ' (Jy)'
    plt.xlabel(xaxis)
    plt.ylabel(yaxis)
    if logy:
        plt.yscale('log')

    tb.close()
    plt.savefig(fname)
    savetime = time.process_time()
    logger.info("Wrote figure '{0}' in {1:.0f} seconds.".format(fname,savetime - plottime))
    plt.close()


def parse_args():

    """Parse arguments into this script.

    Returns:
    --------
    args : class ``argparse.ArgumentParser``
        Known and validated arguments."""

    parser = argparse.ArgumentParser(description='Fast plot of data from MS or caltable.')

    parser.add_argument("-M","--MS", metavar="path", required=True, type=str, help="Path to MeasurementSet or caltable.")
    parser.add_argument("-c","--col", metavar="column", required=False, type=str, default='DATA', help="Column (e.g. 'DATA', 'CORRECTED_DATA', 'MODEL_DATA', or 'CPARAM' for caltables). Default: DATA")
    parser.add_argument("-F","--field", metavar="ID", required=False, type=str, default='', help="Field ID (number). Default: ''")
    parser.add_argument("-a","--antenna", metavar="number", required=False, type=str, default='', help="Antenna (number). Default: ''")
    parser.add_argument("-x","--xaxis", metavar="axis", required=False, type=str, default='Freq', help="X-axis: ['Freq','Chan','Time','Amp','Phase','Real','Imag']. Default: Freq")
    parser.add_argument("-y","--yaxis", metavar="axis", required=False, type=str, default='Amp', help="Y-axis: ['Freq','Chan','Time','Amp','Phase','Real','Imag']. Default: Amp")
    parser.add_argument("-u","--freq_unit", metavar="unit", required=False, type=str, default='MHz', help="Frequency unit for labelling 'Freq' axis. Default: MHz")
    parser.add_argument("-f","--fname", metavar="filename", required=False, type=str, default='plot.png', help="Output filename (and extension). Default: plot.png")
    parser.add_argument("-l","--logy", action="store_true", required=False, default=False, help="Log the y-axis? Default: False")
    parser.add_argument("-m","--markersize",metavar="size", required=False, default=1, type=int, help="Plot marker size. Default: 1")
    parser.add_argument("-e","--extent",metavar="extend", required=False, type=float, default=0.0, help="Scale the limits of the x-axis by this fraction its maximum value. Default: 0.0")

    args, unknown = parser.parse_known_args()

    if len(unknown) > 0:
        parser.error('Unknown input argument(s) present - {0}'.format(unknown))

    return args

def main():

    args = parse_args()
    fastplot(**vars(args))

if __name__ == "__main__":
    main()
