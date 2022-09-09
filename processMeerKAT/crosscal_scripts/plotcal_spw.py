#Copyright (C) 2022 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

#!/usr/bin/env python3

import sys
import os

# Adapt PYTHONPATH to include processMeerKAT
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import glob
import config_parser
import traceback

import matplotlib
# Agg doesn't need X - matplotlib doesn't work with xvfb
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

from config_parser import validate_args as va
import bookkeeping
import glob
PLOT_DIR = 'plots'
EXTN = 'png'

from casatasks import *
from casatools import table,msmetadata
tb = table()
msmd = msmetadata()
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def avg_ants(arrlist):
    return [np.mean(arr, axis=-1) for arr in arrlist]


def lengthen(edat, inpdat):
    """
    Tries to extend, if that fails it appends
    """

    try:
        edat.extend(inpdat)
    except TypeError:
        edat.append(inpdat)

    return edat



def plotcal(plotstr, field_id, dirs, caldir, table_ext, title, outname, xlim=None, ylim=None):
    # Check plotstr and ant
    if not any([plotstr in ss for ss in ['amp,time', 'phase,time', 'amp,freq', 'phase,freq', 'delay,freq', 'imag,real']]):
        raise ValueError("Invalid plotstr.")


    tables = []
    cwd = os.getcwd()
    for dd in dirs:
        tmpdir = os.path.join(dd, caldir)
        if not os.path.exists(tmpdir):
            logger.warning("Path {} not found. Skipping.".format(tmpdir))

        os.chdir(tmpdir)
        tmp = glob.glob("*.{}".format(table_ext))
        tables.extend([os.path.join(tmpdir, tt) for tt in tmp if os.path.exists(tt)])
        os.chdir(cwd)

    if len(tables) == 0:
        logger.warning("No valid caltables with extention {} found.".format(table_ext))
        logger.warning("Skipping.")
        return

    xdat = []
    xdaty = [] # Only used when plotting real
    ydatx = []
    ydaty = []
    fields = []

    xstr = plotstr.split(',')[1]
    ystr = plotstr.split(',')[0]
    do_field_sel = False
    field = 0


    for tt in tables:
        tb.open(tt+'/ANTENNA')
        ant_name = tb.getcol('NAME')
        tb.close()

        nant = ant_name.size

        tb.open(tt)
        field = tb.getcol('FIELD_ID')
        if np.unique(field).size == 1:
            do_field_sel = False
        else:
            do_field_sel = True
            fields.append(field)

        tb.close()

        if 'freq' in xstr.lower():
            tb.open(tt+'/SPECTRAL_WINDOW')
            chanfreq = np.squeeze(tb.getcol('CHAN_FREQ'))/1E6
            tb.close()
            xlabel = 'Frequency (MHz)'

            if 'delay' in ystr.lower():
                xdat.append(chanfreq)
            else:
                xdat.extend(chanfreq)

        elif 'time' in xstr.lower():
            tb.open(tt)
            time = np.squeeze(tb.getcol('TIME'))
            xdat = lengthen(xdat, time)

            xlabel = 'Time'

        elif 'real' in xstr.lower():
            tb.open(tt)
            real = np.squeeze(tb.getcol('CPARAM')).real
            xdat = lengthen(xdat, real[0])
            xdaty = lengthen(xdaty, real[1])
            #xdat.extend(real[0])
            #xdaty.extend(real[1])
            xlabel = 'Real'
        else:
            # This should never happen
            raise ValueError("Unknown option {}.".format(xstr.lower()))

        tb.open(tt)
        if 'delay' in ystr.lower():
            dat = np.squeeze(tb.getcol('FPARAM'))
        else:
            dat = np.squeeze(tb.getcol('CPARAM'))

        if len(dat.shape) == 1:
            npol = 1
        else:
            npol = dat.shape[0]

        if npol == 1:
            datx = dat
            daty = np.zeros_like(dat)
        else:
            datx = dat[0]
            daty = dat[-1]

        tb.close()


        if 'amp' in ystr.lower():
            ydatx = lengthen(ydatx, np.abs(datx))
            ydaty = lengthen(ydaty, np.abs(daty))

            ylabel = 'Amplitude'

        elif 'phase' in ystr.lower():
            ydatx = lengthen(ydatx, np.rad2deg(np.angle(datx)))
            ydaty = lengthen(ydaty, np.rad2deg(np.angle(daty)))
            #ydatx.extend(np.rad2deg(np.angle(datx)))
            #ydaty.extend(np.rad2deg(np.angle(daty)))

            ylabel = 'Phase (deg)'

        elif 'imag' in ystr.lower():
            ydatx = lengthen(ydatx, datx.imag)
            ydaty = lengthen(ydaty, daty.imag)

            ylabel = 'Imag'

        elif 'delay' in ystr.lower():
            ydatx = lengthen(ydatx, datx)
            ydaty = lengthen(ydaty, daty)

            ylabel = 'Delay'
        else:
            raise ValueError("Unknown option {}".format(ystr.lower()))

    fields = np.asarray(fields)

    xdat = np.asarray(xdat)
    if 'real' in xstr.lower():
        xdaty = np.asarray(xdaty)

    ydatx = np.asarray(ydatx)
    ydaty = np.asarray(ydaty)


    if do_field_sel:
        xdat = xdat.reshape(fields.shape)
        ydatx = ydatx.reshape(fields.shape)
        ydaty = ydaty.reshape(fields.shape)

        idx = np.where(fields == field_id)
        xdat = xdat[idx]
        ydatx = ydatx[idx]
        ydaty = ydaty[idx]


        if 'real' in xstr.lower():
            xdaty = xdaty.reshape(fields.shape)
            xdaty = xdaty[idx]
    else:
        logger.warning("No field selection performed. Only one field present in the caltable")
        field_id = np.unique(field)[0]

    if 'freq' in xstr.lower():
        ydatx, ydaty = avg_ants([ydatx, ydaty])

    if 'time' in xstr.lower() or 'real' in xstr.lower():
        ydatx = np.asarray(ydatx).reshape(xdat.shape)
        ydaty = np.asarray(ydaty).reshape(xdat.shape)

        npt = xdat.shape[0]//nant
        xdat = xdat.reshape(npt, nant)
        ydatx = ydatx.reshape(npt, nant)
        ydaty = ydaty.reshape(npt, nant)

        if 'real' in xstr.lower():
            xdaty = xdaty.reshape(npt, nant)

        if 'real' in xstr.lower():
            ydatx, ydaty, xdat, xdaty = avg_ants([ydatx, ydaty, xdat, xdaty])
        else:
            ydatx, ydaty, xdat = avg_ants([ydatx, ydaty, xdat])

    plt.ioff()
    fig, ax = plt.subplots()

    if 'real' in xstr.lower():
        if npol == 1:
            ax.scatter(xdat, ydatx, label='Pol Avg', facecolor='blue', edgecolor='none')
        else:
            ax.scatter(xdat, ydatx, label='X', facecolor='blue', edgecolor='none')
            ax.scatter(xdaty, ydaty, label='Y', facecolor='orange', edgecolor='none')
    else:
        if npol == 1:
            ax.scatter(xdat, ydatx, label='Pol Avg', facecolor='blue', edgecolor='none')
        else:
            ax.scatter(xdat, ydatx, label='X', facecolor='blue', edgecolor='none')
            ax.scatter(xdat, ydaty, label='Y', facecolor='orange', edgecolor='none')

    title = 'Antenna Average Field {} {}'.format(field_id, title)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    ax.set_xlim(xlim)
    ax.set_ylim(ylim)

    plt.legend()
    plt.tight_layout()

    plt.savefig('{0}.{1}'.format(outname,EXTN), bbox_inches='tight')


def main(args,taskvals):

    try:
        if not os.path.exists(PLOT_DIR):
            os.makedirs(PLOT_DIR)

        fields = bookkeeping.get_field_ids(taskvals['fields'])
        visname = va(taskvals, 'run', 'crosscal_vis', str)
        polfield = bookkeeping.polfield_name(visname)

        msmd.open(visname)

        caldir = 'caltables'
        spwdir = config_parser.parse_spw(args['config'])[3]

        if type(spwdir) is str:
            spwdir = glob.glob(spwdir)

        for ff in fields.gainfields.split(','):
            plotstr='phase,time'
            table_ext = 'gcal'
            title='Gain Phase'
            outname = '{}/field_{}_gain_phase'.format(PLOT_DIR,ff)
            plotcal(plotstr, int(msmd.fieldsforname(ff)[0]), spwdir, caldir, table_ext, title, outname)

            plotstr='amp,time'
            table_ext = 'gcal'
            title='Gain Amp'
            outname = '{}/field_{}_gain_amp'.format(PLOT_DIR,ff)
            plotcal(plotstr, int(msmd.fieldsforname(ff)[0]), spwdir, caldir, table_ext, title, outname)

        #print("k")
        #plotstr='delay,freq'
        #table_ext = 'kcal'
        #title='Delay'
        #outname = '{}/field_{}_delay'.format(PLOT_DIR,fields.fluxfield)
        #plotcal(plotstr, int(msmd.fieldsforname(fields.fluxfield)[0]), spwdir, caldir, table_ext, title, outname)

        #print("kcross")
        #plotstr='delay,freq'
        #table_ext = 'xdel'
        #title='Crosshand Delay'
        #outname = '{}/field_{}_crosshanddelay'.format(PLOT_DIR,fields.fluxfield)
        #plotcal(plotstr, int(msmd.fieldsforname(fields.fluxfield)[0]), spwdir, caldir, table_ext, title, outname)

        plotstr='amp,freq'
        table_ext = 'bcal'
        title='Bandpass Amp'
        outname = '{}/field_{}_bandpass_amp'.format(PLOT_DIR,fields.fluxfield)
        plotcal(plotstr, int(msmd.fieldsforname(fields.fluxfield)[0]), spwdir, caldir, table_ext, title, outname)

        plotstr='phase,freq'
        table_ext = 'bcal'
        title='Bandpass Phase'
        outname = '{}/field_{}_bandpass_phase'.format(PLOT_DIR,fields.fluxfield)
        plotcal(plotstr, int(msmd.fieldsforname(fields.fluxfield)[0]), spwdir, caldir, table_ext, title, outname)

        plotstr='amp,freq'
        table_ext = 'pcal'
        title='Leakage Amp'
        outname = '{}/field_{}_leakage_amp'.format(PLOT_DIR,fields.bpassfield)
        plotcal(plotstr, int(msmd.fieldsforname(fields.dpolfield)[0]), spwdir, caldir, table_ext, title, outname, None, [0, 0.1])
        plotstr='phase,freq'
        table_ext = 'pcal'
        title='Leakage Phase'
        outname = '{}/field_{}_leakage_phase'.format(PLOT_DIR,fields.bpassfield)
        plotcal(plotstr, int(msmd.fieldsforname(fields.dpolfield)[0]), spwdir, caldir, table_ext, title, outname)

        plotstr='phase,freq'
        table_ext = 'xyambcal'
        title='XY Phase'
        outname = '{}/field_{}_xyamb_phase'.format(PLOT_DIR,polfield)
        plotcal(plotstr, int(msmd.fieldsforname(polfield)[0]), spwdir, caldir, table_ext, title, outname)

        plotstr='phase,freq'
        table_ext = 'xycal'
        title='XY Phase (amb resolved)'
        outname = '{}/field_{}_xy_phase'.format(PLOT_DIR,polfield)
        plotcal(plotstr, int(msmd.fieldsforname(polfield)[0]), spwdir, caldir, table_ext, title, outname)

        msmd.done()

    except Exception as err:
        logger.error('Exception found in the pipeline of type {0}: {1}'.format(type(err),err))
        logger.error(traceback.format_exc())
        msmd.done()

if __name__ == "__main__":

    bookkeeping.run_script(main,logfile)
