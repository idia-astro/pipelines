#!/usr/bin/env python3

import os
import glob
import config_parser
import matplotlib
# Agg doesn't need X - matplotlib doesn't work with xvfb
matplotlib.use('Agg', warn=False)
import matplotlib.pyplot as plt


from config_parser import validate_args as va
from cal_scripts import bookkeeping
import glob
PLOT_DIR = 'plots'
EXTN = 'pdf'

def avg_ants(arrlist):
    return [np.mean(arr, axis=-1) for arr in arrlist]

def plotcal(plotstr, field_id, spwdir, caldir, table_ext, title, outname, xlim=None, ylim=None):
    # Check plotstr and ant
    if not any([plotstr in ss for ss in ['amp,time', 'phase,time', 'amp,freq', 'phase,freq', 'delay,freq', 'imag,real']]):
        raise ValueError("Invalid plotstr.")

    tables = []
    #dirs = glob.glob(os.path.join(path, spwdir))
    dirs = glob.glob(spwdir)
    cwd = os.getcwd()
    for dd in dirs:
        tmpdir = os.path.join(dd, caldir)
        os.chdir(tmpdir)
        tmp = glob.glob("*.{}".format(table_ext))
        tables.extend([os.path.join(tmpdir, tt) for tt in tmp if os.path.exists(tt)])
        os.chdir(cwd)

    if len(tables) == 0:
        raise ValueError("No valid caltables with extention {} found.".format(table_ext))

    xdat = []
    xdaty = [] # Only used when plotting real
    ydatx = []
    ydaty = []
    fields = []

    xstr = plotstr.split(',')[1]
    ystr = plotstr.split(',')[0]

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
            try:
                xdat.extend(time)
            except AttributeError:
                xdat.append(time)

            xlabel = 'Time'

        elif 'real' in xstr.lower():
            tb.open(tt)
            real = np.squeeze(tb.getcol('CPARAM')).real
            xdat.extend(real[0])
            xdaty.extend(real[1])
            xlabel = 'Real'
        else:
            raise ValueError("Unknown option {}.".format(xstr.lower()))

        tb.open(tt)
        if 'delay' in ystr.lower():
            dat = np.squeeze(tb.getcol('FPARAM'))
        else:
            dat = np.squeeze(tb.getcol('CPARAM'))

        datx = dat[0]
        daty = dat[1]
        npol = dat.shape[0]
        tb.close()

        if 'amp' in ystr.lower():
            ydatx.extend(np.abs(datx))
            ydaty.extend(np.abs(daty))
            ylabel = 'Amplitude'

        elif 'phase' in ystr.lower():
            ydatx.extend(np.rad2deg(np.angle(datx)))
            ydaty.extend(np.rad2deg(np.angle(daty)))
            ylabel = 'Phase (deg)'

        elif 'imag' in ystr.lower():
            ydatx.extend(datx.imag)
            ydaty.extend(daty.imag)
            ylabel = 'Imag'

        elif 'delay' in ystr.lower():
            ydatx.extend(datx)
            ydaty.extend(daty)
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
        print("Warning : No field selection performed. Only one field present in the caltable")
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
        ax.scatter(xdat, ydatx, label='X', facecolor='blue', edgecolor='none')
        ax.scatter(xdaty, ydaty, label='Y', facecolor='orange', edgecolor='none')
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

    plt.savefig(outname+'.pdf', bbox_inches='tight')
    plt.savefig(outname+'.png', bbox_inches='tight')



def main(args,taskvals):
    try:
        fields = bookkeeping.get_field_ids(taskvals['fields'])

        caldir = 'caltables'
        spwdir = "*MHz"

        for ff in fields.gainfields.split(','):
            plotstr='phase,time'
            table_ext = 'gcal'
            title='Gain Phase'
            outname = 'field_{}_gain_phase'.format(ff)
            plotcal(plotstr, int(ff), spwdir, caldir, table_ext, title, outname)

            plotstr='amp,time'
            table_ext = 'gcal'
            title='Gain Amp'
            outname = 'field_{}_gain_amp'.format(ff)
            plotcal(plotstr, int(ff), spwdir, caldir, table_ext, title, outname)

        #print("k")
        #plotstr='delay,freq'
        #table_ext = 'kcal'
        #title='Delay'
        #outname = 'field_{}_delay'.format(fields.fluxfield)
        #plotcal(plotstr, int(fields.fluxfield), spwdir, caldir, table_ext, title, outname)

        #print("kcross")
        #plotstr='delay,freq'
        #table_ext = 'xdel'
        #title='Crosshand Delay'
        #outname = 'field_{}_crosshanddelay'.format(fields.fluxfield)
        #plotcal(plotstr, int(fields.fluxfield), spwdir, caldir, table_ext, title, outname)

        plotstr='amp,freq'
        table_ext = 'bcal'
        title='Bandpass Amp'
        outname = 'field_{}_bandpass_amp'.format(fields.fluxfield)
        plotcal(plotstr, int(fields.fluxfield), spwdir, caldir, table_ext, title, outname)

        plotstr='phase,freq'
        table_ext = 'bcal'
        title='Bandpass Phase'
        outname = 'field_{}_bandpass_phase'.format(fields.fluxfield)
        plotcal(plotstr, int(fields.fluxfield), spwdir, caldir, table_ext, title, outname)

        plotstr='amp,freq'
        table_ext = 'pcal'
        title='Leakage Amp'
        outname = 'field_{}_leakage_amp'.format(fields.dpolfield)
        plotcal(plotstr, int(fields.dpolfield), spwdir, caldir, table_ext, title, outname, None, [0, 0.1])

        plotstr='phase,freq'
        table_ext = 'pcal'
        title='Leakage Phase'
        outname = 'field_{}_leakage_phase'.format(fields.dpolfield)
        plotcal(plotstr, int(fields.dpolfield), spwdir, caldir, table_ext, title, outname)

        plotstr='phase,freq'
        table_ext = 'xyambcal'
        title='XY Phase'
        outname = 'field_{}_xy_phase'.format(fields.dpolfield)
        plotcal(plotstr, int(fields.dpolfield), spwdir, caldir, table_ext, title, outname)

    except Exception as e:
        print(e)

if __name__ == "__main__":
    bookkeeping.run_script(main)
