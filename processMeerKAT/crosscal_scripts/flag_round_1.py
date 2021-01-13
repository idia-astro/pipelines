#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os

import config_parser
from config_parser import validate_args as va
import bookkeeping

from casatasks import *
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
import casampi

def do_pre_flag(visname, fields, badfreqranges, badants):

    clip = [0., 50.]

    if len(badfreqranges):
        badspw = '0:' + ',0:'.join(badfreqranges)
        flagdata(vis=visname, mode='manual', spw=badspw)

    if len(badants):
        badants = ",".join([str(bb) for bb in badants])
        flagdata(vis=visname, mode='manual', antenna=badants)

    flagdata(vis=visname, mode='manual', autocorr=True, action='apply',
            flagbackup=True, savepars=False, writeflags=True)

    #Manually clip all fields in config
    allfields = ','.join(set([i for i in (','.join([fields.gainfields] + [fields.targetfield] + [fields.extrafields]).split(',')) if i])) #remove duplicate and empty fields

    flagdata(vis=visname, mode="clip", field=allfields,
            clipminmax=clip, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    #tfcrop calfields and targetfield with different cutoffs / fits
    calfields = ','.join(set([i for i in (','.join([fields.gainfields] + [fields.extrafields]).split(',')) if i])) #remove duplicate and empty fields

    flagdata(vis=visname, mode='tfcrop', field=calfields,
            ntime='scan', timecutoff=5.0, freqcutoff=5.0, timefit='line',
            freqfit='line', extendflags=False, timedevscale=5., freqdevscale=5.,
            extendpols=True, growaround=False, action='apply', flagbackup=True,
            overwrite=True, writeflags=True, datacolumn='DATA')

    flagdata(vis=visname, mode='tfcrop', field=fields.targetfield,
            ntime='scan', timecutoff=6.0, freqcutoff=6.0, timefit='poly',
            freqfit='poly', extendflags=False, timedevscale=5., freqdevscale=5.,
            extendpols=True, growaround=False, action='apply', flagbackup=True,
            overwrite=True, writeflags=True, datacolumn='DATA')

    # Conservatively extend flags for all fields in config
    flagdata(vis=visname, mode='extend', field=allfields,
            datacolumn='data', clipzeros=True, ntime='scan', extendflags=False,
            extendpols=True, growtime=80., growfreq=80., growaround=False,
            flagneartime=False, flagnearfreq=False, action='apply',
            flagbackup=True, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='summary', datacolumn='DATA',
            name=visname+'.flag.summary')

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)

    badfreqranges = taskvals['crosscal'].pop('badfreqranges', ['935~947MHz', '1160~1310MHz', '1476~1611MHz', '1670~1700MHz'])
    badants = taskvals['crosscal'].pop('badants')

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    do_pre_flag(visname, fields, badfreqranges, badants)

if __name__ == '__main__':

    bookkeeping.run_script(main)
