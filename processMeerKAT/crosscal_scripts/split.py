#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os

# Adapt PYTHONPATH to include processMeerKAT
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import config_parser
import bookkeeping
from config_parser import validate_args as va

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
from casatools import msmetadata
import casampi
msmd = msmetadata()

def split_vis(visname, spw, fields, specavg, timeavg, keepmms):

    outputbase = os.path.splitext(os.path.split(visname)[1])[0]
    extn = 'mms' if keepmms else 'ms'
    newvis = visname

    for field in fields:
        if field != '':
            for fname in field.split(','):
                if fname.isdigit():
                    fname = msmd.namesforfields(int(fname))[0]

                outname = '%s.%s.%s' % (outputbase, fname, extn)
                if not os.path.exists(outname):

                    split(vis=visname, outputvis=outname, datacolumn='corrected',
                                field=fname, spw=spw, keepflags=True, keepmms=keepmms,
                                width=specavg, timebin=timeavg)

                if fname == fields.targetfield.split(',')[0]:
                    newvis = outname

    return newvis

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    spw = va(taskvals, 'crosscal', 'spw', str, default='')

    specavg = va(taskvals, 'crosscal', 'width', int, default=1)
    timeavg = va(taskvals, 'crosscal', 'timeavg', str, default='8s')
    keepmms = va(taskvals, 'crosscal', 'keepmms', bool)

    msmd.open(visname)
    newvis = split_vis(visname, spw, fields, specavg, timeavg, keepmms)

    config_parser.overwrite_config(args['config'], conf_dict={'vis' : "'{0}'".format(newvis)}, conf_sec='data')
    config_parser.overwrite_config(args['config'], conf_dict={'crosscal_vis': "'{0}'".format(visname)}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
    msmd.done()

if __name__ == '__main__':

    bookkeeping.run_script(main,logfile)
