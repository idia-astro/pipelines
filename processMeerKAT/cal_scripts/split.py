import sys
import os

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va

def split_vis(visname, spw, fields, specavg, timeavg, keepmms):
    outputbase = os.path.splitext(visname)[0]

    msmd.open(visname)
    extn = 'mms' if keepmms else 'ms'

    for field in fields:
        for subf in field.split(','):
            fname = msmd.namesforfields(int(subf))[0]

            outname = '%s.%s.%s' % (outputbase, fname, extn)
            if not os.path.exists(outname):
                mstransform(vis=visname, outputvis=outname, datacolumn='corrected',
                        field=fname, spw=spw, keepflags=False, createmms=keepmms,
                        chanaverage=True, chanbin=specavg, timeaverage=True,
                        timebin=timeavg, usewtspectrum=True)


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    spw = va(taskvals, 'crosscal', 'spw', str, default='')

    specavg = va(taskvals, 'crosscal', 'specavg', int, default=1)
    timeavg = va(taskvals, 'crosscal', 'timeavg', str, default='8s')
    keepmms = va(taskvals, 'crosscal', 'keepmms', bool)

    split_vis(visname, spw, fields, specavg, timeavg, keepmms)
