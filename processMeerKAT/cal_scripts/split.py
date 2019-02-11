import sys
import os

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va

def split_vis(visname, spw, fields, specavg, timeavg):
    outputbase = visname.replace('.mms', '')

    msmd.open(visname)

    for field in fields:
        for subf in field.split(','):
            fname = msmd.namesforfields(int(subf))[0]

            outname = '%s.%s.mms' % (outputbase, fname)
            if not os.path.exists(outname):
                split(vis=visname, outputvis=outname,
                        datacolumn='corrected', field=fname, spw=spw,
                        keepflags=False, keepmms=True, width=specavg,
                        timebin=timeavg)


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    visname = os.path.split(visname.replace('.ms', '.mms'))[1]

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    spw = va(taskvals, 'crosscal', 'spw', str, default='')

    specavg = va(taskvals, 'crosscal', 'specavg', int, default=1)
    timeavg = va(taskvals, 'crosscal', 'timeavg', str, default='8s')

    split_vis(visname, spw, fields, specavg, timeavg)
