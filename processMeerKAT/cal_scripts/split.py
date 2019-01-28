import sys
import os

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va

def split_vis(visname, spw, fields, specave, timeave):
    outputbase = visname.replace('.mms', '')

    targfields = fields.targetfield.split(',')
    secfields = fields.secondaryfield.split(',')

    msmd.open(visname)
    fnames = msmd.namesforfields([int(ff) for ff in targfields])
    secondaryname = msmd.namesforfields([int(ss) for ss in secfields])
    primaryname = msmd.namesforfields(int(fields.fluxfield))
    msmd.close()


    for ind, field in enumerate(fnames):
        split(vis=visname, outputvis = outputbase+'.'+field+'.mms',
                datacolumn='corrected', field = targfields[ind], spw = spw,
                keepflags=False, keepmms = True, width = specave,
                timebin = timeave)


    if len(secondaryname) > 1:
        for ind, sname in enumerate(secondaryname):
            split(vis=visname, outputvis = outputbase+'.'+sname+'.mms',
                    datacolumn='corrected', field = secfields[ind], spw = spw,
                    keepflags=False, keepmms = True, width = specave, timebin = timeave)


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

    specave = va(taskvals, 'crosscal', 'specave', int, default=1)
    timeave = va(taskvals, 'crosscal', 'timeave', str, default='8s')

    split_vis(visname, spw, fields, specave, timeave)
