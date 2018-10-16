import sys

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va

def split_vis(visname, spw, fields, specave, timeave):
    outputbase = visname.strip('.ms')
    split(vis=visname, outputvis = outputbase+'.'+fields.targetfield+'.ms',
            datacolumn='corrected', field = fields.targetfield, spw = spw,
            keepflags=True, keepmms = True, width = specave, timebin = timeave)

    split(vis=visname, outputvis = outputbase+'.'+fields.secondaryfield+'.ms',
            datacolumn='corrected', field = fields.secondaryfield, spw = spw,
            keepflags=True, keepmms = True, width = specave, timebin = timeave)

if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    visname = visname.replace('.ms', '.mms')

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    spw = va(taskvals, 'crosscal', 'spw', str, default='')

    specave = va(taskvals, 'crosscal', 'specave', int, default=1)
    timeave = va(taskvals, 'crosscal', 'timeave', str, default='8s')

    split_vis(visname, spw, fields, specave, timeave)
