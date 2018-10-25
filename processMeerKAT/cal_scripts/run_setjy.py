import os

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va


def do_setjy(visname, spw, fields, standard):
    clearcal(vis=visname)

    print " starting setjy for flux calibrator"
    setjy(vis=visname, field = fields.fluxfield, spw = spw, scalebychan=True,
            standard=standard)

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
    standard = va(taskvals, 'crosscal', 'standard', str,
            default='Perley-Butler 2010')

    do_setjy(visname, spw, fields, standard)
