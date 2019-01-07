import sys
import os

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va

def do_parallel_cal(visname, fields, calfiles, referenceant,
        minbaselines, standard, do_clearcal=False):

    print " starting antenna-based delay (kcorr)\n -> %s" % calfiles.kcorrfile
    gaincal(vis=visname, caltable = calfiles.kcorrfile, field
            = fields.kcorrfield, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'K',
            solint = 'inf', combine = '', parang = False, append = False)

    print " starting bandpass -> %s" % calfiles.bpassfile
    bandpass(vis=visname, caltable = calfiles.bpassfile,
            field = fields.bpassfield, refant = referenceant,
            minblperant = minbaselines, solnorm = True,  solint = 'inf',
            combine = 'scan', bandtype = 'B', fillgaps = 8,
            gaintable = calfiles.kcorrfile, gainfield = fields.kcorrfield,
            parang = False, append = False)

    print " starting gain calibration\n -> %s" % calfiles.gainfile
    gaincal(vis=visname, caltable = calfiles.gainfile,
            field = fields.gainfields, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'G',
            solint = 'inf', combine = '', calmode='ap',
            gaintable=[calfiles.kcorrfile, calfiles.bpassfile],
            gainfield=[fields.kcorrfield, fields.bpassfield],
            parang = False, append = False)

    # Only run fluxscale if bootstrapping
    if len(fields.gainfields) > 1:
        fluxscale(vis=visname, caltable=calfiles.gainfile,
                reference=[fields.fluxfield], transfer='',
                fluxtable=calfiles.fluxfile, append=False)



if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    visname = os.path.split(visname.replace('.ms', '.mms'))[1]

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    minbaselines = va(taskvals, 'crosscal', 'minbaselines', int, default=4)
    standard = va(taskvals, 'crosscal', 'standard', str, default='Perley-Butler 2010')
    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')

    do_parallel_cal(visname, fields, calfiles, refant,
            minbaselines, standard, do_clearcal=True)
