import sys

import config_parser
from cal_scripts import bookkeeping

def do_parallel_cal(visname, spw, fields, calfiles, referenceant,
        minbaselines, do_clearcal=False):
    if do_clearcal:
        clearcal(vis=visname)

    print " starting setjy for flux calibrator"
    setjy(vis=visname, field = fields.fluxfield, spw = spw, scalebychan=True,
            standard='Perley-Butler 2010')

    print " starting antenna-based delay (kcorr)\n -> %s" % calfiles.kcorrfile
    gaincal(vis=visname, caltable = calfiles.kcorrfile, field
            = fields.kcorrfield, spw = spw, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'K',
            solint = 'inf', combine = '', parang = False, append = False)

    print " starting bandpass -> %s" % calfiles.bpassfile
    bandpass(vis=visname, caltable = calfiles.bpassfile,
            field = fields.bpassfield, spw = spw, refant = referenceant,
            minblperant = minbaselines, solnorm = True,  solint = 'inf',
            combine = 'scan', bandtype = 'B', fillgaps = 8,
            gaintable = calfiles.kcorrfile, gainfield = fields.kcorrfield,
            parang = False, append = False)

    print " starting gain calibration\n -> %s" % calfiles.gainfile
    gaincal(vis=visname, caltable = calfiles.gainfile,
            field = fields.gainfields, spw = spw, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'G',
            solint = 'inf', combine = '', calmode='ap',
            gaintable=[calfiles.kcorrfile, calfiles.bpassfile],
            gainfield=[fields.kcorrfield, fields.bpassfield],
            parang = False, append = False)

    fluxscale(vis=visname, caltable=calfiles.gainfile,
            reference=[fields.fluxfield], transfer='',
            fluxtable=calfiles.fluxfile, append=False)


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = taskvals['data']['vis']
    visname = visname.replace('.ms', '.mms')

    calfiles = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    spw = taskvals['crosscal'].pop('spw', '')
    refant = taskvals['crosscal'].pop('referenceant', 'm005')
    minbaselines = taskvals['crosscal'].pop('minbaselines', 4)

    do_parallel_cal(visname, spw, fields, calfiles, refant,
            minbaselines, do_clearcal=True)
