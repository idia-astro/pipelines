import os

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va


def do_setjy(visname, spw, fields, standard):
    clearcal(vis=visname)

    fluxlist = ['J0408-6545', '0408-6545', '']

    print " starting setjy for flux calibrator"
    if any([ff in fields.fluxfield for ff in fluxlist]):
        for ff in fluxlist:
            if ff in fields.fluxfield:
                fieldname = ff

        smodel = [17.066, 0.0, 0.0, 0.0]
        spix = [-1.179]
        reffreq="1284MHz"

        print "Using manual flux density scale - "
        print "Flux model: ", smodel
        print "Spix: ", spix
        print "Ref freq ", refreq

        setjy(vis=visname, field=fieldname, scalebychan=True, standard='manual',
                fluxdensity=smodel, spix=spix, refreq=refreq)
    else:
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
