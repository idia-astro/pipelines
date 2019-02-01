from __future__ import print_function

import os, sys

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def do_setjy(visname, spw, fields, standard):
    clearcal(vis=visname)

    fluxlist = ['J0408-6545', '0408-6545', '']

    msmd.open(visname)
    fnames = msmd.namesforfields([int(ff) for ff in fields.fluxfield.split(',')])
    msmd.done()
    msmd.close()

    do_manual=False
    for ff in fluxlist:
        if ff in fnames:
            setjyname = ff
            do_manual=True
            break
        else:
            setjyname = fields.fluxfield.split(',')[0]


    if do_manual:
        smodel = [17.066, 0.0, 0.0, 0.0]
        spix = [-1.179]
        reffreq="1284MHz"

        logger.info("Using manual flux density scale - ")
        logger.info("Flux model: ", smodel)
        logger.info("Spix: ", spix)
        logger.info("Ref freq ", reffreq)

        setjy(vis=visname, field=setjyname, scalebychan=True, standard='manual',
                fluxdensity=smodel, spix=spix, reffreq=reffreq)
    else:
        setjy(vis=visname, field = setjyname, spw = spw,
                scalebychan=True, standard=standard)

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
