import sys
import os

import config_parser
from config_parser import validate_args as va
from cal_scripts import bookkeeping

def run_tclean(visname, fields):
    """
    Run a quick and dirty tclean that will produce an image of the phase cal as well as the target.
    No W projection will be applied, and the image size and cell size are will be restricted to
    1024px and 1arcsec respectively, to keep the image creation reasonably quick.
    """

    impath = os.path.join(os.getcwd(), 'images/')
    if not os.path.exists(impath):
        os.makedirs(impath)

    secimname = visname.replace('.mms', '') + '_%s.im' % (fields.secondaryfield)

    targimname = []
    if len(fields.targetfield) > 1:
        for tt in fields.targetfield:
        targimname.append(visname.replace('.mms', '') + '_%s.im' % (tt))
    else:
        targimname.append(visname.replace('.mms', '') + '_%s.im' % (fields.targetfield))

    tclean(vis=visname, imagename=secimname, datacolumn='corrected',
            field=fields.secondaryfield, imsize=[512,512], threshold=0,
            niter=1000, weighting='briggs', robust=0, cell='1arcsec',
            specmode='mfs', deconvolver='mtmfs', nterms=2, scales=[],
            savemodel='none', gridder='standard', wprojplanes=1,
            restoration=True, pblimit=0, parallel=True)

    for ind, tt in targimname:
        if len(targimname) > 1:
            field = fields.targetfield[ind]
        else:
            field = fields.targetfield

        tclean(vis=visname, imagename=tt, datacolumn='corrected',
                field=field, imsize=[1024,1024], threshold=0,
                niter=1000, weighting='briggs', robust=0, cell='1arcsec',
                specmode='mfs', deconvolver='mtmfs', nterms=2, scales=[],
                savemodel='none', gridder='wproject', wprojplanes=16,
                restoration=True, pblimit=0, parallel=True)


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    visname = os.path.split(visname.replace('.ms', '.mms'))[1]

    fields = bookkeeping.get_field_ids(taskvals['fields'])

    run_tclean(visname, fields)
