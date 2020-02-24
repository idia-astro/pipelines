#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os

import config_parser
from config_parser import validate_args as va

def selfcal_part1(vis, imagename, imsize, cell, gridder, wprojplanes,
                   deconvolver, robust, niter, multiscale, threshold,
                   nterms, regionfile, restart_no, nloop, solint, calmode,
                   atrous, loop):

    ll = params['loop']

    if ll == 0:
        imagename = vis.replace('.ms', '') + '_im_%d_0' % (ll + restart_no)
        regionfile = ''
    else:
        imagename = vis.replace('.ms', '') + '_im_%d' % (ll + restart_no):

        if ll < (params['nloop']-1):
            caltable = vis.replace('.ms', '') + '.gcal%d' % (ll + restart_no)

            applycal(vis=vis, selectdata=False, gaintable=caltable, parang=True)

            flagdata(vis=vis, mode='rflag', datacolumn='RESIDUAL', field='', timecutoff=5.0,
                    freqcutoff=5.0, timefit='line', freqfit='line', flagdimension='freqtime',
                    extendflags=False, timedevscale=3.0, freqdevscale=3.0, spectralmax=500,
                    extendpols=False, growaround=False, flagneartime=False, flagnearfreq=False,


    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize, cell=cell, stokes='I', gridder=gridder,
            wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=True,
            weighting='briggs', robust = robust, niter=niter[ll], multiscale=multiscale[ll],
            threshold=threshold[ll], nterms=nterms[ll],
            savemodel='none', pblimit=-1, mask=regionfile, parallel = True)


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['selfcal']

    for arg in ['multiscale','nterms','calmode','atrous']:
        params[arg] = [params[arg]] * len(params['niter'])

    if 'loop' not in params:
        params['loop'] = 0

    selfcal_part1(**params)

