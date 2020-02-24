import os

# So that CASA can find pyBDSF
os.putenv('PYTHONPATH', '/usr/lib/python2.7/dist-packages/')

nloop   = 5         # Number of clean + bdsm + self-cal loops.
restart_no = 0      # If nonzero, adds this number to nloop to name images
vis     = ''

# The length of the niter, multiscale, and nterms lists should be the same as
# nloop. The length of solint, calmode, and chan_by_chan should be one less than
# nloop. This is because imaging is run nloop times, but self-calibration should
# not be run on the last image, and therefore should be one smaller than nloop

# ----------- Input parameters for imaging
cell            = '2.0arcsec'
robust          = -0.5
imsize          = [8192, 8192]
wprojplanes     = 128

niter           = [8000, 11000, 14000, 15000, 200000]
threshold       = [100e-6, 50e-6, 20e-6, 10e-6, 4e-6] # In units of Jy
multiscale      = [[15, 10, 5]] * len(niter)
nterms          = [2] * len(niter)
gridder         = 'wproject'
deconvolver     = 'mtmfs'

# ----------- Input parameters for self-calibration

solint          = ['10min','5min','2min','1min']
calmode         = ['p'] * len(solint)
chan_by_chan    = [False]*len(solint)

### Source finding (bdsm)
# Should be same as length of lists in calib, since bdsm will be run only
# prior to self-cal.
atrous = [True] * (len(niter))

clearcal(vis=vis)


if any(len(cal) > len(niter) for cal in [solint, calmode, chan_by_chan]):
    errmsg = 'The length of the solint, calmode and chan_by_chan arrays ' \
             'should be one less than the length of the niter array. ' \
             'Please fix the script before running again.'
    raise ValueError(errmsg)

if any(len(cal) != len(solint) for cal in [solint, calmode, chan_by_chan]):
    errmsg = "The length of the solint, calmode and chan_by_chan arrays "\
            "must be identical. Please fix the script and run again."
    raise ValueError(errmsg)

if not os.path.exists(vis):
    raise FileNotFoundError("MS %s not found" % (vis))

for ll in range(nloop):
    if ll == 0:
        imagename = vis.replace('.ms', '') + '_im_%d_0' % (ll + restart_no)
        regionfile = ''
    else:
        imagename = vis.replace('.ms', '') + '_im_%d' % (ll + restart_no)

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize, cell=cell, stokes='I', gridder=gridder,
            wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=True,
            weighting='briggs', robust = robust, niter=niter[ll], multiscale=multiscale[ll],
            threshold=threshold[ll], nterms=nterms[ll],
            savemodel='modelcolumn', pblimit=-1, mask=regionfile)

    if atrous[ll]:
        atrous_str = '--atrous-do'
    else:
        atrous_str = ''

    if nterms[ll] > 1:
        bdsmname = imagename + ".image.tt0"
    else:
        bdsmname = imagename + ".image"

    regionfile = imagename + ".casabox"
    os.system('/usr/bin/python bdsm_model.py {} {} --thresh-isl 20 '
        '--thresh-pix 10 {} --clobber --adaptive-rms-box '
        '--rms-map'.format(bdsmname, regionfile, atrous_str))

    # First round - Use pyBDSF regions to re-clean the image
    if (ll==0):
        #rmtables(imagename + '*')
        imagename = vis.replace('.ms', '') + '_im_%d' % (ll + restart_no)
        tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
                imsize=imsize, cell=cell, stokes='I', gridder=gridder,
                wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=True,
                weighting='briggs', robust = robust, niter=niter[ll],
                threshold=threshold[ll], multiscale=multiscale[ll],
                nterms=nterms[ll], savemodel='modelcolumn', pblimit=-1,
                mask=regionfile)

        if nterms[ll] > 1:
            bdsmname = imagename + ".image.tt0"
        else:
            bdsmname = imagename + ".image"
        regionfile = imagename + ".casabox"
        os.system('/usr/bin/python bdsm_model.py {} {} --thresh-isl 20 '
            '--thresh-pix 10 {} --clobber --adaptive-rms-box '
            '--rms-map'.format(bdsmname, regionfile, atrous_str))


    if ll < nloop-1:
        caltable = vis.replace('.ms', '') + '.gcal%d' % (ll + restart_no)

        if chan_by_chan[ll]:
            bandpass(vis=vis, caltable=caltable, selectdata=False, solint=solint[ll],
                    combine='scan', fillgaps=5, bandtype='B', parang=True)
        else:
            gaincal(vis=vis, caltable=caltable, selectdata=False, solint=solint[ll],
                    calmode=calmode[ll], append=False, parang=True)

        applycal(vis=vis, selectdata=False, gaintable=caltable, parang=True)

        if ll > 0:
            flagdata(vis=vis, mode='rflag', datacolumn='RESIDUAL', field='', timecutoff=5.0,
                    freqcutoff=5.0, timefit='line', freqfit='line', flagdimension='freqtime',
                    extendflags=False, timedevscale=3.0, freqdevscale=3.0, spectralmax=500,
                    extendpols=False, growaround=False, flagneartime=False, flagnearfreq=False,
                    action='apply', flagbackup=True, overwrite=True, writeflags=True)
