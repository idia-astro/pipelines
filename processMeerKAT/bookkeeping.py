#Copyright (C) 2022 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

#!/usr/bin/env python3

import sys
import traceback

import config_parser
from collections import namedtuple
import os
import glob
import re

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def get_calfiles(visname, caldir):
        base = os.path.splitext(visname)[0]
        kcorrfile = os.path.join(caldir,base + '.kcal')
        bpassfile = os.path.join(caldir,base + '.bcal')
        gainfile =  os.path.join(caldir,base + '.gcal')
        dpolfile =  os.path.join(caldir,base + '.pcal')
        xpolfile =  os.path.join(caldir,base + '.xcal')
        xdelfile =  os.path.join(caldir,base + '.xdel')
        fluxfile =  os.path.join(caldir,base + '.fluxscale')

        calfiles = namedtuple('calfiles',
                ['kcorrfile', 'bpassfile', 'gainfile', 'dpolfile', 'xpolfile',
                    'xdelfile', 'fluxfile'])
        return calfiles(kcorrfile, bpassfile, gainfile, dpolfile, xpolfile,
                xdelfile, fluxfile)


def bookkeeping(visname):
    # Book keeping
    caldir = os.path.join(os.getcwd(), 'caltables')
    calfiles = get_calfiles(visname, caldir)

    return calfiles, caldir

def get_field_ids(fields):
    """
    Given an input list of source names, finds the associated field
    IDS from the MS and returns them as a list.
    """

    targetfield    = fields['targetfields']
    extrafields    = fields['extrafields']
    fluxfield      = fields['fluxfield']
    bpassfield     = fields['bpassfield']
    secondaryfield = fields['phasecalfield']
    kcorrfield     = fields['phasecalfield']
    xdelfield      = fields['phasecalfield']
    dpolfield      = fields['phasecalfield']
    xpolfield      = fields['phasecalfield']

    if fluxfield != secondaryfield:
        gainfields = \
                str(fluxfield) + ',' + str(secondaryfield)
    else:
        gainfields = str(fluxfield)

    FieldIDs = namedtuple('FieldIDs', ['targetfield', 'fluxfield',
                    'bpassfield', 'secondaryfield', 'kcorrfield', 'xdelfield',
                    'dpolfield', 'xpolfield', 'gainfields', 'extrafields'])

    return FieldIDs(targetfield, fluxfield, bpassfield, secondaryfield,
            kcorrfield, xdelfield, dpolfield, xpolfield, gainfields, extrafields)

def polfield_name(visname):

    from casatools import msmetadata
    msmd = msmetadata()
    msmd.open(visname)
    fieldnames = msmd.fieldnames()
    msmd.done()

    polfield = ''
    if any([ff in ["3C286", "1328+307", "1331+305", "J1331+3030"] for ff in fieldnames]):
        polfield= list(set(["3C286", "1328+307", "1331+305", "J1331+3030"]).intersection(set(fieldnames)))[0]
    elif any([ff in ["3C138", "0518+165", "0521+166", "J0521+1638"] for ff in fieldnames]):
        polfield = list(set(["3C138", "0518+165", "0521+166", "J0521+1638"]).intersection(set(fieldnames)))[0]
    elif any([ff in ["3C48", "0134+329", "0137+331", "J0137+3309"] for ff in fieldnames]):
        polfield = list(set(["3C48", "0134+329", "0137+331", "J0137+3309"]).intersection(set(fieldnames)))[0]
    elif "J1130-1449" in fieldnames:
        polfield = "J1130-1449"
    else:
        logger.warning("No valid polarization field found. Defaulting to use the phase calibrator to solve for XY phase.")
        logger.warning("The polarization solutions found will likely be wrong. Please check the results carefully.")

    return polfield

def check_file(filepath):

    # Python2 only has IOError, so define FileNotFound
    try:
        FileNotFoundError
    except NameError:
        FileNotFoundError = IOError

    if not os.path.exists(filepath):
        logger.error('Calibration table "{0}" was not written. Please check the CASA output and whether a solution was found.'.format(filepath))
        raise FileNotFoundError
    else:
        logger.info('Calibration table "{0}" successfully written.'.format(filepath))

def get_selfcal_params():

    #Flag for input errors
    exit = False

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['selfcal']
    other_params = list(params.keys())

    params['vis'] = taskvals['data']['vis']
    params['refant'] = taskvals['crosscal']['refant']
    params['dopol'] = taskvals['run']['dopol']

    if params['dopol'] and 'G' in params['gaintype']:
        logger.warning("dopol is True, but gaintype includes 'G'. Use gaintype='T' for polarisation on linear feeds (e.g. MeerKAT).")

    single_args = ['nloops','loop','discard_nloops','outlier_threshold','outlier_radius'] #need to be 1 long (i.e. not a list)
    gaincal_args = ['solint','calmode','gaintype','flag'] #need to be nloops long
    list_args = ['imsize'] #allowed to be lists of lists

    for arg in single_args:
        if arg in other_params:
            other_params.pop(other_params.index(arg))

    for arg in single_args:
        if type(params[arg]) is list or type(params[arg]) is str and ',' in params[arg]:
            logger.error("Parameter '{0}' in '{1}' cannot be a list. It must be a single value.".format(arg,args['config']))
            exit = True

    for arg in other_params:
        if type(params[arg]) is str and ',' in params[arg]:
            logger.error("Parameter '{0}' in '{1}' cannot use comma-seprated values. It must be a list or values, or a single value.".format(arg,args['config']))
            exit = True

        # These can be a list of lists or a simple list (if specifying a single value).
        # So make sure these two cases are covered.
        if arg in list_args:
            # Not a list of lists, so turn it into one of right length
            if type(params[arg]) is list and (len(params[arg]) == 0 or type(params[arg][0]) is not list):
                params[arg] = [params[arg],] * (params['nloops'] + 1)
            # Not a list at all, so put it into a list
            elif type(params[arg]) is not list:
                params[arg] = [[params[arg],],] * (params['nloops'] + 1)
            # A list of lists of length 1, so put into list of lists of right length
            elif type(params[arg]) is list and type(params[arg][0]) is list and len(params[arg]) == 1:
                params[arg] = [params[arg][0],] * (params['nloops'] + 1)

        elif type(params[arg]) is not list:
            if arg in gaincal_args:
                params[arg] = [params[arg]] * (params['nloops'])
            else:
                params[arg] = [params[arg]] * (params['nloops'] + 1)

    for arg in other_params:
        #By this point params[arg] will be a list
        if arg in gaincal_args and len(params[arg]) != params['nloops']:
            logger.error("Parameter '{0}' in '{1}' is the wrong length. It is {2} long but must be 'nloops' ({3}) long or a single value (not a list).".format(arg,args['config'],len(params[arg]),params['nloops']))
            exit = True

        elif arg not in gaincal_args and len(params[arg]) != params['nloops'] + 1:
            logger.error("Parameter '{0}' in '{1}' is the wrong length. It is {2} long but must be 'nloops' + 1 ({3}) long or a single value (not a list).".format(arg,args['config'],len(params[arg]),params['nloops']+1))
            exit = True

    if exit:
        sys.exit(1)

    return args,params

def get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,outlier_radius,threshold,step):

    from casatools import msmetadata,quanta
    from read_ms import check_spw
    msmd = msmetadata()
    qa = quanta()

    if ',' in vis:                 #detect if we are combining tracks
        vis = vis.split(',')[0]
        combTracks = True
    else:
        combTracks = False
    
    if os.path.exists('{0}/SUBMSS'.format(vis)):
        tmpvis = glob.glob('{0}/SUBMSS/*'.format(vis))[0]
    else:
        tmpvis = vis

    msmd.open(tmpvis)

    visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    visbase = re.sub('\.\d+\.*\d*\~\d+\.*\d*[a-z,A-Z]?[Hz,hz,hZ,HZ]*\.','.',visbase) # Strip any SPWs from basename (when running outlier imaging separately per SPW)
    targetfields = config_parser.get_key(config_parser.parse_args()['config'], 'fields', 'targetfields')

    #Force taking first target field (relevant for writing outliers.txt at beginning of pipeline)
    if type(targetfields) is str and ',' in targetfields:
        targetfield = targetfields.split(',')[0]
        msg = 'Multiple target fields input ("{0}"), but only one position can be used to identify outliers (for outlier imaging). Using "{1}".'
        logger.warning(msg.format(targetfields,targetfield))
    else:
        targetfield = targetfields
    #Make sure it's an integer
    try:
        targetfield = int(targetfield)
    except ValueError: # It's not an int, but a str
        targetfield = msmd.fieldsforname(targetfield)[0]

    target_str = msmd.namesforfields(targetfield)[0]

    if combTracks:                           #currently setting visbase to the CWD in the case of combining tracks
        visbase = os.getcwd().split('/')[-1]

    if '.ms' in visbase and target_str not in visbase:
        basename = visbase.replace('.ms','.{0}'.format(target_str))
    else:
        basename = visbase.replace('.mms', '')

    imbase = basename + '_im_%d' # Images will be produced in $CWD
    imagename = imbase % loop
    outimage = imagename + '.image'
    pixmask = imagename + ".pixmask"
    maskfile = imagename + ".islmask"
    rmsfile = imagename + ".rms"
    caltable = basename + '.gcal%d' % loop
    prev_caltables = sorted(glob.glob('*.gcal?'))
    cfcache = basename + '.cf'
    thresh = 10

    if deconvolver[loop] == 'mtmfs':
        outimage += '.tt0'

    if step not in ['tclean','sky'] and not os.path.exists(outimage):
        logger.error("Image '{0}' doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(outimage,loop))
        sys.exit(1)

    if step in ['tclean','predict']:
        pixmask = imbase % (loop-1) + '.pixmask'
        rmsfile = imbase % (loop-1) + '.rms'
    if step in ['tclean','predict','sky'] and ((loop == 0 and not os.path.exists(pixmask)) or (0 < loop < nloops and calmode[loop] == '')):
        pixmask = ''

    #Check no missing caltables
    for i in range(0,loop):
        if calmode[i] != '' and not os.path.exists(basename + '.gcal%d' % i):
            logger.error("Calibration table '{0}' doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(basename + '.gcal%d' % i,i))
            sys.exit(1)
    for i in range(discard_nloops):
        prev_caltables.pop(0)

    if outlier_threshold != '' and outlier_threshold != 0: # and (loop > 0 or step in ['sky','bdsf'] and loop == 0):
        if step in ['tclean','predict','sky']:
            outlierfile = 'outliers_loop{0}.txt'.format(loop)
        else:
            outlierfile = 'outliers_loop{0}.txt'.format(loop+1)

        #Derive sky model radius for outliers, assuming channel 0 (of SPW 0) is lowest frequency and therefore largest FWHM
        if outlier_radius == 0.0 or outlier_radius == '' and step == 'sky':
            SPW = check_spw(config_parser.parse_args()['config'],msmd)
            low_freq = float(SPW.replace('*:','').split('~')[0]) * 1e6 #MHz to Hz
            rads=1.025*qa.constants(v='c')['value']/low_freq/ msmd.antennadiameter()['0']['value']
            FWHM=qa.convert(qa.quantity(rads,'rad'),'deg')['value']
            sky_model_radius = 1.5*FWHM #degrees
            logger.warning('Using calculated search radius of {0:.1f} degrees.'.format(sky_model_radius))
        else:
            if step == 'sky':
                logger.info('Using preset search radius of {0} degrees'.format(outlier_radius))
            sky_model_radius = outlier_radius
    else:
        outlierfile = ''
        sky_model_radius = 0.0

    msmd.done()

    if not (type(threshold[loop]) is str and 'Jy' in threshold[loop]) and threshold[loop] > 1:
        if step in ['tclean','predict']:
            if os.path.exists(rmsfile):
                from casatasks import imstat
                stats = imstat(imagename=rmsfile)
                threshold[loop] *= stats['min'][0]
            else:
                logger.error("'{0}' doesn't exist. Can't do thresholding at S/N > {1}. Loop 0 must use an absolute threshold value. Check the logs to see why RMS map not created.".format(rmsfile,threshold[loop]))
                sys.exit(1)
        elif step == 'bdsf':
            thresh = threshold[loop]

    return imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,thresh,maskfile,targetfield,sky_model_radius

def rename_logs(logfile=''):

    if logfile != '' and os.path.exists(logfile):
        if 'SLURM_ARRAY_JOB_ID' in os.environ:
            IDs = '{SLURM_JOB_NAME}-{SLURM_ARRAY_JOB_ID}_{SLURM_ARRAY_TASK_ID}'.format(**os.environ)
        else:
            IDs = '{SLURM_JOB_NAME}-{SLURM_JOB_ID}'.format(**os.environ)

        os.rename(logfile,'logs/{0}.mpi'.format(IDs))
        for log in glob.glob('*.last'):
            os.rename(log,'logs/{0}-{1}.last'.format(os.path.splitext(log)[0],IDs))

def get_imaging_params():

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['image']
    params['vis'] = taskvals['data']['vis']
    params['keepmms'] = taskvals['crosscal']['keepmms']
    params['spw'] = taskvals['crosscal']['spw']
    params.pop('fitspw')
    params.pop('fitorder')

    #Rename the masks that were already used
    if params['outlierfile'] != '' and os.path.exists(params['outlierfile']):
        outliers=open(params['outlierfile']).read()
        outlier_bases = re.findall(r'imagename=(.*)\n',outliers)
        for name in outlier_bases:
            mask = '{0}.mask'.format(name)
            if os.path.exists(mask):
                newname = '{0}.old'.format(mask)
                logger.info('Re-using old mask for "{0}". Renaming "{1}" to "{2}" to avoid mask conflict.'.format(name,mask,newname))
                os.rename(mask,newname)

    return args,params

def run_script(func,logfile=''):

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    continue_run = config_parser.validate_args(taskvals, 'run', 'continue', bool, default=True)
    spw = config_parser.validate_args(taskvals, 'crosscal', 'spw', str)
    nspw = config_parser.validate_args(taskvals, 'crosscal', 'nspw', int)

    if continue_run:
        try:
            func(args,taskvals)
            rename_logs(logfile)
        except Exception as err:
            logger.error('Exception found in the pipeline of type {0}: {1}'.format(type(err),err))
            logger.error(traceback.format_exc())
            config_parser.overwrite_config(args['config'], conf_dict={'continue' : False}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
            if nspw > 1:
                for SPW in spw.split(','):
                    spw_config = '{0}/{1}'.format(SPW.replace('*:',''),args['config'])
                    config_parser.overwrite_config(spw_config, conf_dict={'continue' : False}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
            rename_logs(logfile)
            sys.exit(1)
    else:
        logger.error('Exception found in previous pipeline job, which set "continue=False" in [run] section of "{0}". Skipping "{1}".'.format(args['config'],os.path.split(sys.argv[2])[1]))
        #os.system('./killJobs.sh') # and cancelling remaining jobs (scancel not found since /opt overwritten)
        rename_logs(logfile)
        sys.exit(1)
