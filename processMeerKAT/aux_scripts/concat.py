#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import os
import sys
import glob
from shutil import copytree

import config_parser
from config_parser import validate_args as va
import bookkeeping

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
from casatools import msmetadata,image
msmd = msmetadata()
ia = image()

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def sortbySPW(visname):
    return float(visname.split('~')[0])

def check_output(fname,files,pattern,out,job='concat',filetype='image'):

    if os.path.exists(out):
        logger.info('Output file "{0}" already exists. Skipping {1}.'.format(out,job))
        return None
    elif len(files) == 0:
        logger.warning("Didn't find any {0}s with '{1}'".format(filetype,pattern))
        return None
    elif len(files) == 1:
        logger.warning("Only found 1 {0} with '{1}'. Will copy to this directory.".format(filetype,pattern))
        copytree(files[0], out)
        return None
    return files

def get_infiles(dirs,suffix):

    any = False
    if type(dirs) is str:
        pattern = '{0}/{1}'.format(dirs,suffix)
        files = glob.glob(pattern)
    elif type(dirs) is list:
        files=[]
        for i,loc in enumerate(dirs):
            img = glob.glob('{0}/{1}'.format(loc,suffix))
            if len(img) > 0:
                if any is False: #output error msg for previous cases
                    for j in range(0,i):
                        logger.warning("Expected to find file with '{0}/{1}".format(dirs[j],suffix))
                any = True
                files.append(img[0])
            elif any:
                logger.warning("Expected to find file with '{0}/{1}".format(loc,suffix))

        pattern = '{%s}/%s' % (','.join(dirs),suffix)

    else:
        logger.error('Only type str and list is supported when searching for files to concatenate. Please check "spw" parameter in your config.')
        files = []

    return files,pattern

def do_concat(visname, fields, dirs='*MHz'):

    msmd.open(visname)

    newvis = visname
    logger.info('Beginning {0}.'.format(sys.argv[0]))
    basename, ext = os.path.splitext(visname)
    filebase = os.path.split(basename)[1]

    for field in [fields.targetfield,fields.gainfields,fields.extrafields]:
        if field != '':
            for fname in field.split(','):
                if fname.isdigit():
                    fname = msmd.namesforfields(int(fname))[0]

                #Concat tt0 images (into continuum cube)
                suffix = 'images/*{0}*image.tt0'.format(fname)
                files,pattern = get_infiles(dirs,suffix)
                out = '{0}.{1}.contcube'.format(filebase,fname)
                images = check_output(fname,files,pattern,out,job='imageconcat',filetype='image')
                if images is not None:
                    images.sort(key=sortbySPW)
                    logger.info('Creating continuum cube with following command:')
                    logger.info('ia.imageconcat(infiles={0}, outfile={1}, axis=-1, relax=True)'.format(images,out))
                    ia.imageconcat(infiles=images, outfile=out, axis=-1, relax=True)

                    if os.path.exists(out):
                        if not os.path.exists(out+'.fits'):
                            exportfits(imagename=out, fitsimage=out+'.fits')
                    else:
                        logger.error("Output image '{0}' attempted to write but was not written.".format(out))

                #Concat images (into continuum cube)
                suffix = 'images/*{0}*image'.format(fname)
                files,pattern = get_infiles(dirs,suffix)
                out = '{0}.{1}.contcube'.format(filebase,fname)
                images = check_output(fname,files,pattern,out,job='imageconcat',filetype='image')
                if images is not None:
                    images.sort(key=sortbySPW)
                    logger.info('Creating continuum cube with following command:')
                    logger.info('ia.imageconcat(infiles={0}, outfile={1}, axis=-1, relax=True)'.format(images,out))
                    ia.imageconcat(infiles=images, outfile=out, axis=-1, relax=True)

                    if os.path.exists(out):
                        if not os.path.exists(out+'.fits'):
                            exportfits(imagename=out, fitsimage=out+'.fits')
                    else:
                        logger.error("Output image '{0}' attempted to write but was not written.".format(out))

                #Concat MSs
                suffix = '*{0}*.ms'.format(fname)
                files,pattern = get_infiles(dirs,suffix)
                out = '{0}.{1}.ms'.format(filebase,fname)
                MSs = check_output(fname,files,pattern,out,job='concat',filetype='MS')
                if MSs is not None:
                    MSs.sort(key=sortbySPW)
                    logger.info('Concatenating MSs with following command:')
                    logger.info('concat(vis={0}, concatvis={1})'.format(MSs,out))
                    concat(vis=MSs, concatvis=out)
                    if fname == fields.targetfield.split(',')[0]:
                        newvis = out

                    if not os.path.exists(out):
                        logger.error("Output MS '{0}' attempted to write but was not written.".format(out))

                #Concat MMSs
                suffix = '*{0}*.mms'.format(fname)
                files,pattern = get_infiles(dirs,suffix)
                out = '{0}.{1}.mms'.format(filebase,fname)
                MMSs = check_output(fname,files,pattern,out,job='virtualconcat',filetype='MMS')
                if MMSs is not None:
                    MMSs.sort(key=sortbySPW)
                    logger.info('Concatenating MMSs with following command:')
                    logger.info('virtualconcat(vis={0}, concatvis={1})'.format(MMSs,out))
                    virtualconcat(vis=MMSs, concatvis=out)
                    if fname == fields.targetfield.split(',')[0]:
                        newvis = out

                    if not os.path.exists(out):
                        logger.error("Output MMS '{0}' attempted to write but was not written.".format(out))

    msmd.done()
    logger.info('Completed {0}.'.format(sys.argv[0]))

    return newvis

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)
    spw = va(taskvals, 'crosscal', 'spw', str, default='')
    nspw = va(taskvals, 'crosscal', 'nspw', int, default='')
    fields = bookkeeping.get_field_ids(taskvals['fields'])
    dirs = config_parser.parse_spw(args['config'])[3]

    if ',' in spw:
        newvis = do_concat(visname, fields, dirs)
        config_parser.overwrite_config(args['config'], conf_dict={'vis' : "'{0}'".format(newvis)}, conf_sec='data')
        config_parser.overwrite_config(args['config'], conf_dict={'crosscal_vis': "'{0}'".format(visname)}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
    else:
        logger.error("Only found one SPW in '{0}', so will skip concat.".format(args['config']))

if __name__ == '__main__':

    bookkeeping.run_script(main,logfile)
