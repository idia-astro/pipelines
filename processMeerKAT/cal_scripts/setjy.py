#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import os, sys, shutil

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va
import numpy as np
import logging
from time import gmtime
logging.Formatter.converter = gmtime

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def linfit(xInput, xDataList, yDataList):
    """

    """
    y_predict = np.poly1d(np.polyfit(xDataList, yDataList, 1))
    yPredict = y_predict(xInput)
    return yPredict


def do_setjy(visname, spw, fields, standard):
    fluxlist = ["J0408-6545", "0408-6545", ""]

    msmd.open(visname)
    fnames = msmd.namesforfields([int(ff) for ff in fields.fluxfield.split(",")])

    do_manual = False
    for ff in fluxlist:
        if ff in fnames:
            setjyname = ff
            do_manual = True
            break
        else:
            setjyname = fields.fluxfield.split(",")[0]

    if do_manual:
        smodel = [17.066, 0.0, 0.0, 0.0]
        spix = [-1.179]
        reffreq = "1284MHz"

        logger.info("Using manual flux density scale - ")
        logger.info("Flux model: %s ", smodel)
        logger.info("Spix: %s", spix)
        logger.info("Ref freq %s", reffreq)

        setjy(vis=visname,field=setjyname,scalebychan=True,standard="manual",fluxdensity=smodel,spix=spix,reffreq=reffreq,ismms=True)
    else:
        setjy(vis=visname, field=setjyname, spw=spw, scalebychan=True, standard=standard,ismms=True)

    fieldnames = msmd.fieldnames()


    # Check if 3C286 exists in the data
    is3C286 = False
    try:
        calibrator_3C286 = list(set(["3C286", "1328+307", "1331+305", "J1331+3030"]).intersection(set(fieldnames)))[0]
    except IndexError:
        calibrator_3C286 = []

    if len(calibrator_3C286):
        is3C286 = True
        id3C286 = str(msmd.fieldsforname(calibrator_3C286)[0])

    if is3C286:
        logger.info("Detected calibrator name(s):  %s" % calibrator_3C286)
        logger.info("Flux and spectral index taken/calculated from:  https://science.nrao.edu/facilities/vla/docs/manuals/oss/performance/fdscale")
        logger.info("Estimating polarization index and position angle of polarized emission from linear fit based on: Perley & Butler 2013 (https://ui.adsabs.harvard.edu/abs/2013ApJS..204...19P/abstract)")
        # central freq of spw
        spwMeanFreq = msmd.meanfreq(0, unit='GHz')
        freqList = np.array([1.05, 1.45, 1.64, 1.95])
        # fractional linear polarisation
        fracPolList = [0.086, 0.095, 0.099, 0.101]
        polindex = linfit(spwMeanFreq, freqList, fracPolList)
        logger.info("Predicted polindex at frequency %s: %s", spwMeanFreq, polindex)
        # position angle of polarized intensity
        polPositionAngleList = [33, 33, 33, 33]
        polangle = linfit(spwMeanFreq, freqList, polPositionAngleList)
        logger.info("Predicted pol angle at frequency %s: %s", spwMeanFreq, polangle)

        reffreq = "1.45GHz"
        logger.info("Ref freq %s", reffreq)
        setjy(vis=visname,
            field=id3C286,
            scalebychan=True,
            standard="manual",
            fluxdensity=[-14.6, 0.0, 0.0, 0.0],
            #spix=-0.52, # between 1465MHz and 1565MHz
            reffreq=reffreq,
            polindex=[polindex],
            polangle=[polangle],
            rotmeas=0,ismms=True)


    # Check if 3C138 exists in the data
    is3C138 = False
    try:
        calibrator_3C138 = list(set(["3C138", "0518+165", "0521+166", "J0521+1638"]).intersection(set(fieldnames)))[0]
    except IndexError:
        calibrator_3C138 = []

    if len(calibrator_3C138):
        is3C138 = True
        id3C138 = str(msmd.fieldsforname(calibrator_3C138)[0])

    if is3C138:
        logger.info("Detected calibrator name(s):  %s" % calibrator_3C138)
        logger.info("Flux and spectral index taken/calculated from:  https://science.nrao.edu/facilities/vla/docs/manuals/oss/performance/fdscale")
        logger.info("Estimating polarization index and position angle of polarized emission from linear fit based on: Perley & Butler 2013 (https://ui.adsabs.harvard.edu/abs/2013ApJS..204...19P/abstract)")
        # central freq of spw
        spwMeanFreq = msmd.meanfreq(0, unit='GHz')
        freqList = np.array([1.05, 1.45, 1.64, 1.95])
        # fractional linear polarisation
        fracPolList = [0.056, 0.075, 0.084, 0.09]
        polindex = linfit(spwMeanFreq, freqList, fracPolList)
        logger.info("Predicted polindex at frequency %s: %s", spwMeanFreq, polindex)
        # position angle of polarized intensity
        polPositionAngleList = [-14, -11, -10, -10]
        polangle = linfit(spwMeanFreq, freqList, polPositionAngleList)
        logger.info("Predicted pol angle at frequency %s: %s", spwMeanFreq, polangle)

        reffreq = "1.45GHz"
        logger.info("Ref freq %s", reffreq)
        setjy(vis=visname,
            field=id3C138,
            scalebychan=True,
            standard="manual",
            fluxdensity=[-8.26, 0.0, 0.0, 0.0],
            #spix=-0.57,  # between 1465MHz and 1565MHz
            reffreq=reffreq,
            polindex=[polindex],
            polangle=[polangle],
            rotmeas=0,ismms=True)

    msmd.done()


def main(args,taskvals):

    visname = va(taskvals, "data", "vis", str)

    if os.path.exists(os.path.join(os.getcwd(), "caltables")):
        shutil.rmtree(os.path.join(os.getcwd(), "caltables"))

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals["fields"])

    spw = va(taskvals, "crosscal", "spw", str, default="")
    standard = va(taskvals, "crosscal", "standard", str, default="Stevens-Reynolds 2016")

    do_setjy(visname, spw, fields, standard)

if __name__ == '__main__':

    bookkeeping.run_script(main)
