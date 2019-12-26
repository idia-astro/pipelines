#Copyright (C) 2019 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

from __future__ import print_function

import os, sys, shutil

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va
import numpy as np
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def get_y_value_from_linear_fit(xInput, xDataList, yDataList):
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

        setjy(vis=visname,field=setjyname,scalebychan=True,standard="manual",fluxdensity=smodel,spix=spix,reffreq=reffreq)
    else:
        setjy(vis=visname, field=setjyname, spw=spw, scalebychan=True, standard=standard)

    fieldnames = msmd.fieldnames()


    # Check if 3C286 exists in the data
    is3C286 = True
    try:
        calibrator_3C286 = list(set(["3C286", "1328+307", "1331+305", "J1331+3030"]).intersection(set(fieldnames)))
    except IndexError:
        is3C286 = False

    if is3C286:
        logger.info("Detected calibrator name(s):  %s", ", ".join(calibrator_3C286))
        logger.info("Flux and spectral index taken/calculated from:  https://science.nrao.edu/facilities/vla/docs/manuals/oss/performance/fdscale")
        logger.info("Estimating polarization index and position angle of polarized emission from linear fit based on: Perley & Butler 2013 (https://ui.adsabs.harvard.edu/abs/2013ApJS..204...19P/abstract)")
        # central freq of spw
        spwMeanFreq = msmd.meanfreq(0)
        freqList = np.array([1.05, 1.45, 1.64, 1.95]) * 1e9
        # fractional linear polarisation
        fracPolList = [0.086, 0.095, 0.099, 0.101]
        polindex = get_y_value_from_linear_fit(spwMeanFreq, freqList, fracPolList)
        logger.info("Predicted polindex at frequecny %s: %s", spwMeanFreq, polindex)
        # position angle of polarized intensity
        polPositionAngleList = [33, 33, 33, 33]
        polangle = get_y_value_from_linear_fit(spwMeanFreq, freqList, polPositionAngleList)
        logger.info("Predicted pol angle at frequecny %s: %s", spwMeanFreq, polangle)

        reffreq = "1.45GHz"
        logger.info("Ref freq %s", reffreq)
        setjy(vis=visname,
            field=setjyname,
            scalebychan=True,
            standard="manual",
            fluxdensity=[14.6, 0.0, 0.0, 0.0],
            spix=-0.52, # between 1465MHz and 1565MHz
            reffreq=reffreq,
            polindex=[polindex],
            polangle=[polangle],
            rotmeas=7.0)


    # Check if 3C138 exists in the data
    is3C138 = True
    try:
        calibrator_3C138 = set(["3C138", "0518+165", "0521+166", "J0521+1638"]).intersection(set(fieldnames))
    except IndexError:
        is3C138 = False

    if is3C138:
        logger.info("Detected calibrator name(s):  %s", ", ".join(calibrator_3C138))
        logger.info("Flux and spectral index taken/calculated from:  https://science.nrao.edu/facilities/vla/docs/manuals/oss/performance/fdscale")
        logger.info("Estimating polarization index and position angle of polarized emission from linear fit based on: Perley & Butler 2013 (https://ui.adsabs.harvard.edu/abs/2013ApJS..204...19P/abstract)")
        # central freq of spw
        spwMeanFreq = msmd.meanfreq(0)
        freqList = np.array([1.05, 1.45, 1.64, 1.95]) * 1e9
        # fractional linear polarisation
        fracPolList = [0.056, 0.075, 0.084, 0.09]
        polindex = get_y_value_from_linear_fit(spwMeanFreq, freqList, fracPolList)
        logger.info("Predicted polindex at frequecny %s: %s", spwMeanFreq, polindex)
        # position angle of polarized intensity
        polPositionAngleList = [-14, -11, -10, -10]
        polangle = get_y_value_from_linear_fit(
            spwMeanFreq, freqList, polPositionAngleList
        )
        logger.info("Predicted pol angle at frequecny %s: %s", spwMeanFreq, polangle)

        reffreq = "1.45GHz"
        logger.info("Ref freq %s", reffreq)
        setjy(vis=visname,
            field=setjyname,
            scalebychan=True,
            standard="manual",
            fluxdensity=[8.26, 0.0, 0.0, 0.0],
            spix=-0.57,  # between 1465MHz and 1565MHz
            reffreq=reffreq,
            polindex=[polindex],
            polangle=[polangle],
            rotmeas=7.0)

    msmd.done()


if __name__ == "__main__":
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args["config"])

    visname = va(taskvals, "data", "vis", str)

    if os.path.exists(os.path.join(os.getcwd(), "caltables")):
        shutil.rmtree(os.path.join(os.getcwd(), "caltables"))

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals["fields"])

    spw = va(taskvals, "crosscal", "spw", str, default="")
    standard = va(taskvals, "crosscal", "standard", str, default="Stevens-Reynolds 2016")

    do_setjy(visname, spw, fields, standard)
