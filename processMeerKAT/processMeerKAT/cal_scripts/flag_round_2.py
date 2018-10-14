
import sys

sys.path.append('/data/users/krishna/pipeline/processMeerKAT/processMeerKAT')

import config_parser
from cal_scripts import bookkeeping

def do_pre_flag_2(visname, spw, fields):
    clipfluxcal   = [0., 50.]
    clipphasecal  = [0., 50.]
    cliptarget    = [0., 20.]

    flagdata(vis=visname, mode="clip", spw = spw, field=fields.fluxfield,
            clipminmax=clipfluxcal, datacolumn="corrected", clipoutside=True,
            clipzeros=True, extendpols=False, action="apply", flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw = spw,
            field=fields.secondaryfield, clipminmax=clipphasecal,
            datacolumn="corrected", clipoutside=True, clipzeros=True,
            extendpols=False, action="apply", flagbackup=True, savepars=False,
            overwrite=True, writeflags=True)

    # After clip, now flag using 'tfcrop' option for flux and phase cal tight
    # flagging
    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected",
            field=fields.gainfields, ntime="scan", timecutoff=6.0,
            freqcutoff=5.0, timefit="line", freqfit="line",
            flagdimension="freqtime", extendflags=False, timedevscale=5.0,
            freqdevscale=5.0, extendpols=False, growaround=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # now flag using 'rflag' option  for flux and phase cal tight flagging
    flagdata(vis=visname, mode="rflag", datacolumn="corrected",
            field=fields.gainfields, timecutoff=5.0, freqcutoff=5.0,
            timefit="poly", freqfit="line", flagdimension="freqtime",
            extendflags=False, timedevscale=4.0, freqdevscale=4.0,
            spectralmax=500.0, extendpols=False, growaround=False,
            flagneartime=False, flagnearfreq=False, action="apply",
            flagbackup=True, overwrite=True, writeflags=True)

    ## Now extend the flags (70% more means full flag, change if required)
    flagdata(vis=visname, mode="extend", spw = spw, field=fields.gainfields,
            datacolumn="corrected", clipzeros=True, ntime="scan",
            extendflags=False, extendpols=False, growtime=90.0, growfreq=90.0,
            growaround=False, flagneartime=False, flagnearfreq=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # Now flag for target - moderate flagging, more flagging in self-cal cycles
    flagdata(vis=visname, mode="clip", spw = spw, field=fields.targetfield,
            clipminmax=cliptarget, datacolumn="corrected", clipoutside=True,
            clipzeros=True, extendpols=False, action="apply", flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected",
            field=fields.targetfield, ntime="scan", timecutoff=6.0, freqcutoff=5.0,
            timefit="poly", freqfit="line", flagdimension="freqtime",
            extendflags=False, timedevscale=5.0, freqdevscale=5.0,
            extendpols=False, growaround=False, action="apply", flagbackup=True,
            overwrite=True, writeflags=True)

    # now flag using 'rflag' option
    flagdata(vis=visname, mode="rflag", datacolumn="corrected",
            field=fields.targetfield, timecutoff=5.0, freqcutoff=5.0, timefit="poly",
            freqfit="poly", flagdimension="freqtime", extendflags=False,
            timedevscale=5.0, freqdevscale=5.0, spectralmax=500.0,
            extendpols=False, growaround=False, flagneartime=False,
            flagnearfreq=False, action="apply", flagbackup=True, overwrite=True,
            writeflags=True)

    # Now summary
    flagdata(vis=visname, mode="summary", datacolumn="corrected",
            extendflags=True, name=visname + 'summary.split', action="apply",
            flagbackup=True, overwrite=True, writeflags=True)


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

    do_pre_flag_2(visname, spw, fields)
