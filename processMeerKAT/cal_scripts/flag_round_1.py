import sys

sys.path.append('/data/users/krishna/pipeline/processMeerKAT/processMeerKAT')

import config_parser
from cal_scripts import bookkeeping

def do_pre_flag(visname, spw, fields):
    clipfluxcal   = [0., 50.]
    clipphasecal  = [0., 50.]
    cliptarget    = [0., 20.]

    flagdata(vis=visname, mode='manual', autocorr=True, action='apply',
            flagbackup=True, savepars=False, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=spw, field=fields.fluxfield,
            clipminmax=clipfluxcal, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=spw, field=fields.secondaryfield,
            clipminmax=clipphasecal, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='tfcrop', field=fields.gainfields, spw=spw,
            ntime='scan', timecutoff=5.0, freqcutoff=5.0, timefit='line',
            freqfit='line', extendflags=False, timedevscale=5., freqdevscale=5.,
            extendpols=True, growaround=False, action='apply', flagbackup=True,
            overwrite=True, writeflags=True, datacolumn='DATA')

    # Conservatively extend flags
    flagdata(vis=visname, mode='extend', spw=spw, field=fields.gainfields,
            datacolumn='data', clipzeros=True, ntime='scan', extendflags=False,
            extendpols=True, growtime=80., growfreq=80., growaround=False,
            flagneartime=False, flagnearfreq=False, action='apply',
            flagbackup=True, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=spw, field=fields.targetfield,
            clipminmax=cliptarget, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='tfcrop', field=fields.targetfield, spw=spw,
            ntime='scan', timecutoff=6.0, freqcutoff=6.0, timefit='poly',
            freqfit='poly', extendflags=False, timedevscale=5., freqdevscale=5.,
            extendpols=True, growaround=False, action='apply', flagbackup=True,
            overwrite=True, writeflags=True, datacolumn='DATA')

    flagdata(vis=visname, mode='extend', spw=spw, field=fields.targetfield,
            datacolumn='data', clipzeros=True, ntime='scan', extendflags=False,
            extendpols=True, growtime=80., growfreq=80., growaround=False,
            flagneartime=False, flagnearfreq=False, action='apply',
            flagbackup=True, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='summary', datacolumn='DATA',
            name=visname+'.flag.summary')



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

    do_pre_flag(visname, spw, fields)
