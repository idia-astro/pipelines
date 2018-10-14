import sys

sys.path.append('/data/users/krishna/pipeline/processMeerKAT/processMeerKAT')
import config_parser
from cal_scripts import bookkeeping

def split_vis(visname, spw, fields, specave, timeave):
    outputbase = visname.strip('.ms')
    split(vis=visname, outputvis = outputbase+'.'+fields.targetfield+'.ms',
            datacolumn='corrected', field = fields.targetfield, spw = spw,
            keepflags=True, keepmms = True, width = specave, timebin = timeave)

    split(vis=visname, outputvis = outputbase+'.'+fields.secondaryfield+'.ms',
            datacolumn='corrected', field = fields.secondaryfield, spw = spw,
            keepflags=True, keepmms = True, width = specave, timebin = timeave)

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

    specave = taskvals['crosscal'].pop('specave', 2)
    timeave = taskvals['crosscal'].pop('timeave', '8s')

    split_vis(visname, spw, fields, specave, timeave)
