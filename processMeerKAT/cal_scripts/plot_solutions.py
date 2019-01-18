import os
import config_parser
from config_parser import validate_args as va
from cal_scripts import bookkeeping
PLOT_DIR = 'plots'

def main():

    config = config_parser.parse_args()['config']

    # Parse config file
    taskvals, config = config_parser.parse_config(config)

    visname = va(taskvals, 'data', 'vis', str)
    visname = os.path.split(visname.replace('.ms', '.mms'))[1]

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    if not os.path.exists(PLOT_DIR):
        os.mkdir(PLOT_DIR)

    #Plot solutions for bandpass calibrator
    plotms(vis=calfiles.bpassfile, xaxis='Real', yaxis='Imag', coloraxis='antenna1', plotfile='{0}/bpass_real_imag.png'.format(PLOT_DIR),showgui=False)
    plotms(vis=calfiles.bpassfile, xaxis='chan', yaxis='Amp', coloraxis='antenna1', plotfile='{0}/bpass_chan_amp.png'.format(PLOT_DIR),showgui=False)
    plotms(vis=calfiles.bpassfile, xaxis='chan', yaxis='Phase', coloraxis='antenna1', plotfile='{0}/bpass_chan_phase.png'.format(PLOT_DIR),showgui=False)

    #Plot solutions for phase calibrator
    plotms(vis=calfiles.gainfile, xaxis='Time', yaxis='Amp', coloraxis='antenna1', plotfile='{0}/phasecal_time_amp.png'.format(PLOT_DIR),showgui=False)
    plotms(vis=calfiles.gainfile, xaxis='Time', yaxis='Phase', coloraxis='antenna1', plotfile='{0}/phasecal_time_phase.png'.format(PLOT_DIR),showgui=False)

if __name__ == "__main__":
    main()
