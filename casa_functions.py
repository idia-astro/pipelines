#!/usr/bin/python2.7
import os

def get_field(MS,intent):

    """Return field ID for input MS that has input indent. For targets, this is returned as a numpy array,
    but for calibrators, the first ID is taken only (for now).

    Arguments:
    ----------
    MS : str
        Input measurement set (relative or absolute path).
    intent : str
        Source intent.

    Returns:
    --------
    field : int or class `np.ndarray`
        Field ID(s) with input intent."""

    msmd.open(MS)
    field = msmd.fieldsforintent(intent)
    msmd.close()

    if field.size > 0:
        if intent == 'TARGET':
            return field
        else:
            return field[0]
    else:
        return -1

def split(MS, MMS, subms, column='data'):

    partition(vis=MS,outputvis=MMS,createmms=True,numsubms=subms,datacolumn=column)

def flag(MMS,intent='TARGET',mode='tfcrop',corr='xx,yy'):

    field = str(get_field(MMS,intent))
    flagdata(vis=MMS,correlation=corr,mode=mode,field=field)

def plot(MMS,intent='TARGET',x='freq',y='amp',column='data',fname='plotms.png',corr='',color='ant1'):

    field = str(get_field(MMS,intent))
    plotms(vis=MMS,field=field,xaxis=x,yaxis=y,correlation=corr,ydatacolumn=column,plotfile=fname,coloraxis=color,showgui=False)

def boot(MMS,intent='CALIBRATE_BANDPASS'):

    field = str(get_field(MMS,intent))
    setjy(vis=MMS,field=field,standard='Perley-Butler 2010')

def bpass(MMS,intent='CALIBRATE_BANDPASS',solint='inf',caltable='bandpass'):

    field = str(get_field(MMS,intent))
    bandpass(vis=MMS,field=field,solint=solint,caltable=caltable)

