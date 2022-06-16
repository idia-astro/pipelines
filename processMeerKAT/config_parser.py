#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

#!/usr/bin/env python3

import argparse
import configparser
import ast
import processMeerKAT

def parse_args():
    """
    Parse the command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-C','--config', default=processMeerKAT.CONFIG, required=False, help='Name of the input config file')

    args, __ = parser.parse_known_args()

    return vars(args)


def parse_config(filename):
    """
    Given an input config file, parses it to extract key-value pairs that
    should represent task parameters and values respectively.
    """

    config = configparser.SafeConfigParser(allow_no_value=True)
    config.read(filename)

    # Build a nested dictionary with tasknames at the top level
    # and parameter values one level down.
    taskvals = dict()
    for section in config.sections():

        if section not in taskvals:
            taskvals[section] = dict()

        for option in config.options(section):
            # Evaluate to the right type()
            try:
                taskvals[section][option] = ast.literal_eval(config.get(section, option))
            except (ValueError,SyntaxError):
                err = "Cannot format field '{0}' in config file '{1}'".format(option,filename)
                err += ", which is currently set to {0}. Ensure strings are in 'quotes'.".format(config.get(section, option))
                raise ValueError(err)

    return taskvals, config

def has_key(filename, section, key):
    config_dict,config = parse_config(filename)
    if has_section(filename, section) and key in config_dict[section]:
        return True
    return False

def has_section(filename, section):

    config_dict,config = parse_config(filename)
    return section in config_dict

def get_key(filename, section, key):
    config_dict,config = parse_config(filename)
    if has_key(filename, section, key):
        return config_dict[section][key]
    return ''

def remove_section(filename, section):

    config_dict,config = parse_config(filename)
    config.remove_section(section)
    config_file = open(filename, 'w')
    config.write(config_file)
    config_file.close()

def overwrite_config(filename, conf_dict={}, conf_sec='', sec_comment=''):

    config_dict,config = parse_config(filename)

    if conf_sec not in config.sections():
        processMeerKAT.logger.debug('Writing [{0}] section in config file "{1}" with:\n{2}.'.format(conf_sec,filename,conf_dict))
        config.add_section(conf_sec)
    else:
        processMeerKAT.logger.debug('Overwritting [{0}] section in config file "{1}" with:\n{2}.'.format(conf_sec,filename,conf_dict))

    if sec_comment != '':
        config.set(conf_sec, sec_comment)

    for key in conf_dict.keys():
        config.set(conf_sec, key, str(conf_dict[key]))

    config_file = open(filename, 'w')
    config.write(config_file)
    config_file.close()

def parse_spw(filename):

    config_dict,config = parse_config(filename)
    spw = config_dict['crosscal']['spw']
    nspw = config_dict['crosscal']['nspw']

    if ',' in spw:
        SPWs = spw.split(',')
        low,high,unit,dirs = [0]*len(SPWs),[0]*len(SPWs),['']*len(SPWs),['']*len(SPWs)
        for i,SPW in enumerate(SPWs):
            low[i],high[i],unit[i],func = processMeerKAT.get_spw_bounds(SPW)
            dirs[i] = '{0}~{1}{2}'.format(low[i],high[i],unit[i])

        lowest = min(low)
        highest = max(high)

        # Uncomment to simply use e.g. '*MHz'
        # if all([i == unit[0] for i in unit]):
        #     unit = unit[0]
        #     dirs = '*{0}'.format(unit)

    else:
        low,high,unit,func = processMeerKAT.get_spw_bounds(spw)
        dirs = []

    return low,high,unit,dirs

def validate_args(kwdict, section, key, dtype, default=None):
    """
    Validate the dictionary created by parse_config. Make sure
    that traling characters are removed, and the input types are correct.

    kwdict  The dictionary retured by config_parser.parse_config
    section The section in the config file to consider
    key     The specific keyword to validate
    dtype   The type the keyword should conform to.
    default If not none, if the keyword doesn't exist, assigns
            the variable this default value

    Valid types are:
        str, float, int, bool

    If str, the trailing '/' and trailing whitespaces are removed.
    An exception is raised if the validation fails.
    """

    # The input has already been parsed from the config file using
    # ast.literal_eval, so the dictionary values should have recognisable
    # python types.

    if default is not None:
        val = kwdict[section].pop(key, default)
    else:
        val = kwdict[section][key]

    if dtype is str:
        try:
            val = str(val).rstrip('/ ')
        except UnicodeError as err: # Pretty much the only error str() can raise
            raise
    elif dtype is int:
        try:
            val = int(val)
        except ValueError as err:
            raise
    elif dtype is float:
        try:
            val = float(val)
        except ValueError as err:
            raise
    elif dtype is bool:
        try:
            val = bool(val)
        except ValueError as err:
            raise
    else:
        raise NotImplementedError('Only str, int, bool, and float are valid types.')

    return val

if __name__ == '__main__':
    cliargs = parse_args()
    taskvals,config = parse_config(cliargs.config)
    print(taskvals)
