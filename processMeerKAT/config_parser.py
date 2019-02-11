#!/usr/bin/env python2.7

import argparse
import ConfigParser
import ast
import processMeerKAT

def parse_args():
    """
    Parse the command line arguments
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True, help='Name of the input config file')

    args, __ = parser.parse_known_args()

    return vars(args)


def parse_config(filename):
    """
    Given an input config file, parses it to extract key-value pairs that
    should represent task parameters and values respectively.
    """

    config = ConfigParser.SafeConfigParser(allow_no_value=True)
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

