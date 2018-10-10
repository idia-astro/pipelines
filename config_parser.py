#! /usr/bin/env python

import argparse
import ConfigParser
import ast

def parse_args():
    """
    Parse the command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Dummy program to parse a config file using the ConfigParser class.')
    parser.add_argument('config', help='Name of the input config file')

    args, __ = parser.parse_known_args()

    return args


def parse_config(filename):
    """
    Given an input config file, parses it to extract key-value pairs that
    should represent task parameters and values respectively.
    """

    config = ConfigParser.SafeConfigParser()
    config.read(filename)

    # Build a nested dictionary with tasknames at the top level
    # and parameter values one level down.
    taskvals = dict()
    for section in config.sections():

        if section not in taskvals:
            taskvals[section] = dict()

        for option in config.options(section):
            # Evaluate to the right type()
            taskvals[section][option] =\
                            ast.literal_eval(config.get(section, option))

    return taskvals,config

def overwrite_config(filename,additional_dict={},additional_sec=''):

    config_dict,config = parse_config(filename)

    if additional_sec not in config.sections():
        config.add_section(additional_sec)

        for key in additional_dict.keys():
            config.set(additional_sec, key, str(additional_dict[key]))

        config_file = open(filename, 'w')
        config.write(config_file)
        config_file.close()

    else:
        print '{0} section exists in {1}. Will not overwrite.'.format(additional_sec,filename)


if __name__ == '__main__':
    cliargs = parse_args()
    taskvals,config = parse_config(cliargs.config)
    print taskvals

