#! /usr/bin/env python

import argparse


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

    import ConfigParser
    import ast

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
            taskvals[section][option] = ast.literal_eval(config.get(section, option))


    return taskvals

if __name__ == '__main__':
    cliargs = parse_args()
    taskvals = parse_config(cliargs.config)
