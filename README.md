<p align="center">
   <img src="https://raw.githubusercontent.com/idia-pipelines/idia-pipelines.github.io/master/assets/idia_logo.jpg" alt="IDIA pipelines"/>
</p>

# The IDIA MeerKAT Pipeline

The IDIA MeerKAT pipeline is a radio interferometric calibration pipeline designed to process MeerKAT data. **It is under heavy development, and so far only implements the cross-calibration steps, and quick-look images. Please report any issues you find in the [GitHub issues](https://github.com/idia-astro/pipelines/issues).**

## Requirements

This pipeline is designed to run on the Ilifu cluster, making use of SLURM and MPICASA. For other uses, please contact the authors. Currently, use of the pipeline requires access to the Ilifu cloud infrastructure. You can request access using the following [form](http://docs.ilifu.ac.za/#/access/request_time).

## Quick Start

### 1. In order to use the `processMeerKAT.py` script, source the `setup.sh` file:

        source /data/exp_soft/pipelines/master/setup.sh

which will add the correct paths to your `$PATH` and `$PYTHONPATH` in order to correctly use the pipeline. We recommend you add this to your `~/.profile`, for future use.

### 2. Build a config file:

        processMeerKAT.py -B -C myconfig.txt -M mydata.ms


This defines several variables that are read by the pipeline while calibrating the data, as well as requesting resources on the cluster. The config file parameters are described by in-line comments in the config file itself wherever possible.

### 3. To run the pipeline:

        processMeerKAT.py -R -C myconfig.txt

This will create `submit_pipeline.sh`, which you can then run like `./submit_pipeline.sh` to submit all pipeline jobs to the SLURM queue.

Other convenience scripts are also created that allow you to monitor and (if necessary) kill the jobs. `summary.sh` provides a brief overview of the status of the jobs in the pipeline, `findErrors.sh` checks the log files for commonly reported errors, and `killJobs.sh` kills all the jobs from the current run of the pipeline, ignoring any other jobs you might have running.

For help, run `processMeerKAT.py -h`, which provides a brief description of all the command line arguments.

The documentation can be accessed on the [pipelines website](https://idia-pipelines.github.io/docs/processMeerKAT), or on the [Github wiki](https://github.com/idia-astro/pipelines/wiki).
