<p align="center">
   <img src="https://i1.wp.com/www.idia.ac.za/wp-content/uploads/2018/08/cropped-idia_grey_BW.png" alt="IDIA pipelines"/>
</p>

# processMeerKAT: The IDIA Pipeline

The IDIA pipeline is a radio interferometric calibration pipeline designed to
process MeerKAT data. **It still under heavy development, and at the moment only
implements the cross-calibration steps. Please report any issues you find in the
issue tracker above.**.

## Requirements

This pipeline is designed to run on the IDIA data intensive research facility,
making use of SLURM and MPICASA. For other uses, please contact the authors.
Currently, use of the pipeline requires access to the IDIA cloud infrastructure.

## Quick start

1. In order to use the `processMeerKAT` script, source the `setup.sh` file :

        source /data/exp_soft/pipelines/master/setup.sh

    which will add the correct paths to your `$PATH` and `$PYTHONPATH` in order
    to correctly use the pipeline.

2. Build a config file, which defines several variables that are read by the
   pipeline while calibrating the data, as well as requesting resources on the
   cluster.

        processMeerKAT.py -B --config myconfig.txt -M mydata.ms


    The config file parameters are described by in-line comments in the config
    file itself wherever possible.

3. To run the pipeline,

        processMeerKAT.py -R --config myconfig.txt

    This will create `submit_pipeline.sh`, which you can then run like
    `./submit_pipeline.sh` to submit all pipeline jobs to a SLURM queue.

Other convenience scripts are also created that allow you to monitor and (if
necessary) kill the jobs. `summary.sh` provides a brief overview of the status
of the jobs in the pipeline, `findErrors.sh` checks the log files for commonly
reported errors, and `killJobs.sh` kills all the jobs from the current run of
the pipeline, ignoring any other jobs you might have running.

For help, run `processMeerKAT -h` which provides a brief description of all the
parameters in the script.

The documentation can be accessed on the [processMeerKAT
website](https://idia-pipelines.github.io/docs/processMeerKAT), or on the Github
wiki.
