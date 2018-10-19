# Pipelines

#### This repository contains scripts for pipeline processing of MeerKAT data. It is a work in progress, and so far, just runs the cross-calibration steps.

## Requirements

This pipeline is designed to run on the IDIA data intensive research facility, making use of SLURM and MPICASA. For other use, please contact the authors.

## Getting it working

1. To get things working, run setup.sh, which will add to your PATH and PYTHONPATH.

2. To build a config file, which the pipeline reads as input for how to process the data, run 

```processMeerKAT.py -B --config myconfig.txt -M mydata.ms```

3. To run the pipeline, run 

```processMeerKAT.py -R --config myconfig.txt```

This will create `submit_pipeline.sh`, which you can then run to submit all pipeline jobs to a SLURM queue. It will also create `killJobs.sh`, which you can run to kill the submitted jobs.

4. For help, run
```processMeerKAT.py -h```
