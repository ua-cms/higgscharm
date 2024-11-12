#!/bin/bash

singularity exec -B /cvmfs -B /pnfs -B /user -B /scratch /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:0.7.22-py3.9-g7f049 EXECUTABLEPATH/to_run_JOBNAME.sh