#!/bin/bash

singularity exec -B /cvmfs -B /pnfs -B /user -B /scratch /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest-py3.10 EXECUTABLEPATH/to_run_JOBNAME.sh