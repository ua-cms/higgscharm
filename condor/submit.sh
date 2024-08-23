#!/bin/bash

singularity exec -B /cvmfs -B /pnfs -B /user -B /scratch /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-almalinux8:latest EXECUTABLEPATH/to_run_JOBNAME.sh