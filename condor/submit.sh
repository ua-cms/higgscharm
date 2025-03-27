#!/bin/bash

JOBID=$1

export XRD_NETWORKSTACK=IPv4
export XRD_RUNFORKHANDLER=1
export X509_USER_PROXY=X509PATH
voms-proxy-info -all
voms-proxy-info -all -file X509PATH
cd MAINDIRECTORY

ARGS="--processor PROCESSOR --year YEAR --output_path OUTPUTPATH --output_format OUTPUTFORMAT --dataset DATASET_$JOBID"

python -c "import json; json.dump(json.load(open('JOBDIR/split_samples.json'))['$JOBID'], open('MAINDIRECTORY/partition_fileset.json', 'w'), indent=4)"
ARGS="$ARGS --partition_json MAINDIRECTORY/partition_fileset.json"

python3 submit.py $ARGS