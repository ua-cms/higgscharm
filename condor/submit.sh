#!/bin/bash -xe

JOBID=$1
BASEDIR=$2
X509PATH=$3

# Set the working directory as HOME by default, then use the user's AFS home
# directory when it is available to ensure access to their persistent files.
export HOME=`pwd`
if [ -d /afs/cern.ch/user/${USER:0:1}/$USER ]; then
  export HOME=/afs/cern.ch/user/${USER:0:1}/$USER
fi

# Configure XRootD to use IPv4 and enable its run fork handler, then provide
# the grid certificate proxy required to access protected CMS storage resources.
export XRD_NETWORKSTACK=IPv4
export XRD_RUNFORKHANDLER=1
export X509_USER_PROXY=$X509PATH
voms-proxy-info -all -file $X509PATH

# Store the job's initial working directory before switching to the analysis
# repository, since the job configuration and partition files are located here.
WORKDIR=`pwd`

# Load the LCG software environment, which provides the Python installation
# and the packages required to run the analysis.
LCG_SETUP="/cvmfs/sft.cern.ch/lcg/views/LCG_110/x86_64-el9-gcc13-opt/setup.sh"
source "$LCG_SETUP"

# Read the analysis configuration from arguments.json and store each field
# in an associative array for convenient access when building the submit command.
declare -A ARGS
for key in workflow year output_path output_format dataset user executor; do
    ARGS[$key]=$(python3 -c "import json; print(json.load(open('$WORKDIR/arguments.json'))['$key'])")
done

# Assemble the command-line arguments required by submit.py, appending the
# current job ID to the dataset name to identify the specific partition being processed.
OPTS="--workflow ${ARGS[workflow]} --year ${ARGS[year]} --output_path ${ARGS[output_path]} --executor ${ARGS[executor]} --output_format ${ARGS[output_format]} --dataset ${ARGS[dataset]}_$JOBID --user ${ARGS[user]}"

# Extract the fileset assigned to this job from partitions.json and save it
# as a separate JSON file that submit.py can use to process only this partition.
python3 -c "import json; json.dump(json.load(open('$WORKDIR/partitions.json'))['$JOBID'], open('$WORKDIR/partition_fileset.json', 'w'), indent=4)"
OPTS="$OPTS --partition_json $WORKDIR/partition_fileset.json"
echo $OPTS

# Switch to the analysis repository and launch the processing script
# with the configuration and partition-specific fileset prepared above.
cd $BASEDIR
python3 submit.py $OPTS