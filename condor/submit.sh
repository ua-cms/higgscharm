#!/bin/bash

export XRD_NETWORKSTACK=IPv4
export XRD_RUNFORKHANDLER=1
export X509_USER_PROXY=X509PATH
export X509_USER_CERT=X509PATH
export X509_CERT_DIR=/cvmfs/cms.cern.ch/grid/etc/grid-security/certificates
export X509_VOMS_DIR=/cvmfs/cms.cern.ch/grid/etc/grid-security/vomsdir
export XRD_REQUESTTIMEOUT=3600
voms-proxy-info -all -file X509PATH
cd MAINDIRECTORY

COMMAND