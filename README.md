# H+c

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based NanoAOD V12 files using [Coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.

- [Processors](#Processors)
- [Build input datasets](#Build-input-datasets)
- [Submit Condor jobs](#Submit-Condor-jobs)
- [Postprocessing](#Postprocessing)

### Processors

* `zzto4l`: Select events in a $H \rightarrow ZZ \rightarrow \ell\ell$ region
* `ztomumu`: Select events in a $Z\rightarrow \mu \mu$ region
* `ztoee`: Select events in a $Z\rightarrow ee$ region

The processors are defined in [`analysis/processors/<processor>.py`](https://github.com/deoache/higgscharm/tree/lxplus/analysis/processors). The selections, variables, output histograms, triggers, among other features, are defined through a configuration file located in `analysis/configs/processor/<processor>/<year>.yaml` (see [here](https://github.com/deoache/higgscharm/blob/lxplus/analysis/configs/README.md) for a detailed description). 


### Build input datasets

Each year/campaign has a config file in [`analysis/filesets/<campaign>_nanov12.yaml`](https://github.com/deoache/higgscharm/tree/lxplus/analysis/filesets) from which the input datasets are built. A config file contains the key name, era, associated process, cross section and DAS query of the datasets

```yaml
EGammaD:
  era: D
  query: EGamma/Run2022D-22Sep2023-v1/NANOAOD
  process: Data
  xsec: null
DYto2L_2Jets_50:
  era: MC
  query: DYto2L-2Jets_MLL-50_TuneCP5_13p6TeV_amcatnloFXFX-pythia8/Run3Summer22NanoAODv12-130X_mcRun3_2022_realistic_v5-v2/NANOAODSIM
  process: DY+Jets
  xsec: 6688.0
```

To build the datasets, first connect to lxplus and clone the repository (if you have not done it yet)
```
ssh <your_username>@lxplus.cern.ch

git clone -b lxplus https://github.com/deoache/higgscharm.git
cd higgscharm
```
You need to have a valid grid proxy in the CMS VO. (see [here](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideLcgAccess) for details on how to register in the CMS VO). The needed grid proxy is obtained via the usual command
```
voms-proxy-init --voms cms
```

Use the [make_filesets.py](https://github.com/deoache/higgscharm/blob/lxplus/analysis/filesets/make_filesets.py) script to build the input filesets with xrootd endpoints:
```
# get the singularity shell 
singularity shell -B /afs -B /eos -B /cvmfs /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest-py3.10

# move to the fileset directory
cd analysis/filesets/

# run the 'make_filesets' script
python3 make_filesets.py --year <year>

# exit the singularity shell
exit
```

### Submit Condor jobs


Jobs are submitted via the [submit_condor.py](https://github.com/deoache/higgscharm/blob/lxplus/submit_condor.py) script:
```
usage: submit_condor.py [-h] [--processor {ztomumu,ztoee,zzto4l,hww}] [--dataset DATASET] [--year {2022preEE,2022postEE,2023preBPix,2023postBPix}] [--nfiles NFILES] [--eos] [--submit]
                        [--output_format {coffea,root}]

optional arguments:
  -h, --help            show this help message and exit
  --processor {ztomumu,ztoee,zzto4l,hww}
                        processor to be used
  --dataset DATASET     dataset name
  --year {2022preEE,2022postEE,2023preBPix,2023postBPix}
                        dataset year
  --nfiles NFILES       number of root files to include in each dataset partition (default 10)
  --eos                 Enable saving outputs to /eos
  --submit              Enable Condor job submission. If not provided, it just builds condor files
  --output_format {coffea,root}
                        format of output histogram
```
Example:
```
python3 submit_condor.py --processor ztomumu --dataset MuonE --year 2022postEE --submit --eos
```
**Note**: It's recommended to add the `--eos` flag to save the outputs in your `/eos` area, so the postprocessing step can be done from [SWAN](https://swan-k8s.cern.ch/hub/spawn). **In this case, you will need to clone the repo also in [SWAN](https://swan-k8s.cern.ch/hub/spawn) (select the 105a release) in order to be able to run the postprocess**.

The [runner.py](https://github.com/deoache/higgscharm/blob/lxplus/runner.py) script is built on top of `submit_condor.py` and can be used to submit all jobs (MC + data) of a processor/year
```
usage: runner.py [-h] [--processor {ztomumu,ztoee,zzto4l,hww}] [--year {2022preEE,2022postEE,2023preBPix,2023postBPix}] [--nfiles NFILES] [--submit] [--eos]
                 [--output_format {coffea,root}]

optional arguments:
  -h, --help            show this help message and exit
  --processor {ztomumu,ztoee,zzto4l,hww}
                        processor to be used
  --year {2022preEE,2022postEE,2023preBPix,2023postBPix}
                        dataset year
  --nfiles NFILES       number of root files to include in each dataset partition (default 10)
  --submit              Enable Condor job submission. If not provided, it just builds condor files
  --eos                 Enable saving outputs to /eos
  --output_format {coffea,root}
                        format of output histogram
```
Example:
```
python3 runner.py --processor ztomumu --year 2022postEE --submit --eos
``` 
After submitting the jobs you can watch their status by typing:
```
watch condor_q
```
You can use the `resubmitter.py` script to see which jobs are still to be executed
```
usage: resubmitter.py [-h] [--processor {ztomumu,ztoee,zzto4l,hww}] [--year {2022preEE,2022postEE,2023preBPix,2023postBPix}] [--eos] [--resubmit] [--output_format {coffea,root}]

optional arguments:
  -h, --help            show this help message and exit
  --processor {ztomumu,ztoee,zzto4l,hww}
                        processor to be used
  --year {2022preEE,2022postEE,2023preBPix,2023postBPix}
                        year of the data
  --eos                 Enable reading outputs from /eos
  --resubmit            if True resubmit the jobs. if False only print the missing jobs
  --output_format {coffea,root}
                        format of output histograms
```
Example:
```
python3 resubmitter.py --processor ztomumu --year 2022postEE --eos
```
Some jobs might crash due to dataset reading problems on some sites. In this case, identify the problematic site from the condor error file, remove it from the [sites list](https://github.com/deoache/higgscharm/blob/lxplus/analysis/filesets/make_filesets.py#L9-L32), generate the datasets again with `make_filesets.py`, create new condor files with `runner.py` or `submit_condor.py` (without the `--submit` flag), and resubmit the missing jobs adding the `--resubmit` flag:
```
python3 resubmitter.py --processor ztomumu --year 2022postEE --eos --resubmit
```

### Postprocessing

Once all jobs are done for a processor/year, you can get the results using the `run_postprocess.py` script:
```
usage: run_postprocess.py [-h] [--processor PROCESSOR] [--year {2022preEE,2022postEE}] [--log_scale] [--yratio_limits YRATIO_LIMITS YRATIO_LIMITS] [--postprocess] [--plot]
                          [--extension {pdf,png}] [--output_format {coffea,root}]

optional arguments:
  -h, --help            show this help message and exit
  --processor PROCESSOR
                        processor to be used {ztomumu, ztoee, zzto4l, hww}
  --year {2022preEE,2022postEE}
                        year of the data
  --log_scale           Enable log scale for y-axis
  --yratio_limits YRATIO_LIMITS YRATIO_LIMITS
                        Set y-axis ratio limits as a tuple (e.g., --yratio_limits 0 2)
  --postprocess         Enable postprocessing
  --plot                Enable plotting
  --extension {pdf,png}
                        extension to be used for plotting
  --output_format {coffea,root}
                        format of output histograms
```
Example:
```
python3 run_postprocess.py --processor ztomumu --year 2022postEE --postprocess --plot
``` 
Results will be saved to the same directory as the output files