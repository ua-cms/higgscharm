# H+c

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based BTV-PFNano files using [Coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.

- [Processors](#Processors)
- [How to submit jobs](#How-to-submit-jobs)
- [Postprocessing](#Postprocessing)

### Processors

* `ztomumu`: Select events in a $Z\rightarrow \mu \mu$ region
* `ztoee`: Select events in a $Z\rightarrow ee$ region

The processors are defined in [`analysis/processors/<processor>.py`](https://github.com/deoache/higgscharm/tree/lxplus/analysis/processors). The selections, variables, output histograms, triggers, among other features, are defined through a configuration file located in `analysis/configs/processor/<processor>/<year>.yaml` (see [here](https://github.com/deoache/higgscharm/blob/lxplus/analysis/configs/README.md) for a detailed description). 


### How to submit jobs

Connect to lxplus and clone the repository
```
# connect to LXPLUS
ssh <your_username>@lxplus.cern.ch

# clone the repository (if you have not done it yet)
git clone -b lxplus https://github.com/deoache/higgscharm.git
cd higgscharm
```
You need to have a valid grid proxy in the CMS VO. (see [here](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideLcgAccess) for details on how to register in the CMS VO). The needed grid proxy is obtained via the usual command
```
voms-proxy-init --voms cms
```
Jobs are submitted at LXPLUS via the [submit_condor.py](https://github.com/deoache/higgscharm/blob/lxplus/submit_condor.py) script:
```
usage: submit_condor.py [-h] [--processor PROCESSOR] [--dataset DATASET] [--year YEAR] [--nfiles NFILES] [--eos] [--submit]

optional arguments:
  -h, --help            show this help message and exit
  --processor PROCESSOR
                        processor to be used {ztomumu, ztoee} (default ztomumu)
  --dataset DATASET     dataset name
  --year YEAR           dataset year {2022preEE, 2022postEE} (default 2022postEE)
  --nfiles NFILES       number of root files to include in each dataset partition (default 20)
  --eos                 Enable saving outputs to /eos
  --submit              Enable Condor job submission. If not provided, it just builds condor files
```
Example:
```
python3 submit_condor.py --processor ztomumu --dataset MuonE --year 2022postEE --submit
```
**Note**: If you add the `--eos` flag, outputs will be save to your `/eos` area. **In this case, you will need to clone the repo also in [SWAN](https://swan-k8s.cern.ch/hub/spawn) (select the 105a release) in order to be able to run the postprocess step afterwards**.

The [runner.py](https://github.com/deoache/higgscharm/blob/lxplus/runner.py) script is built on top of `submit_condor.py` and can be used to submit all jobs (background + data) of a control region for certain year
```
usage: runner.py [-h] [--processor PROCESSOR] [--year YEAR] [--nfiles NFILES] [--submit] [--eos]

optional arguments:
  -h, --help            show this help message and exit
  --processor PROCESSOR
                        processor to be used {ztomumu, ztoee} (default ztomumu)
  --year YEAR           dataset year {2022preEE, 2022postEE} (default 2022postEE)
  --nfiles NFILES       number of root files to include in each dataset partition (default 20)
  --submit              Enable Condor job submission. If not provided, it just builds condor files
  --eos                 Enable saving outputs to /eos
```
Example:
```
python3 runner.py --processor ztomumu --year 2022postEE --submit
``` 
After submitting the jobs you can watch their status by typing:
```
watch condor_q
```
Some jobs might crash due to T2B being down or for whatever reason. You can use the `resubmitter.py` script to see which jobs are still to be executed
```
usage: resubmitter.py [-h] [--resubmit] [--processor PROCESSOR] [--year YEAR] [--eos]

optional arguments:
  -h, --help            show this help message and exit
  --resubmit            if True resubmit the jobs. if False only print the missing jobs
  --processor PROCESSOR
                        processor to be used {ztomumu, ztoee}
  --year YEAR           year of the data {2022preEE, 2022postEE}
  --eos                 Enable reading outputs from /eos
```
Example:
```
python3 resubmitter.py --processor ztomumu --year 2022postEE
```
Adding the `--resubmit` flag will resubmit the missing jobs.

Outputs will be stored at `/eos/user/<user_name>[0]/<user_name>/higgscharm/outputs/<processor>/<year>`. 

### Postprocessing

Once you have all the output files for a processor/year, you can get the results using the `run_postprocess.py` script:
```
usage: run_postprocess.py [-h] [--processor PROCESSOR] [--year YEAR] [--log_scale] [--yratio_limits YRATIO_LIMITS YRATIO_LIMITS] [--eos]

optional arguments:
  -h, --help            show this help message and exit
  --processor PROCESSOR
                        processor to be used {ztomumu, ztoee}
  --year YEAR           year of the data {2022preEE, 2022postEE}
  --log_scale           Enable log scale for y-axis
  --yratio_limits YRATIO_LIMITS YRATIO_LIMITS
                        Set y-axis ratio limits as a tuple (e.g., --yratio_limits 0 2)
  --eos                 Enable reading outputs from /eos
```
from the main folder type:
```
# from LXPLUS
python3 run_postprocess.py --processor ztomumu --year 2022postEE --log_scale
``` 
```
# from SWAN
python3 run_postprocess.py --processor ztomumu --year 2022postEE --log_scale --eos
``` 
Results will be saved to the same directory as the output files