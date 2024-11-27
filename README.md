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

Connect to lxplus and clone the repository (**you need to clone the repo also in [SWAN](https://swan-k8s.cern.ch/hub/spawn) (select the 105a release) in order to be able to run the postprocess**)
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
usage: submit_condor.py [-h] [--processor PROCESSOR] [--dataset DATASET] [--year YEAR] [--submit] [--nfiles NFILES]

optional arguments:
  -h, --help            show this help message and exit
  --processor PROCESSOR
                        processor to be used {ztomumu, ztoee} (default ztomumu)
  --dataset DATASET     dataset name
  --year YEAR           dataset year {2022preEE, 2022postEE} (default 2022postEE)
  --submit              Enable Condor job submission. If not provided, it just condor files 
  --nfiles NFILES       number of root files to include in each dataset partition (default 20)
```
The [runner.py](https://github.com/deoache/higgscharm/blob/lxplus/runner.py) script is built on top of `submit_condor.py` and can be used to submit all jobs (background + data) of a control region for certain year: 
```
python3 runner.py --processor <processor> --year <year>
``` 
After submitting the jobs you can watch their status typing:
```
watch condor_q
```
Some jobs might crash due to T2B being down or for whatever reason. You can use the `resubmitter.py` script to see which jobs are still to be executed
```
python3 resubmitter.py --processor <processor> --year <year>
```
Adding the `--resubmit` flag will resubmit these missing jobs.

Outputs will be stored at `/eos/user/<user_name>[0]/<user_name>/higgscharm/outputs/<processor>/<year>`. 

### Postprocessing

Once you have run the corresponding datasets for a processor, you can get the results using the `run_postprocess.py` script:
```
usage: run_postprocess.py [-h] [--processor PROCESSOR] [--year YEAR] [--log_scale] [--yratio_limits YRATIO_LIMITS YRATIO_LIMITS] [--output_dir OUTPUT_DIR]

optional arguments:
  -h, --help            show this help message and exit
  --processor PROCESSOR
                        processor to be used {ztomumu, ztoee}
  --year YEAR           year of the data {2022preEE, 2022postEE}
  --log_scale           Enable log scale for y-axis
  --yratio_limits YRATIO_LIMITS YRATIO_LIMITS
                        Set y-axis ratio limits as a tuple (e.g., --yratio_limits 0 2)
  --output_dir OUTPUT_DIR
                        Path to the outputs directory (optional)
```
In SWAN, type: 
```
# go to higgscharm folder
cd higgscharm

# run postprocess script
python3 run_postprocess.py --processor ztomumu --year 2022preEE --log_scale
``` 
Results will be saved to the same directory as the output files