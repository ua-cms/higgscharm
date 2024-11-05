# H+c

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based BTV-PFNano files using [Coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.

- [Processors](#Processors)
- [Datasets](#Datasets)
- [Postprocessing](#Postprocessing)

### Processors

* `ztomumu`: Select events in a $Z\rightarrow \mu \mu$ region
* `ztoee`: Select events in a $Z\rightarrow ee$ region

The processors are defined in [`analysis/processors/<processor>.py`](https://github.com/deoache/higgscharm/tree/T2B/analysis/processors). The selections, variables, output histograms, triggers, among other features, are defined through a configuration file located in `analysis/configs/processor/<processor>/<year>.yaml` (see [here](https://github.com/deoache/higgscharm/blob/T2B/analysis/configs/README.md) for a detailed description). 


Jobs are submitted at T2B via the [submit_condor.py](https://github.com/deoache/higgscharm/blob/T2B/submit_condor.py) script:
```
usage: submit_condor.py [-h] [--processor PROCESSOR] [--dataset DATASET] [--year YEAR] [--stepsize STEPSIZE]

options:
  -h, --help            show this help message and exit
  --processor PROCESSOR
                        processor to be used {ztomumu, ztoee} (default ztomumu)
  --dataset DATASET     dataset name
  --year YEAR           dataset year {2022preEE, 2022postEE} (default 2022postEE)
  --stepsize STEPSIZE   stepsize param for coffea.dataset_tools.preprocess function
```
Example:
```bash
# connect to T2B
ssh -X -o ServerAliveInterval=100 <your_username>@m0.iihe.ac.be

# clone the repository (if you have not done it yet)
git clone -b T2B https://github.com/deoache/higgscharm.git
cd higgscharm

# submit condor jobs for some processor and dataset
python3 submit_condor.py --processor ztomumu --dataset MuonC --year 2022preEE 
``` 
After submitting the jobs you can watch their status typing:
```bash
watch condor_q
```
Outputs will be stored at `/pnfs/iihe/cms/store/user/<your_username>/higgscharm_outputs/<processor>/<year>`. 

### Datasets

The BTV-PFNano datasets have been produced following https://github.com/cms-btv-pog/btvnano-prod and stored in T2B (check the [2022 Summary Table Slide](https://docs.google.com/presentation/d/1F4ndU7DBcyvrEEyLfYqb29NGkBPs20EAnBxe_l7AEII/edit#slide=id.g289f499aa6b_2_52)). After the production of a dataset is finished, it is added to `analysis/filesets/<year>_fieleset.json` with the following format:

* Background MC
```
"DYto2L_2Jets_10to50":{
    "process":"DY+Jets",
    "era":"MC",
    "xsec":20950.0,
    "path":"/pnfs/iihe/cms/store/user/<your_username>/PFNano_Run3/path_to_dataset/"
}
```
* Data
```
"MuonC":{
    "process":"Data",
    "era":"C",
    "xsec":"None",
    "path":"/pnfs/iihe/cms/store/user/<your_username>/PFNano_Run3/path_to_dataset/"
}
```
In some cases, root files corresponding to a dataset are stored in multiple folders that share the same parent folder. In such cases, `path` must point to this parent folder.

### Postprocessing

Once you have run the corresponding datasets for a processor, you can get the results by typing:
```bash
singularity shell -B /cvmfs -B /pnfs -B /user /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-base-almalinux8:0.7.22-py3.8
``` 

```bash
python3 run_postprocess.py --processor <processor> --year <year>
``` 
You can also add the `--log_scale` flag to change the y-axis to log scale. Results will be saved to the same directory as the output files