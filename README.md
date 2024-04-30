# H+c

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based BTV-PFNano files using the [coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.

- [Filesets](#Filesets)
- [Processors](#Processors)


### Filesets

Coffea relies mainly on uproot to provide access to ROOT files for analysis. The ROOT files can be preprocessed with uproot and dask-awkward to extract the fileset. A fileset is a collection of metadata about the file location, file name, chunks splitting, that can be used directly to configure the uproot reading. 

To perform the preprocessing of some dataset locally type (at lxplus):
```
# activate your proxy
voms-proxy-init --voms cms

# get the singularity shell 
singularity shell -B /afs -B /eos -B /cvmfs /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask:latest-py3.10

# run the 'build_dataset_runnable' script
python build_dataset_runnable.py --dataset_name <dataset name> --year <year>
```
You can also submit condor jobs to build the filesets:
```
# activate your proxy
voms-proxy-init --voms cms

# run the 'build_dataset_runnable_condor' script
python build_dataset_runnable_condor.py.py --dataset_name <dataset name> --year <year>
```
Use `--dataset_name all` to submit jobs for all datasets defined at `analysis/configs/dataset/<year>`. 

The preprocessed datasets will be saved to `analysis/filesets/dataset_runnables/<year>`. They can be used directly to start an analysis with dask-awkward.

    
### Processors

Processors are coffea’s way of encapsulating an analysis. The available processors are:
* `signal`: Use to select a candidate higgs and one c-tagged jet. Output parquet files. 
```
python submit_condor.py --processor signal --dataset_name <dataset name>
```    

* `tag_eff`: Use to fill histograms subsequently used to compute the efficiency maps of some tagger for a particular hadronic flavor and a particular working point
```
python submit_condor.py --processor tag_eff --dataset_name <dataset name> --tagger <tagger> --flavor <hadron flavor> --wp <working point>
```
where the available options are 

    - tagger: deepjet, pnet or part
    - flavor: c or b
    - wp: loose, medium or tight

* `taggers`: Use to get CvsL and CvsB distributions for the DeepJet, ParticleNet and RobustParticleTransformer taggers
```
python submit_condor.py --processor taggers --dataset_name <dataset name>
```