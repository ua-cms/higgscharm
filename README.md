# H+c

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based BTV-PFNano files using [Coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.

- [Processors](#Processors)
- [Datasets](#Datasets)
- [Postprocessing](#Postprocessing)

### Processors

* `ztomumu`: Select events in a $Z\rightarrow \mu \mu$ region

It is defined in the [ZToMuMuProcessor](https://github.com/deoache/higgscharm/blob/dask/analysis/processors/ztomumu.py) class. The selection, variables, output histograms, triggers, among other features, are defined through a configuration file located in `analysis/configs/processor/<year>` (see [here](https://github.com/deoache/higgscharm/blob/dask/analysis/configs/README.md) for a detailed description). 


To submit jobs at T2B using HTCondor we use [submit_condor.py](https://github.com/deoache/higgscharm/blob/dask/submit_condor.py):
```bash
# connect to T2B
ssh -X -o ServerAliveInterval=100 <your_username>@m0.iihe.ac.be

# clone the repository (if you have not done it yet)
git clone -b dask https://github.com/deoache/higgscharm.git
cd higgscharm

# submit condor jobs for some processor and dataset
python3 submit_condor.py --processor ztomumu --dataset <dataset name> --year <year> 
``` 
The script will:
* Create the folders containing the [logs and outputs](https://github.com/deoache/higgscharm/blob/dask/condor/utils.py#L17-L20) within the `/condor` folder.
* Build the [condor](https://github.com/deoache/higgscharm/blob/dask/condor/utils.py#L22-L37) and [executable](https://github.com/deoache/higgscharm/blob/dask/condor/utils.py#L39-L49) files from the [submit.sub](https://github.com/deoache/higgscharm/blob/dask/condor/submit.sub) and [to_submit.sh](https://github.com/deoache/higgscharm/blob/dask/condor/to_submit.sh) templates, respectively (click [here](https://batchdocs.web.cern.ch/local/quick.html) for more info).
* Create a [run script](https://github.com/deoache/higgscharm/blob/dask/condor/utils.py#L51-L61) from the [submit.sh](https://github.com/deoache/higgscharm/blob/dask/condor/submit.sh) template in order to run the executable file using a Coffea singularity image ([coffea images](https://github.com/CoffeaTeam/af-images) |  [SingularityContainers at T2B](https://t2bwiki.iihe.ac.be/SingularityContainers)) 

After submitting the jobs you can watch their status typing:
```bash
condor_q
```

Outputs will be stored at `/pnfs/iihe/cms/store/user/<your_username>/higgscharm_outputs`. 

### Datasets

The BTV-PFNano datasets have been produced following https://github.com/cms-btv-pog/btvnano-prod and stored in T2B. After the production of a dataset is finished, it is added to `analysis/filesets/<year>_fieleset.json` with the following format:

```
"DYto2L_2Jets_10to50": {
    "process": "DY+Jets",
    "era": "MC",
    "xsec": 20950.0,
    "path": "/pnfs/iihe/cms/store/user/<username>/PFNano_Run3/path_to_dataset/"
}
```
* In some cases, root files corresponding to a dataset are stored in multiple folders that share the same parent folder. In such cases, `path` must point to this parent folder.

* For the `ztomumu` processor you should use the following datasets (check the [2022 Summary Table Slide](https://docs.google.com/presentation/d/1F4ndU7DBcyvrEEyLfYqb29NGkBPs20EAnBxe_l7AEII/edit#slide=id.g289f499aa6b_2_52)):
    * 2022:
        * Data: `MuonC`, `MuonD`.
        * Background: `DYto2L_2Jets_10to50`, `DYto2L_2Jets_50`
    * 2022EE:
        * Data: `MuonE`, `MuonF`, `MuonG`
        * Background: `DYto2L_2Jets_10to50`, `DYto2L_2Jets_50`

### Postprocessing

Once you have run the corresponding datasets for a processor, you can get the results by typing:
```bash
singularity shell -B /cvmfs -B /pnfs -B /user /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-almalinux8:latest

python3 run_postprocess.py --processor ztomumu --year <year>
``` 
Results (plots, cutflow and results tables, processor config and postprocessor output) will be saved in the same directory as the output files