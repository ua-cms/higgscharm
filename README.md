# H+c

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based BTV-PFNano files using the [coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.


### Processors

The available processor is:
* `ztoll`: Select events with Z($\rightarrow \ell \ell$) final events

To run the processor:
```
# connect to T2B
ssh -X -o ServerAliveInterval=100 <your_username>@m0.iihe.ac.be

# clone the repository (if you have not done it yet)
git clone -b t2b_iihe https://github.com/deoache/higgscharm.git
cd higgscharm

# submit condor jobs for some processor and dataset
python3 submit_condor.py --processor ztoll --dataset_name <dataset name> --year <year> --lepton_flavor <lepton flavor>
```    
where 
* `--lepton_flavor`: `muon` or `electron`. 
* `--year`: `2022` or `2022EE`

You can find the available datasets at `analysis/configs/dataset/<year>`

Outputs will be stored at `/pnfs/iihe/cms/store/user/<your_username>/higgscharm_outputs`. Once you have run the corresponding datasets for a processor, you can get the results by typing:
```
singularity shell -B /cvmfs -B /pnfs -B /user /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-almalinux8:latest

python3 run_postprocess.py --processor <processor> --year <year> --lepton_flavor <lepton flavor>
``` 
The results will be saved in the same directory as the output files