# H+c

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based BTV-PFNano files using the [coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.


### Processors

The available processor is:
* `ztomumu`: Select events in a Z($\rightarrow \mu \mu$) region

To run the processor:
```
# connect to T2B
ssh -X -o ServerAliveInterval=100 <your_username>@m0.iihe.ac.be

# clone the repository (if you have not done it yet)
git clone -b dask https://github.com/deoache/higgscharm.git
cd higgscharm

# submit condor jobs for some processor and dataset
python3 submit_condor.py --processor ztomumu --dataset <dataset name> --year <year> 
```    
* Data samples: `MuonC` and `MuonD` for `2022`. `MuonE`, `MuonF` and `MuonD` for `2022EE`
* Background samples: `DYto2L_2Jets_10to50` and `DYto2L_2Jets_50` for `2022` and `2022EE`

Outputs will be stored at `/pnfs/iihe/cms/store/user/<your_username>/higgscharm_outputs`. Once you have run the corresponding datasets for a processor, you can get the results by typing:
```
singularity shell -B /cvmfs -B /pnfs -B /user /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-almalinux8:latest

python3 run_postprocess.py --processor ztomumu --year <year>
``` 
Results will be saved in the same directory as the output files