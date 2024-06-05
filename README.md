# H+c

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based BTV-PFNano files using the [coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.


### Processors

The available processors are:
* `ztomumu`: Select events with Z($\mu \mu$) final events
* `zplusjet`: Select events with Z($\mu \mu$)+c final events (a candidate Z and one c-tagged jet)
* `signal`: Select events with H(ZZ)+c final events (a candidate Higgs and one c-tagged jet)
* `tag_eff`: Save jet features needed to compute efficiency maps
* `taggers`: Save CvsL and CvsB distributions for the DeepJet, ParticleNet and RobustParticleTransformer taggers

To run a processor:
```
# connect to lxplus 
ssh <your_username>@lxplus.cern.ch

# clone the repository (if you have not done it yet)
git clone https://github.com/deoache/higgscharm.git
cd higgscharm

# submit condor jobs for some processor and dataset
python3 submit_condor.py --processor <processor> --dataset_name <dataset name>
```    
You can find the available datasets at [analysis/configs/dataset/2022EE](https://github.com/deoache/higgscharm/tree/main/analysis/configs/dataset/2022EE). The `tag_eff` processor requires additional arguments: `--tagger` (deepjet, pnet or part), `--flavor` (c or b), and `--wp` (loose, medium or tight)