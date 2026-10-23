# H+c

[![Codestyle](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Python package for analyzing H+c events. The package uses a columnar framework to process input tree-based NanoAOD V12 files using [Coffea](https://coffeateam.github.io/coffea/) and [scikit-hep](https://scikit-hep.org) Python libraries.

- [Setting up the environment](#Setting-up-the-environment)
- [Input filesets](#Input-filesets)
- [Workflows](#Workflows)
- [Local test](#Local-test)
- [Submit Condor jobs](#Submit-Condor-jobs)
- [Postprocessing](#Postprocessing)

## Setting up the environment

**1. Log in to lxplus and clone the repository**
   
If you haven't done so already:
```bash
ssh <your_username>@lxplus.cern.ch

git clone -b coffea-2026 https://github.com/deoache/bsm3g_coffea.git
cd bsm3g_coffea
```

**2. Initialize a valid CMS grid proxy**

To access remote datasets via xrootd, you need a valid grid proxy.

To generate the proxy:
```bash
voms-proxy-init --voms cms
```
If you're not registered in the CMS VO, follow these [instructions](https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideLcgAccess) to request access.


**3. Initialize the environment**

Run the following command
```bash
source setup.sh
```
This loads the CERN LCG_110 environment, which provides the required software stack, including Coffea 2026.5.0.

The setup script also ensures that the rucio-clients package is available. If it is not already installed, it is automatically installed in a local directory.

The environment must be initialized in each new shell session before running the analysis.


### Input filesets
 

Each data-taking year or campaign has a corresponding config file in `analysis/filesets/<year>_<nano version>.yaml`, which defines the input datasets to process

```yaml
EGammaRun2022D:
  era: D
  query: EGamma/Run2022D-22Sep2023-v1/NANOAOD
  process: Data
  key: electron
  xsec: None
DYJetsToLL:
  era: mc
  query: DYJetsToLL_M-50_TuneCP5_13p6TeV-madgraphMLM-pythia8/Run3Summer22NanoAODv12-forPOG_130X_mcRun3_2022_realistic_v5-v2/NANOAODSIM
  process: DY+Jets
  key: dy_lo
  xsec: 5558.0
```

- **`era`**: Dataset era. For data, this is typically the run letter (e.g. `B`, `F`, etc.). For MC datasets use `"mc"`, and for signal datasets use `"signal"`.
- **`process`**: Process name used during postprocessing to group multiple datasets into the same physical process.
- **`key`**: Label that associates the dataset with a group defined in the workflow configuration under `datasets` (see next section).
- **`query`**: DAS path used to fetch the files for this dataset.
- **`xsec`**: Cross section in picobarns (pb). Should be `null` for data. Required for MC and signal samples in order to compute event weights.

These fileset definitions are the entry point for the analysis: they control which datasets are included, how they are grouped, and how they are weighted, ensuring a transparent and reproducible workflow across different campaigns.

### Workflows

A workflow defines the full configuration of an analysis: which datasets are used, how objects and events are selected, which corrections and weights are applied, and which histograms are produced. Workflows are stored as YAML files in the [`analysis/workflows`](https://github.com/ua-cms/higgscharm/tree/main/analysis/workflows) directory.

Each workflow YAML is organized into the following main sections:

- **`datasets`**: which samples to run over (referencing dataset *keys* defined in the filesets)
- **`object_selection`**: how to build and filter physics objects (muons, electrons, jets, etc.)
- **`event_selection`**: event-level cuts, triggers, and category definitions
- **`corrections`**: physics corrections and event weights  
- **`histogram_config`**: definition of output histograms

Together, these sections control the full behavior of the analysis pipeline. **You can find a detailed explanation of the structure of a workflow** [here](https://github.com/ua-cms/higgscharm/blob/dev/signal/analysis/workflows/README.md) 

The available workflows are:

* $Z\rightarrow \ell\ell$
    * `ztomumu`: Select events in a $Z\rightarrow \mu \mu$ region
    * `ztoee`: Select events in a $Z\rightarrow ee$ region
* $H\rightarrow ZZ \rightarrow 4\ell$
    * Signal region 
        * `zzto4l`: Select $H \rightarrow ZZ \rightarrow 4\ell$ events
    * Z+X background estimation
        * `zplusl_os`: Select $Z+\ell$ events (lepton fake rate estimation with the OS Method)
        * `zplusll_os`: Select $Z+\ell\ell$ events (Z+X estimation with the OS Method)
        * `zplusl_ss`: Select $Z+\ell$ events (lepton fake rate estimation with the SS Method)
        * `zplusll_ss`: Select $Z+\ell\ell$ events (Z+X estimation with the SS Method)
* $H(\rightarrow ZZ \rightarrow 4\ell)+c$
    *  `hplusc`: Select $H(\rightarrow ZZ \rightarrow 4\ell)+c$ events


The workflows (selections, variables, output histograms, triggers, etc) are defined through a configuration file located in `analysis/workflows/workflow/<workflow>.yaml`. [Here](https://github.com/deoache/higgscharm/blob/lxplus/analysis/workflows/README.md) you can find a detailed description on how to create the config file.


### Local run

Local tests can be run using the `tester.ipynb`. There, you can set the workflow, year, dataset, and executor (feel free to add more datasets in case you need to run a particular workflow). This way, you can check that the workflow is running without issues before submitting batch jobs. It also allows you to interact with the output to check that it makes sense and contains the expected information.


To ensure that the notebook uses the same software environment as the analysis, it should be executed with the LCG_110 environment which can be added as a Jupyter kernel by running the following commands in a terminal:

```bash
mkdir -p ~/.local/share/jupyter/kernels/lcg_110
cat << 'EOF' > ~/.local/share/jupyter/kernels/lcg_110/kernel.json
{
 "argv": [
  "/bin/bash",
  "-c",
  "source /cvmfs/sft.cern.ch/lcg/views/LCG_110/x86_64-el9-gcc13-opt/setup.sh && exec python3 -m ipykernel_launcher -f {connection_file}"
 ],
 "display_name": "LCG 110 (CVMFS)",
 "language": "python"
}
EOF
```
After creating the kernel, refresh the available Jupyter kernels in your IDE. If the new kernel is not detected, restart the IDE. The LCG 110 (CVMFS) kernel should then be available for selection in the notebook.


### Submit Condor jobs

**1. Submit all datasets from a workflow**

Use [runner.py](https://github.com/deoache/higgscharm/blob/lxplus/runner.py) to submit jobs for a specific workflow and year. 
```bash
python3 runner.py --workflow <workflow> --year <year> --submit
``` 

You could use [submit_condor.py](https://github.com/deoache/bsm3g_coffea/blob/main/submit_condor.py) to submit jobs for a specific dataset:
```bash
python3 submit_condor.py --workflow <workflow> --dataset <dataset> --year <campaign> --submit 
```

**2. Monitor job status**

To continuously monitor your Condor jobs:
```bash
watch condor_q
```
To get a summary of missing, failed, or incomplete jobs, and resubmit them, use:
```bash
python3 jobs_status.py --workflow <workflow> --year <campaign> 
```
This command will:
- Report total expected, completed, and missing jobs per dataset.
- Analyze recent xrootd-related errors on a per-dataset basis.
- Optionally regenerate input filesets for datasets with failing sites. By default, datasets are grouped by identical failing sites to minimize repeated fetches.
- Use the `--single_fetch` flag to merge all datasets with failing sites into a single group such that `fetch.py` is run only once.
- Optionally resubmit missing jobs.


### Postprocessing

Once the Condor jobs are completed and all outputs are saved under your EOS area on the `higgscharm_outputs/` directory.

You can run `run_postprocess.py` to aggregate results, compute cutflows, and generate plots
```bash
python3 run_postprocess.py --workflow <workflow> --year <year> --postprocess --plot --log
``` 

After running post-processing for the two campaigns of a particular year, you can use the same command (with `--year 2022` or `--year 2023`) to automatically combine both campaigns and compute joint results and plots.

Results will be saved to the same directory as the output files.