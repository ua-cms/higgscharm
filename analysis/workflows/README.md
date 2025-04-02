### Workflow config

Workflow's selection, variables, output histograms, triggers, among other features are defined through a yaml configuration file:


* `object_selection`: Contains the information required for object selection:
```yaml
object_selection:
  muons:
    field: events.Muon
    cuts:
      - events.Muon.pt > 10
      - np.abs(events.Muon.eta) < 2.4
      - np.abs(events.Muon.dxy) < 0.5
      - np.abs(events.Muon.dz) < 1
      - working_points.muon_iso(events, 'tight')
      - working_points.muon_id(events, 'tight')
  electrons:
    field: events.Electron
    cuts:
      - events.Electron.pt > 10
      - np.abs(events.Electron.eta) < 2.5
      - events.Electron.dxy < 0.5
      - events.Electron.dz < 1
      - working_points.electron_id(events, 'wp80iso')
      - delta_r_higher(events.Electron, objects['muons'], 0.4)
  dimuons:
    field: select_dimuons
    cuts:
      - objects['dimuons'].l1.delta_r(objects['dimuons'].l2) > 0.02
      - objects['dimuons'].l1.charge * objects['dimuons'].l2.charge < 0
      - (objects['dimuons'].p4.mass > 60.0) & (objects['dimuons'].p4.mass < 120.0)
```
With `field` you define how to select the object, either through a NanoAOD field (`events.Muon`) or a custom object-selection function (`select_dimuons`) defined as a method of the [ObjectSelector](https://github.com/deoache/higgscharm/blob/lxplus/analysis/selections/object_selections.py) class. Each object is added sequentially to a dictionary called `objects`, which can later be used to access the already selected objects.

`cuts` defines the set of object-level cuts to apply. Similarly, you can use NanoAOD fields (`events.Muon.pt > 24`) to define a cut or any valid expression (`objects['dimuons'].z.mass < 120.0`). Alternatively, you can also use a working point function (`working_points.muon_iso(events, 'tight')`) defined in the [WorkingPoints class](https://github.com/deoache/higgscharm/blob/lxplus/analysis/working_points/working_points.py). 

You can also use `add_cut` to define masks that will be added to the object and can be accessed later in the workflow:

```yaml
muons:
    field: events.Muon
    add_cut:
      is_loose:
        - events.Muon.pt > 5
        - np.abs(events.Muon.eta) < 2.4
        - np.abs(events.Muon.dxy) < 0.5
        - np.abs(events.Muon.dz) < 1
        - events.Muon.isGlobal | (events.Muon.isTracker & (events.Muon.nStations > 0))
      is_relaxed:
        - objects['muons'].is_loose
        - np.abs(events.Muon.sip3d) < 4
      is_tight:
        - objects['muons'].is_loose
        - objects['muons'].is_relaxed
        - events.Muon.isPFcand | ((events.Muon.highPtId > 0) & (events.Muon.pt > 200))
zcandidates:
    field: select_zcandidates 
    add_cut:
      is_ossf:
        - objects['zcandidates'].l1.pdgId == -objects['zcandidates'].l2.pdgId
      is_ss:
        - objects['zcandidates'].l1.pdgId == objects['zcandidates'].l2.pdgId
      is_sr:
        - objects['zcandidates'].is_ossf
        - (1*objects['zcandidates'].l1.is_tight + 1*objects['zcandidates'].l2.is_tight) == 2
      is_sscr:
        - objects['zcandidates'].is_ss
        - objects['zcandidates'].l1.is_relaxed
        - objects['zcandidates'].l2.is_relaxed
```

* `event_selection`: Contains the HLT paths and event-level cuts:
```yaml
event_selection:
  hlt_paths:
    Muon:
      - SingleMu
  selections:
    trigger: get_trigger_mask(events, hlt_paths, dataset, year)
    lumimask: get_lumi_mask(events, year)
    atleast_one_goodvertex: events.PV.npvsGood > 0
    first_muon_pt: ak.firsts(objects['muons'].pt) > 27
    second_muon_pt: ak.pad_none(objects['muons'], target=2)[:, 1].pt > 15
    electron_veto: ak.num(objects['electrons']) == 0
    two_muons: ak.num(objects['muons']) == 2
    one_dimuon: ak.num(objects['dimuons']) == 1
  categories:
    base:
      - atleast_one_goodvertex
      - lumimask
      - trigger
      - first_muon_pt
      - second_muon_pt
      - electron_veto
      - two_muons
      - one_dimuon
```
First, you define which flag(s) to apply to a primary dataset PD with `hlt_paths` (all the flags will be apply to the MC samples in a logic OR). The available flags are defined in [`analysis/selections/trigger_flags.yaml`](https://github.com/deoache/higgscharm/blob/lxplus/analysis/selections/trigger_flags.yaml).  
Then, you define all event-level cuts in `selections`. Similarly to the object selection, you can use any valid expression from a NanoAOD field or a custom event-selection function defined in [`analysis/selections/event_selections.py`](https://github.com/deoache/higgscharm/blob/lxplus/analysis/selections/event_selections.py). Then, you can define one or more categories in `categories` by listing the cuts you want to include for each category. Histograms will be filled for each category.


* `corrections`: Contains the object-level corrections and event-level weights to apply:

```yaml
corrections:
  objects:
    - jets         # JEC
    - muons        # Muon scale and resolution
    - electrons    # Electron scale and resolution 
  event_weights:
    genWeight: true
    pileupWeight: true
    partonshowerWeight: true
    lhepdfWeight: true
    lhescaleWeight: true
    nnlopsWeight: true
    muon:
      - id: tight
      - iso: tight
      - trigger: true
    electron:
      - id: false
      - reco: false
      - trigger: false
```

You can find the logic used to managed these corrections [here](https://github.com/deoache/higgscharm/blob/lxplus/analysis/corrections/correction_manager.py).

* `histogram_config`: Use to define processor's output histograms (more info on Hist histograms [here](https://hist.readthedocs.io/en/latest/)). Here you define the histogram axes associated with the variables you want to include in the analysis. 
```yaml
histogram_config:
  add_syst_axis: true
  add_weight: true
  axes:
    muon_pt:
      type: Regular
      bins: 50
      start: 30
      stop: 300
      label: $p_T(\mu)$ [GeV]
      expression: objects['muons'].pt
    muon_eta:
      type: Regular
      bins: 50
      start: -2.5
      stop: 2.5
      label: $\eta(\mu)$
      expression: objects['muons'].eta
    muon_phi:
      type: Regular
      bins: 50
      start: -3.14159
      stop: 3.14159
      label: $\phi(\mu)$
      expression: objects['muons'].phi
    dimuon_mass:
      type: Regular
      bins: 100
      start: 10
      stop: 150
      label: $m(\mu\mu)$ [GeV]
      expression: objects['dimuons'].z.p4.mass
  layout:
    muon:
      - muon_pt
      - muon_eta
      - muon_phi
    zcandidate:
      - dimuon_mass
```
Note that the variable associated with the axis must be included through the `expression` field using the `objects` dictionary. Output histogram's layout is defined with the `layout` field. In the example above, our output dictionary will contain two histograms labelled `muon` and `zcandidate`, the first with the `muon_pt`, `muon_eta` and `muon_phi` axes, and the second only with the `dimuon_mass` axis (make sure to include axis with the same dimensions within a histogram). If you set `layout: individual` then the output dictionary will contain a histogram for each axis. Note that if you set `add_syst_axis: true`, a StrCategory axis `{"variable_name": {"type": "StrCategory", "categories": [], "growth": True}}` to store systematic variations will be added to each histogram.