### Processor config

Processor's selection, variables, output histograms, triggers, among other features are defined through a yaml configuration file:

* `goldenjson`: Path to the goldenjson of the specific year, used to compute the luminosity mask (for luminosity calibration)
```yaml
goldenjson: analysis/data/Cert_Collisions2022_355100_362760_Golden.txt
```
* `lumidata`: Luminosity file [(see here)](https://github.com/deoache/higgscharm/blob/coffea0.7/analysis/data/README.md) used to compute the integrated luminosity using [Coffea's lumi tools](https://coffeateam.github.io/coffea/modules/coffea.lumi_tools.html).
```yaml
lumidata: analysis/data/lumi2022.csv
```
* `hlt_paths`: Trigger flags to apply. Here you define which flag(s) to apply to a PD (all the flags will be apply to the MC samples in a logic OR). The available flags are defined in [`analysis/selections/trigger_flags.yaml`](https://github.com/deoache/higgscharm/blob/lxplus/analysis/selections/trigger_flags.yaml). 
```yaml
hlt_paths:
  Muon: 
    - SingleMu
```
* `object_selection`: Contains the information required for object selection:
```yaml
object_selection:
  muons:
    field: events.Muon
    cuts:
      pt: events.Muon.pt > 10
      abseta: np.abs(events.Muon.eta) < 2.4
      dxy: np.abs(events.Muon.dxy) < 0.5
      dz: np.abs(events.Muon.dz) < 1
      sip3d: np.abs(events.Muon.sip3d) < 4
      muon_id: tight
      muon_iso: tight
  jets:
    field: events.Jet
    cuts:
      min_pt: events.Jet.pt > 20
      abseta: np.abs(events.Jet.eta) < 2.5
      jet_id: tightlepveto
      jet_muon_dr: delta_r_higher(events.Jet, objects['muons'], 0.4)
      jet_electron_dr: delta_r_higher(events.Jet, objects['electrons'], 0.4)
  dimuons:
    field: select_dimuons
    cuts:
      dr: LorentzVector.delta_r(objects['dimuons'].l1, objects['dimuons'].l2) > 0.02
      opp_charge: objects['dimuons'].l1.charge * objects['dimuons'].l2.charge < 0
      mass_window: (objects['dimuons'].z.mass > 60.0) & (objects['dimuons'].z.mass < 120.0)
```
With `field` you define how to select the object, either through a NanoAOD field (`events.Muon`) or a custom object-selection function (`select_dimuons`) defined as a method of the [ObjectSelector](https://github.com/deoache/higgscharm/blob/lxplus/analysis/selections/object_selections.py) class. Each object is added sequentially to a dictionary called `objects`, which can later be used to access the already selected objects.

`cuts` defines the set of object-level cuts to apply. Similarly, you can use NanoAOD fields (`events.Muon.pt > 24`) to define a cut or any valid expression (`objects['dimuons'].z.mass < 120.0`). Alternatively, you can also use a working point function (WPF) defined in the [WorkingPoints class](https://github.com/deoache/higgscharm/tree/lxplus/analysis/working_points). For instance, given the WPF
```
def muon_id(self, events, wp):
    muons_id_wps = {
        "loose": events.Muon.looseId,
        "medium": events.Muon.mediumId,
        "tight": events.Muon.tightId,
    }
    return muons_id_wps[wp]
```
you can use the item `muon_id: tight` in `cuts` to include the `events.Muon.tightId` cut.

* `event_selection`: Here you define the event-level cuts you want to apply
```yaml
event_selection:
  selections:
    trigger: get_trigger_mask(events, hlt_paths, dataset)
    atleast_one_goodvertex: events.PV.npvsGood > 0
    lumimask: get_lumi_mask(events, goldenjson)
    first_muon_pt: ak.firsts(objects['muons'].pt) > 27
    second_muon_pt: ak.pad_none(objects['muons'], target=2)[:, 1].pt > 15
    electron_veto: ak.num(objects['electrons']) == 0
    two_muons: ak.num(objects['muons']) == 2
    one_z: ak.num(objects['dimuons'].z) == 1
  categories:
    base:
      - atleast_one_goodvertex
      - lumimask
      - trigger
      - first_muon_pt
      - second_muon_pt
      - electron_veto
      - two_muons
      - one_z
```
First, you define all event-level cuts in `selections`. Similarly to the object selection, you can use any valid expression from a NanoAOD field or a custom event-selection function defined in [`analysis/selections/event_selections.py`](https://github.com/deoache/higgscharm/blob/coffea0.7/analysis/selections/event_selections.py). Then, you can define one or more categories in `categories` by listing the cuts you want to include for each category. Histograms will be filled for each category.

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