### Processor config

Processor's selection, variables, output histograms, triggers, among other features are defined through a yaml configuration file:

* `goldenjson`: Path to the goldenjson of the specific year, used to compute the luminosity mask (for luminosity calibration)
* `lumidata`: Luminosity file (see [here](https://github.com/deoache/higgscharm/blob/coffea0.7/analysis/data/README.md) used to compute the integrated luminosity using [Coffea's lumi tools](https://coffeateam.github.io/coffea/modules/coffea.lumi_tools.html).
* `hlt_paths`: List of HLT paths 
```yaml
goldenjson: analysis/data/Cert_Collisions2022_355100_362760_Golden.txt
lumidata: analysis/data/lumi2022.csv
hlt_paths:
  - IsoMu24
```
* `object_selection`: Contains the information required for object selection:
```yaml
object_selection:
  muons:
    field: events.Muon
    cuts:
      pt: events.Muon.pt > 27
      abseta: np.abs(events.Muon.eta) < 2.4
      dxy: events.Muon.dxy < 0.5
      dz: events.Muon.dz < 1
      sip3d: events.Muon.sip3d < 4
      muon_id: tight
      muon_iso: tight
  dimuons:
    field: select_dimuons
    cuts:
      dr: LorentzVector.delta_r(objects['dimuons'].l1, objects['dimuons'].l2) > 0.02
      opp_charge: objects['dimuons'].l1.charge * objects['dimuons'].l2.charge < 0
      mass_window: (objects['dimuons'].z.mass > 60.0) & (objects['dimuons'].z.mass < 120.0)
```
With `field` you define how to select the object, either through a NanoAOD field or a custom object-selection function defined as a method of the [ObjectSelector](https://github.com/deoache/higgscharm/blob/coffea0.7/analysis/selections/object_selections.py) class (as done for `select_dimuons`). Each object is added sequentially to a dictionary called `objects`, which can later be used to access the already selected objects.

`cuts` defines the set of object-level cuts to apply. Similarly, you can use NanoAOD fields (`events.Muon.pt > 24`) to define a cut or any valid expression (`objects['dimuons'].z.mass < 120.0`). Alternatively, you can also use a working point function (WPF) defined in the [WorkingPoints class](https://github.com/deoache/higgscharm/blob/coffea0.7/analysis/working_points/working_points.py). For instance, given the WPF
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
    atleast_one_goodvertex: events.PV.npvsGood > 0
    lumimask: get_lumi_mask(events, goldenjson)
    trigger: get_trigger_mask(events, hlt_paths)
    trigger_matching: get_trigger_match_mask(events, events.Muon, hlt_paths)
    two_muons: ak.num(objects['muons']) == 2
    one_z: ak.num(objects['dimuons'].z) == 1
  categories:
    base:
      - atleast_one_goodvertex
      - lumimask
      - trigger
      - trigger_matching
      - two_muons
      - one_z
```
First, you define all event-wise cuts in `selections`. Similarly to the object selection, you can use any valid expression from a NanoAOD field or a custom event-selection function defined in [`analysis/selections/event_selections.py`](https://github.com/deoache/higgscharm/blob/coffea0.7/analysis/selections/event_selections.py). Then, you can define one or more categories in `categories` by listing the cuts you want to include for each category. Histograms will be filled for each category.
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
Note that the variable associated with the axis must be included through the `expression` field using the `objects` dictionary. Output histogram's layout is defined with the `layout` field. In the example above, our output dictionary will contain two histograms labelled `muon` and `zcandidate`, the first with the `muon_pt`, `muon_eta` and `muon_phi` axes, and the second with the `dimuon_mass` axis only (make sure to include axis with the same dimensions within a histogram). If you set `layout: individual` then the output dictionary will contain a histogram for each axis. Note that if you set `add_syst_axis: true`, a StrCategory axis `{"variable_name": {"type": "StrCategory", "categories": [], "growth": True}}` to store systematic variations will be added to each histogram.