### Processor config

Processor's selection, variables, output histograms, triggers, among other features are defined through a configuration file:

* `goldenjson`: Path to the goldenjson of the specific year, used to compute the luminosity mask (for luminosity calibration)
* `lumidata`: Luminosity file (see [here](https://github.com/deoache/higgscharm/blob/dask/analysis/data/README.md)) used to compute the integrated luminosity using [Coffea's lumi tools](https://coffeateam.github.io/coffea/modules/coffea.lumi_tools.html).
* `hlt_paths`: List of HLT paths 
* `object_selection`: Dictionary containing the information required for object selection. It has the following structure:
```
object_selection={
    "muons": {
        "expression": "events.Muon",
        "cuts": {
            "pt": "events.Muon.pt > 24",
            "abseta": "np.abs(events.Muon.eta) < 2.4",
            "dxy": "events.Muon.dxy < 0.5",
            "dz": "events.Muon.dz < 1",
            "sip3d": "events.Muon.sip3d < 4",
            "muon_id": "tight",
            "muon_iso": "tight",
        },
    },
    "dimuons": {
        "expression": "select_dileptons(objects['muons'])",
        "cuts": {
            "dr": "((LorentzVector.delta_r(objects['dimuons'].z.leading_lepton, objects['dimuons'].z.subleading_lepton) > 0.02))",
            "mass_window": "(objects['dimuons'].z.p4.mass < 120.0) & (objects['dimuons'].z.p4.mass > 60.0)",
        },
    },
}
```
`expression` is a string used to define how to select the object. It can be a NanoAOD field or a custom object-selection function defined in [`analysis/selections/selections.py`](https://github.com/deoache/higgscharm/blob/dask/analysis/selections/selections.py). Under the hood, each object is added sequentially to a dictionary called `objects`, which can later be used to access the already selected objects. Note that this is exactly what we do here: First, `muons` is selected as `events.Muon` (and added to `objects`), and subsequently we select `dimuons` using `objects['muons']` as `select_dileptons(objects['muons'])`.

`cuts` defines the set of object-level cuts to apply. You can use NanoAOD fields (`events.Muon.pt > 24`) to define a cut or any valid expression (`objects['dimuons'].z.p4.mass < 120.0`). Alternatively, you can also use a working point function (WPF) defined in the [WorkingPoints class](https://github.com/deoache/higgscharm/blob/dask/analysis/working_points/working_points.py). For instance, given the WPF
```
def muon_id(self, events, wp):
    muons_id_wps = {
        "loose": events.Muon.looseId,
        "medium": events.Muon.mediumId,
        "tight": events.Muon.tightId,
    }
    return muons_id_wps[wp]
```
you can use the item `"muon_id": "tight"` in the `cuts` dictionary to include the `events.Muon.tightId` cut.

* `event_selection`: Here you define a dictionary with the event-level cuts you want to apply. Similarly to the object selection, you can use any valid expression from a NanoAOD field or a custom event-selection function defined in [`analysis/selections/selections.py`](https://github.com/deoache/higgscharm/blob/dask/analysis/selections/selections.py).

* `histogram_config`: [Histogram configuration file](https://github.com/deoache/higgscharm/blob/dask/analysis/configs/histogram_config.py) use to define processor's output histograms (more info on Hist histograms [here](https://hist.readthedocs.io/en/latest/)). Here you define the histogram axes associated with the variables you want to include in the analysis. The possible axes types that can be included are: 

    **Regular**: The number of bins, lower and upper limits are defined through `bins`, `start`, and `stop`, respectively:
    ```
    "muon_eta": {
        "type": "Regular",
        "bins": 50,
        "start": -2.4,
        "stop": 2.4,
        "label": "$\eta(\mu)$",
        "expression": "objects['muons'].eta"
    }
    ```
    **Variable**: The variable bin size is defined through `edges`:
    ```
    "muon_pt": {
        "type": "Variable",
        "edges": [30, 60, 90, 120, 150, 180, 210, 240, 300, 500],
        "label": r"$p_T(\mu)$ [GeV]",
        "expression": "objects['muons'].pt"
    }
    ```
    **IntCategory**: Axis categories are defined through `categories`
    ```
    "jet_flavor": {
        "type": "IntCategory",
        "categories": [],
        "growth": True,
        "label": "Jet Hadron Flavor",
        "expression": "objects['jets'].hadronFlavour"
    }
    ```
Note that the variable associated with the axis must be included through the `expression` field using the `objects` dictionary.

Output histogram's layout is defined with the `layout` field. If you set `layout="individual"` then you will get an individual histogram for each axis. Alternatively, you can include multiple axes in a histogram using a dictionary whose values are lists containing the names of the axes to be included in the histogram:

**Individual axes**
```
layout="individual"
```
```
{'muon_eta': Hist(Regular(50, -2.4, 2.4, name='muon_eta', label='$\\eta(\\mu)$'), storage=Weight()) # Sum: WeightedSum(value=0, variance=0),
 'muon_pt': Hist(Variable([30, 60, 90, 120, 150, 180, 210, 240, 300, 500], name='muon_pt', label='$p_T(\\mu)$ [GeV]'), storage=Weight()) # Sum: WeightedSum(value=0, variance=0),
 'jet_flavor': Hist(IntCategory([], growth=True, name='jet_flavor', label='Jet Hadron Flavor'), storage=Weight()) # Sum: WeightedSum(value=0, variance=0)}
```
**Stacked axes**
```
layout = {
    "muon": ["muon_pt", "muon_eta"],
    "jet": ["jet_flavor"]
}
```
```
{'muon': Hist(
   Regular(50, -2.4, 2.4, name='muon_eta', label='$\\eta(\\mu)$'),
   Variable([30, 60, 90, 120, 150, 180, 210, 240, 300, 500], name='muon_pt', label='$p_T(\\mu)$ [GeV]'), storage=Weight()) # Sum: WeightedSum(value=0, variance=0),
'jet': Hist(IntCategory([], growth=True, name='jet_flavor', label='Jet Hadron Flavor'), storage=Weight()) # Sum: WeightedSum(value=0, variance=0)}
```
    
    
Note that if you set `add_syst_axis=True`, a StrCategory axis `{"variable_name": {"type": "StrCategory", "categories": [], "growth": True}}` to store systematic variations will be added to each histogram.