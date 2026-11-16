import warnings
from urllib.parse import quote
import numpy as np
from analysis.corrections.zz_kfactor import lhe_four_lepton  # 2026-11-03: LHE m4l columns
import awkward as ak
from copy import deepcopy
from coffea import processor
from coffea.nanoevents import NanoAODSchema
from coffea.analysis_tools import PackedSelection
from coffea.nanoevents.methods.vector import LorentzVector
from analysis.utils import dump_lumi, dump_parquet, dump_sumw_json
from analysis.workflows.config import WorkflowConfigBuilder
from analysis.histograms import HistBuilder, fill_histograms
from analysis.corrections.correction_manager import (
    object_corrector_manager,
    weight_manager,
)
from analysis.selections import (
    ObjectSelector,
    get_lumi_mask,
    get_trigger_mask,
    get_zzto4l_trigger_mask,
    get_metfilters_mask,
    get_trigger_match_mask,
    get_stitching_mask,
)

NanoAODSchema.warn_missing_crossrefs = False

# 3FS/4FS stitching (LHCHWG-2026-007 sec. 4.4, added 2026-09-27): events are split
# by the number of GEN c-jets (GenJet hadronFlavour == 4, pT > 10 GeV, no eta cut).
# 0 c-jets are taken from 4FS FxFx, >= 1 from 3FS; 1 and >= 2 are stored separately
# so the "massless 1c" variant (0/1 from 4FS, >= 2 from 3FS) stays possible.
# See second-brain Notes/hczz-fs-stitching-recipe-2026-09-25.md.
GENC_PT_MIN = 10.0
GENC_BINS = ("0", "1", "2p")


def count_gen_cjets(events):
    """Per-event number of GEN c-jets (hadronFlavour == 4, pT > GENC_PT_MIN)."""
    genjets = events.GenJet
    is_c = (genjets.hadronFlavour == 4) & (genjets.pt > GENC_PT_MIN)
    return ak.to_numpy(ak.sum(is_c, axis=1))


# H+b 4FS/5FS stitching (same LHCHWG-2026-007 recipe, added 2026-11-16): 0 GEN b-jets (GenJet hadronFlavour == 5,
# pT > 10 GeV, no eta cut) from the 5FS FxFx H+b, >= 1 from the 4FS. Same bins and sidecar layout as the c-jets
# (sumw_genb{0,1,2p}, sumw_preveto_genb{0,1,2p}, + _scalevar) and the per-event column n_genb10. Purely additive.
def count_gen_bjets(events):
    """Per-event number of GEN b-jets (hadronFlavour == 5, pT > GENC_PT_MIN)."""
    genjets = events.GenJet
    is_b = (genjets.hadronFlavour == 5) & (genjets.pt > GENC_PT_MIN)
    return ak.to_numpy(ak.sum(is_b, axis=1))


# GEN c-hadron variables (added 2026-10-02) for the FS stitching by reweighting the 4FS
# FxFx sample with weights derived on the official H->gg 3FS/4FS samples (FS.pdf,
# T. Bevilacqua; c-hadron definition as the HWW FS study, C. Gupta). Unlike GenJets,
# c-hadrons cannot contain the Higgs decay products, so the weights transfer between decays.
# c-hadron: status 2, open charm (|pdgId| 4xx, 4xxx, 104xx, 204xx; charmonium excluded),
# weakly decaying (no c-hadron daughter), not from a b-hadron, |eta| < 2.5.
# See second-brain Notes/hczz-fs-3fs-gridpack-mismatch-2026-10-02.md.
CHAD_ETA_MAX = 2.5
CHAD_PT_THRESHOLDS = (5, 10)


def _is_open_charm_hadron(apid):
    openc = (
        ((apid > 400) & (apid < 500))
        | ((apid > 4000) & (apid < 5000))
        | ((apid > 10400) & (apid < 10500))
        | ((apid > 20400) & (apid < 20500))
    )
    return openc & (((apid // 10) % 100) != 44)


def _is_b_hadron(apid):
    return ((apid > 500) & (apid < 600)) | ((apid > 5000) & (apid < 6000))


def gen_chadron_info(events):
    """Leading GEN c-hadron pT (-1 if none) and the number of c-hadrons with
    pT > T for each T in CHAD_PT_THRESHOLDS."""
    gp = events.GenPart
    apid = abs(gp.pdgId)
    mom = gp.genPartIdxMother
    is_c = _is_open_charm_hadron(apid)
    idx = ak.local_index(apid)
    c_moms = mom[is_c]
    has_c_daughter = ak.any(idx[:, :, None] == c_moms[:, None, :], axis=-1)
    mom_pid = ak.fill_none(apid[ak.mask(mom, mom >= 0)], 0)
    sel = (
        is_c
        & (gp.status == 2)
        & ~has_c_daughter
        & ~_is_b_hadron(mom_pid)
        & (abs(gp.eta) < CHAD_ETA_MAX)
    )
    cpt = gp.pt[sel]
    out = {"gen_chad1_pt": ak.to_numpy(ak.fill_none(ak.max(cpt, axis=1), -1.0))}
    for t in CHAD_PT_THRESHOLDS:
        out[f"n_chad{t}"] = ak.to_numpy(ak.sum(cpt > t, axis=1))
    return out


def sumw_by_genc(events, n_genc, prefix="genc"):
    """
    Pre-selection sum of genWeight per GEN c-jet bin, nominal and multiplied by
    each of the 9 LHEScaleWeight entries (as the ratio to index 4, the same
    convention as analysis/corrections/lhescale.py). The stitching normalises
    each piece with these, and renormalises every scale variation separately.
    """
    genw = ak.to_numpy(events.genWeight)
    masks = {"0": n_genc == 0, "1": n_genc == 1, "2p": n_genc >= 2}
    out = {f"sumw_{prefix}{b}": float(genw[m].sum()) for b, m in masks.items()}

    lhe = events.LHEScaleWeight if "LHEScaleWeight" in events.fields else None
    if lhe is None or len(events) == 0 or not ak.all(ak.num(lhe) == 9):
        return out
    lhe = ak.to_numpy(ak.to_regular(lhe, axis=1))
    ratio = lhe / lhe[:, 4:5]
    for b, m in masks.items():
        out[f"sumw_{prefix}{b}_scalevar"] =(genw[m][:, None] * ratio[m]).sum(axis=0).tolist()
    return out


def update(events, collections):
    """Return a shallow copy of events array with some collections swapped out"""
    out = events
    for name, value in collections.items():
        out = ak.with_field(out, value, name)
    return out


class BaseProcessor(processor.ProcessorABC):
    def __init__(
        self,
        workflow: str,
        year: str,
        output_format: str,
        output_location: str,
        mode="virtual",
    ):
        assert mode in ["eager", "virtual", "dask"]
        self._mode = mode
        self.year = year
        self.workflow = workflow
        self.output_format = output_format
        self.output_location = output_location
        self.run = "2" if year.startswith("201") else "3"

        config_builder = WorkflowConfigBuilder(workflow)
        self.workflow_config = config_builder.build_workflow_config()
        self.histogram_config = self.workflow_config.histogram_config
        self.histograms = HistBuilder(self.workflow_config).build_histogram()
        # HcZZ workflows (corrections: objects: ...) keep the 2022/2023 production
        # bookkeeping: metadata "sumw" after the jet veto, sumw_preveto, GEN c-jet /
        # c-hadron splits, the per-chunk sumw sidecar and the n_genc10/chad columns
        self.hczz = "objects" in self.workflow_config.corrections_config

    def _chunk_key(self, events):
        """Per-chunk output name '<file uuid>_%2F<tree>%3B1_<start>-<stop>' (the uproot
        partition key of coffea 0.7). The job-level partition_key is the same for every
        chunk of a job, so it cannot name per-chunk parquet/sidecar files."""
        md = events.metadata
        if all(k in md for k in ("fileuuid", "treename", "entrystart", "entrystop")):
            uuid = md["fileuuid"]
            if isinstance(uuid, bytes):
                uuid = uuid.hex()
            tree = quote(f"/{md['treename'].strip('/').split(';')[0]};1", safe="")
            return f"{uuid}_{tree}_{md['entrystart']}-{md['entrystop']}"
        # coffea-2026: per-chunk partition key of the events factory (as parquet_writer.dump_parquet)
        return events.attrs["@events_factory"]._partition_key.replace("/", "_")

    def add_cutflow(self, events, output, selection_manager):
        for category, category_cuts in self.workflow_config.event_selection[
            "categories"
        ].items():
            output["metadata"].update({category: {"cutflow": {"initial": self.sumw}}})
            selections = []
            for cut_name in category_cuts:
                selections.append(cut_name)
                current_selection = selection_manager.all(*selections)
                if ak.sum(current_selection) != 0:
                    # events after the jet veto: index their own genWeight
                    sumw_cutflow = (
                        ak.sum(events.genWeight[current_selection])
                        if self.is_mc
                        else len(events[current_selection])
                    )
                    output["metadata"][category]["cutflow"][cut_name] = sumw_cutflow
                else:
                    output["metadata"][category]["cutflow"][cut_name] = 0

    def process(self, events):
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        np.seterr(divide="ignore", invalid="ignore")

        # identify data and simulation
        self.is_mc = hasattr(events, "genWeight")

        # compute sum of gen weights for normalization (before the jet veto)
        self.sumw = ak.sum(events.genWeight) if self.is_mc else len(events)

        sumw_extra = {}
        if self.hczz and self.is_mc:
            sumw_extra["sumw_preveto"] = float(self.sumw)
            # GEN c-jet split before the veto (3FS+4FS stitching); GenJets do not
            # depend on the veto
            if "GenJet" in events.fields:
                sumw_extra.update(
                    sumw_by_genc(events, count_gen_cjets(events), prefix="preveto_genc")
                )
                # same split by GEN b-jets for the H+b 4FS/5FS stitching (2026-11-16)
                sumw_extra.update(
                    sumw_by_genc(events, count_gen_bjets(events), prefix="preveto_genb")
                )

        # apply object corrections, jet veto and prepare systematic variations
        vetoed_events, shifts = object_corrector_manager(
            events=events,
            year=self.year,
            run=self.run,
            dataset=events.metadata["dataset"],
            workflow_config=self.workflow_config,
        )

        self.n_genc = None
        self.n_genb = None
        self.chad = None
        self.lhe4l = None
        if self.hczz:
            # HcZZ convention: metadata "sumw" (and the cutflow "initial") after the veto
            self.sumw = (
                ak.sum(vetoed_events.genWeight) if self.is_mc else len(vetoed_events)
            )
            if self.is_mc and "GenJet" in vetoed_events.fields:
                self.n_genc = count_gen_cjets(vetoed_events)
                sumw_extra.update(sumw_by_genc(vetoed_events, self.n_genc))
                self.n_genb = count_gen_bjets(vetoed_events)   # H+b 4FS/5FS stitching (2026-11-16)
                sumw_extra.update(sumw_by_genc(vetoed_events, self.n_genb, prefix="genb"))
            # LHE four-lepton mass, same-flavour flag and |cos theta*| for the ZZ k-factors (MC with LHEPart only)
            if self.is_mc and "LHEPart" in vetoed_events.fields:
                self.lhe4l = lhe_four_lepton(vetoed_events)
            if self.is_mc and "GenPart" in vetoed_events.fields:
                self.chad = gen_chadron_info(vetoed_events)
                for t in CHAD_PT_THRESHOLDS:
                    sumw_extra.update(
                        sumw_by_genc(
                            vetoed_events, self.chad[f"n_chad{t}"], prefix=f"chad{t}_"
                        )
                    )

        output = processor.accumulate(
            self.process_shift(update(vetoed_events, collections), shift)
            for collections, shift in shifts
        )

        if self.hczz and self.is_mc:
            output["metadata"].update(sumw_extra)
            if self.output_format == "parquet":
                # pre-selection sumw sidecar for every MC chunk (also when no event is
                # selected): the datacard builders read <dataset>/sumw/*.json. Written
                # last, so a chunk that crashes part-way leaves no sidecar.
                dataset = events.metadata["dataset"]
                dump_sumw_json(
                    float(self.sumw),
                    dataset,
                    self._chunk_key(events) + ".json",
                    self.output_location,
                    [self.workflow, self.year, dataset, "sumw"],
                    extra=sumw_extra,
                )
        return output

    def process_shift(self, events, shift):
        # define variables required by workflow expressions evaluated with eval()
        year = self.year
        dataset = events.metadata["dataset"]
        hlt_paths = self.workflow_config.event_selection["hlt_paths"]

        # initialize output metadata
        output = {}
        output["metadata"] = {}
        if shift is None:
            # store the initial sum of weights before object selection
            output["metadata"].update({"sumw": self.sumw})

        if not self.is_mc:
            events["Jet", "hadronFlavour"] = ak.zeros_like(events.Jet.pt)

        # object selection
        object_selection = self.workflow_config.object_selection
        object_selector = ObjectSelector(object_selection, year)
        objects = object_selector.select_objects(events)

        # event selection
        selection_manager = PackedSelection()
        event_selection = self.workflow_config.event_selection
        for selection, mask in event_selection["selections"].items():
            selection_manager.add(selection, eval(mask))

        # lumi sections and cutflow from the nominal pass only (identical across
        # shifts; accumulating them over shifts would count them once per shift)
        if shift is None:
            if not self.is_mc:
                # save (run, luminosityBlock) pairs to metadata
                lumi_mask = eval(event_selection["selections"]["lumimask"])
                dump_lumi(events[lumi_mask], output)
            self.add_cutflow(events, output, selection_manager)

        # Histogram filling / array dumping
        histograms = deepcopy(self.histograms)
        categories = event_selection["categories"]
        for category, category_cuts in categories.items():
            # get selection mask by category
            category_mask = selection_manager.all(*category_cuts)
            nevents_after = ak.sum(category_mask)
            if nevents_after > 0:
                # get pruned events
                pruned_ev = events[category_mask]
                # add each selected object to 'pruned_ev' as a new field
                for obj in objects:
                    pruned_ev[f"selected_{obj}"] = objects[obj][category_mask]
                # get weights container
                weights_container = weight_manager(
                    pruned_ev=pruned_ev,
                    year=year,
                    dataset=dataset,
                    workflow_config=self.workflow_config,
                    category=category,
                    shift=shift,
                )
                if self.hczz and shift is None:
                    # events written per category (bookkeeping job check)
                    output["metadata"][category].update(
                        {
                            "weighted_final_nevents": ak.sum(weights_container.weight()),
                            "raw_final_nevents": nevents_after,
                        }
                    )
                # histograms: only the variables of the layout; parquet: every axis
                variables_map = {}
                layout = self.histogram_config.layout
                if layout != "individual":
                    layout_axes = {
                        variable
                        for variables in layout.values()
                        for variable in variables
                    }
                for variable, axis in self.histogram_config.axes.items():
                    # parquet output keeps every axis (MVA/datacard inputs); histograms only the layout's
                    if self.output_format != "parquet" and layout != "individual" and variable not in layout_axes:
                        continue
                    variables_map[variable] = eval(axis.expression)[category_mask]

                # save variables
                if self.output_format == "coffea":
                    fill_histograms(
                        histogram_config=self.histogram_config,
                        weights_container=weights_container,
                        variables_map=variables_map,
                        histograms=histograms,
                        category=category,
                        is_mc=self.is_mc,
                        shift=shift,
                        flow=True,
                    )
                elif self.output_format == "parquet":
                    mask_np = ak.to_numpy(category_mask)
                    if self.n_genc is not None:
                        variables_map["n_genc10"] = self.n_genc[mask_np]
                    if self.n_genb is not None:
                        variables_map["n_genb10"] = self.n_genb[mask_np]   # H+b stitching (2026-11-16)
                    if self.chad is not None:
                        for key, val in self.chad.items():
                            variables_map[key] = val[mask_np]
                    if self.lhe4l is not None:
                        variables_map["lhe_m4l"] = self.lhe4l[0][mask_np]
                        variables_map["lhe_4l_sameflavour"] = self.lhe4l[1][mask_np]
                        variables_map["lhe_abscosthetastar"] = self.lhe4l[2][mask_np]  # -1 if no OSSF pairing
                    dump_parquet(
                        events=events,
                        weights_container=weights_container,
                        variables_map=variables_map,
                        workflow=self.workflow,
                        year=year,
                        category=category,
                        output_location=self.output_location,
                        shift=shift,
                    )
        # add histograms to output dictionary
        if self.output_format == "coffea":
            output["histograms"] = histograms
        return output

    def postprocess(self, accumulator):
        pass
