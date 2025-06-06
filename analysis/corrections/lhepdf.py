# taken from https://gitlab.cern.ch/cms-analysis/general/HiggsDNA/-/blob/master/higgs_dna/systematics/event_weight_systematics.py?ref_type=heads#L696
import numpy as np
def add_lhepdf_weight(events, weights_container):
    """
    AlphaS weights variations are the last two of the PDF replicas, e.g.,
    https://github.com/cms-sw/cmssw/blob/d37d2797dffc978a78da2fafec3ba480071a0e67/PhysicsTools/NanoAOD/python/genWeightsTable_cfi.py#L10
    https://lhapdfsets.web.cern.ch/current/NNPDF31_nnlo_as_0118_mc_hessian_pdfas/NNPDF31_nnlo_as_0118_mc_hessian_pdfas.info
    """
    try:
        weights.add(
            name="lhe",
            weight=np.ones(len(events)),
            weightUp=events.LHEPdfWeight[:, -1],
            weightDown=events.LHEPdfWeight[:, -2],
        )
    except:
        print("No LHEPdf Weights in dataset, skip systematic: AlphaS Weight")