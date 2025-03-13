# taken from https://gitlab.cern.ch/cms-analysis/general/HiggsDNA/-/blob/master/higgs_dna/systematics/event_weight_systematics.py?ref_type=heads#L719
import numpy as np
def add_partonshower_weight(events, weights_container):
    """
    Parton Shower weights:
    https://github.com/cms-sw/cmssw/blob/caeae4110ddbada1cfdac195404b3c618584e8fb/PhysicsTools/NanoAOD/plugins/GenWeightsTableProducer.cc#L533-L534
    """
    try:
        weights_container.add(
            name="ps_isr",
            weight=np.ones(len(events)),
            weightUp=events.PSWeight[:, 0],
            weightDown=events.PSWeight[:, 2],
        )

        weights_container.add(
            name="ps_fsr",
            weight=np.ones(len(events)),
            weightUp=events.PSWeight[:, 1],
            weightDown=events.PSWeight[:, 3],
        )
    except:
        print("No PS Weights in dataset, skip systematic: PartonShower weight")