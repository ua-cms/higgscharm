import correctionlib
import awkward as ak
from typing import Type
from coffea.analysis_tools import Weights
from analysis.corrections.utils import get_pog_json


def add_pileup_weight(
    events,
    weights_container: Type[Weights],
    year: str,
    variation: str = "nominal",
) -> None:
    """
    add pileup weights to weights container

    Parameters:
    -----------
        events:
            Events array
        weights_container:
            Weight object from coffea.analysis_tools
        year:
            dataset year {2022preEE, 2022postEE, 2023preBPix, 2023postBPix}
        variation:
            if 'nominal' (default) add 'nominal', 'up' and 'down'
            variations to weights container. else, add only 'nominal' weights.
    """
    # define correction set and goldenJSON file names
    cset = correctionlib.CorrectionSet.from_file(
        get_pog_json(json_name="pileup", year=year)
    )
    year_to_corr = {
        "2022preEE": "Collisions2022_355100_357900_eraBCD_GoldenJson",
        "2022postEE": "Collisions2022_359022_362760_eraEFG_GoldenJson",
        "2023preBPix": "Collisions2023_366403_369802_eraBC_GoldenJson",
        "2023postBPix": "Collisions2023_369803_370790_eraD_GoldenJson",
    }
    # get number of true interactions
    nti = events.Pileup.nTrueInt
    # get nominal scale factors
    nominal_sf = cset[year_to_corr[year]].evaluate(ak.to_numpy(nti), "nominal")
    if variation == "nominal":
        # get up and down variations
        up_sf = cset[year_to_corr[year]].evaluate(ak.to_numpy(nti), "up")
        down_sf = cset[year_to_corr[year]].evaluate(ak.to_numpy(nti), "down")
        # add pileup scale factors to weights container
        weights_container.add(
            name="pileup",
            weight=nominal_sf,
            weightUp=up_sf,
            weightDown=down_sf,
        )
    else:
        weights_container.add(
            name="pileup",
            weight=nominal_sf,
        )
