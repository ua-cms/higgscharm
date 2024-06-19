import correctionlib
import awkward as ak
import dask_awkward as dak
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
            dataset year {2022EE}
        variation:
            if 'nominal' (default) add 'nominal', 'up' and 'down'
            variations to weights container. else, add only 'nominal' weights.
    """
    # define correction set and goldenJSON file names
    cset = correctionlib.CorrectionSet.from_file(
        get_pog_json(json_name="pileup", year=year)
    )
    year_to_corr = {
        "2022": "Collisions2022_355100_357900_eraBCD_GoldenJson",
        "2022EE": "Collisions2022_359022_362760_eraEFG_GoldenJson",
    }
    # get number of true interactions
    nti = events.Pileup.nTrueInt
    # get nominal scale factors
    nominal_sf = dak.map_partitions(cset[year_to_corr[year]].evaluate, nti, "nominal")
    if variation == "nominal":
        # get up and down variations
        up_sf = dak.map_partitions(cset[year_to_corr[year]].evaluate, nti, "up")
        down_sf = dak.map_partitions(cset[year_to_corr[year]].evaluate, nti, "down")
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