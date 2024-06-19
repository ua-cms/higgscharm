class WorkingPoints:

    def electron_id(self, electrons, wp):
        electrons_id_wps = {
            "wpiso80": electrons.mvaIso_WP80,
            "wpiso90": electrons.mvaIso_WP90,
            "wpnoiso80": electrons.mvaNoIso_WP80,
            "wpnoiso90": electrons.mvaNoIso_WP90,
        }
        return electrons_id_wps[wp]

    def electron_iso(self, electrons, wp):
        electrons_iso_wps = {
            "loose": (
                electrons.pfRelIso04_all < 0.25
                if hasattr(electrons, "pfRelIso04_all")
                else electrons.pfRelIso03_all < 0.25
            ),
            "medium": (
                electrons.pfRelIso04_all < 0.20
                if hasattr(electrons, "pfRelIso04_all")
                else electrons.pfRelIso03_all < 0.20
            ),
            "tight": (
                electrons.pfRelIso04_all < 0.15
                if hasattr(electrons, "pfRelIso04_all")
                else electrons.pfRelIso03_all < 0.15
            ),
        }
        return electrons_iso_wps[wp]

    def muon_id(self, muons, wp):
        muons_id_wps = {
            "loose": muons.looseId,
            "medium": muons.mediumId,
            "tight": muons.tightId,
        }
        return muons_id_wps[wp]

    def muon_iso(self, muons, wp):
        muons_iso_wps = {
            "loose": (
                muons.pfRelIso04_all < 0.25
                if hasattr(muons, "pfRelIso04_all")
                else muons.pfRelIso03_all < 0.25
            ),
            "medium": (
                muons.pfRelIso04_all < 0.20
                if hasattr(muons, "pfRelIso04_all")
                else muons.pfRelIso03_all < 0.20
            ),
            "tight": (
                muons.pfRelIso04_all < 0.15
                if hasattr(muons, "pfRelIso04_all")
                else muons.pfRelIso03_all < 0.15
            ),
        }
        return muons_iso_wps[wp]

    def jet_tagger(self, jets, flavor, tagger, wp, year):
        tagging_working_points = {
            # https://indico.cern.ch/event/1304360/contributions/5518916/attachments/2692786/4673101/230731_BTV.pdf
            "c": {
                "2022": {
                    "deepjet": {
                        "loose": (jets.btagDeepFlavCvB > 0.208)
                        & (jets.btagDeepFlavCvL > 0.042),
                        "medium": (jets.btagDeepFlavCvB > 0.299)
                        & (jets.btagDeepFlavCvL > 0.108),
                        "tight": (jets.btagDeepFlavCvB > 0.243)
                        & (jets.btagDeepFlavCvL > 0.303),
                    },
                    "pnet": {
                        "loose": (jets.btagPNetCvB > 0.181)
                        & (jets.btagPNetCvL > 0.054),
                        "medium": (jets.btagPNetCvB > 0.306)
                        & (jets.btagPNetCvL > 0.160),
                        "tight": (jets.btagPNetCvB > 0.259)
                        & (jets.btagPNetCvL > 0.492),
                    },
                    "part": {
                        "loose": (jets.btagPNetCvB > 0.068)
                        & (jets.btagPNetCvL > 0.039),
                        "medium": (jets.btagPNetCvB > 0.130)
                        & (jets.btagPNetCvL > 0.117),
                        "tight": (jets.btagPNetCvB > 0.095)
                        & (jets.btagPNetCvL > 0.360),
                    },
                },
                "2022EE": {
                    "deepjet": {
                        "loose": (jets.btagDeepFlavCvB > 0.206)
                        & (jets.btagDeepFlavCvL > 0.042),
                        "medium": (jets.btagDeepFlavCvB > 0.298)
                        & (jets.btagDeepFlavCvL > 0.108),
                        "tight": (jets.btagDeepFlavCvB > 0.241)
                        & (jets.btagDeepFlavCvL > 0.305),
                    },
                    "pnet": {
                        "loose": (jets.btagPNetCvB > 0.182)
                        & (jets.btagPNetCvL > 0.054),
                        "medium": (jets.btagPNetCvB > 0.304)
                        & (jets.btagPNetCvL > 0.160),
                        "tight": (jets.btagPNetCvB > 0.258)
                        & (jets.btagPNetCvL > 0.491),
                    },
                    "part": {
                        "loose": (jets.btagRobustParTAK4CvB > 0.067)
                        & (jets.btagRobustParTAK4CvL > 0.0390),
                        "medium": (jets.btagRobustParTAK4CvB > 0.128)
                        & (jets.btagRobustParTAK4CvL > 0.117),
                        "tight": (jets.btagRobustParTAK4CvB > 0.095)
                        & (jets.btagRobustParTAK4CvL > 0.358),
                    },
                },
            },
            # https://indico.cern.ch/event/1304360/contributions/5518915/attachments/2692528/4678901/BTagPerf_230808_Summer22WPs.pdf
            "b": {
                "2022": {
                    "deepjet": {
                        "loose": jets.btagDeepFlavB > 0.0583,
                        "medium": jets.btagDeepFlavB > 0.3086,
                        "tight": jets.btagDeepFlavB > 0.7183,
                    },
                    "pnet": {
                        "loose": jets.btagDeepFlavB > 0.047,
                        "medium": jets.btagDeepFlavB > 0.245,
                        "tight": jets.btagDeepFlavB > 0.6734,
                    },
                    "part": {
                        "loose": jets.btagDeepFlavB > 0.0849,
                        "medium": jets.btagDeepFlavB > 0.4319,
                        "tight": jets.btagDeepFlavB > 0.8482,
                    },
                },
                "2022EE": {
                    "deepjet": {
                        "loose": jets.btagDeepFlavB > 0.0614,
                        "medium": jets.btagDeepFlavB > 0.3196,
                        "tight": jets.btagDeepFlavB > 0.73,
                    },
                    "pnet": {
                        "loose": jets.btagDeepFlavB > 0.0499,
                        "medium": jets.btagDeepFlavB > 0.2605,
                        "tight": jets.btagDeepFlavB > 0.6915,
                    },
                    "part": {
                        "loose": jets.btagDeepFlavB > 0.0897,
                        "medium": jets.btagDeepFlavB > 0.451,
                        "tight": jets.btagDeepFlavB > 0.8604,
                    },
                },
            },
        }
        return tagging_working_points[flavor][year][tagger][wp]