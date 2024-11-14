class WorkingPoints:
    
    def jet_id(self, events, wp):
        wps = {
            "tightlepveto": events.Jet.jetId == 6,
            "tight": events.Jet.jetId == 2,
        }
        return wps[wp]

    def electron_id(self, events, wp):
        wps = {
            "wp80iso": events.Electron.mvaIso_WP80,
            "wp90iso": events.Electron.mvaIso_WP90,
            "wp80noiso": events.Electron.mvaNoIso_WP80,
            "wp90noiso": events.Electron.mvaNoIso_WP90,
        }
        return wps[wp]

    def electron_iso(self, events, wp):
        wps = {
            "loose": (
                events.Electron.pfRelIso04_all < 0.25
                if hasattr(events.Electron, "pfRelIso04_all")
                else events.Electron.pfRelIso03_all < 0.25
            ),
            "medium": (
                events.Electron.pfRelIso04_all < 0.20
                if hasattr(events.Electron, "pfRelIso04_all")
                else events.Electron.pfRelIso03_all < 0.20
            ),
            "tight": (
                events.Electron.pfRelIso04_all < 0.15
                if hasattr(events.Electron, "pfRelIso04_all")
                else events.Electron.pfRelIso03_all < 0.15
            ),
        }
        return wps[wp]

    def muon_id(self, events, wp):
        muons_id_wps = {
            "loose": events.Muon.looseId,
            "medium": events.Muon.mediumId,
            "tight": events.Muon.tightId,
        }
        return muons_id_wps[wp]

    def muon_iso(self, events, wp):
        wps = {
            "loose": (
                events.Muon.pfRelIso04_all < 0.25
                if hasattr(events.Muon, "pfRelIso04_all")
                else events.Muon.pfRelIso03_all < 0.25
            ),
            "medium": (
                events.Muon.pfRelIso04_all < 0.20
                if hasattr(events.Muon, "pfRelIso04_all")
                else events.Muon.pfRelIso03_all < 0.20
            ),
            "tight": (
                events.Muon.pfRelIso04_all < 0.15
                if hasattr(events.Muon, "pfRelIso04_all")
                else events.Muon.pfRelIso03_all < 0.15
            ),
        }
        return wps[wp]