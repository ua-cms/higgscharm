import hist
import numpy as np
import hist.dask as hda


z_mass_axis = hist.axis.Regular(
    bins=100, start=10, stop=150, name="z_mass", label=r"$m(Z)$ [GeV]"
)
mu1_pt_axis = hist.axis.Regular(
    bins=50, start=0, stop=300, name="mu1_pt", label=r"$p_T(\mu_1)$ [GeV]"
)
mu2_pt_axis = hist.axis.Regular(
    bins=50, start=0, stop=300, name="mu2_pt", label=r"$p_T(\mu_2)$ [GeV]"
)
jet_pt_axis = hist.axis.Regular(
    bins=30, start=30, stop=150, name="cjet_pt", label=r"Jet $p_T$ [GeV]"
)
jet_eta_axis = hist.axis.Regular(
    bins=50, start=-2.5, stop=2.5, name="cjet_eta", label="Jet $\eta$"
)
jet_phi_axis = hist.axis.Regular(
    bins=50, start=-np.pi, stop=np.pi, name="cjet_phi", label="Jet $\phi$"
)
deltaphi_axis = hist.axis.Regular(
    bins=50,
    start=-np.pi,
    stop=np.pi,
    name="cjet_z_deltaphi",
    label="$\Delta\phi$(Jet, Z)",
)
n_jets_axis = hist.axis.IntCategory(categories=np.arange(0, 16), name="njets")
n_vertices_axis = hist.axis.Regular(bins=60, start=0, stop=60, name="npvs")
histograms = {
    "z_mass": hda.hist.Hist(z_mass_axis, hist.storage.Weight()),
    "mu1_pt": hda.hist.Hist(mu1_pt_axis, hist.storage.Weight()),
    "mu2_pt": hda.hist.Hist(mu2_pt_axis, hist.storage.Weight()),
    "cjet_pt": hda.hist.Hist(jet_pt_axis, hist.storage.Weight()),
    "cjet_eta": hda.hist.Hist(jet_eta_axis, hist.storage.Weight()),
    "cjet_phi": hda.hist.Hist(jet_phi_axis, hist.storage.Weight()),
    "cjet_z_deltaphi": hda.hist.Hist(deltaphi_axis, hist.storage.Weight()),
    "njets": hda.hist.Hist(n_jets_axis, hist.storage.Weight()),
    "npvs": hda.hist.Hist(n_vertices_axis, hist.storage.Weight()),
}