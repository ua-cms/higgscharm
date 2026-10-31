"""HcZZ extra classifier inputs (2026-10-28): H->ZZ->4l decay angles and 4l+jet kinematics, all dimensionless.

ONE implementation shared by the training (b-hive: utils/hczz_extra_features.py, used by LZ4HcZZProcessor) and the
analysis scoring (higgscharm: analysis/postprocess/hczz_extra_features.py, used by mva_inference / Z+X inference).
The two copies must stay byte-identical (check with md5sum).

Inputs are plain numpy arrays, one entry per event:
  lep_pt, lep_eta, lep_phi, lep_mass, lep_charge : shape (n, 4), order [Z1 l1, Z1 l2, Z2 l1, Z2 l2]
  pt4l, eta4l, phi4l                              : shape (n,)   (absolute pT_4l, not divided by m4l)
  j_pt, j_eta, j_phi                              : shape (n,)   leading jet (pt <= 0 or NaN = no jet)
  cj_pt, cj_eta, cj_phi                           : shape (n,)   leading c-tagged jet (same convention)

Decay angles (JHUGen/MELA conventions, Gao et al. PRD 81 (2010) 075022): in the 4l rest frame, with the negative
lepton of each Z as its first daughter (charge-ordered, so the result does not depend on the stored lepton order);
the beam axis is the lab z axis boosted into the 4l frame.
  cos_theta_star : angle of Z1 to the beam axis
  cos_theta1/2   : angle of the negative lepton of Z1 (Z2) to the direction opposite Z2 (Z1), in the Z1 (Z2) frame
  phi            : angle between the two Z decay planes
  phi1           : angle between the Z1 decay plane and the (beam, Z1) plane
4l+jet kinematics (j = leading jet, cj = leading c jet), defined without m4l:
  dphi_X_4l = |dphi(X, 4l)|, deta_X_4l = |eta_X - eta_4l|,
  ptbal_X_4l = |pT(4l) + pT(X)| / (pT_4l + pT_X)   (vector sum; 0 = back-to-back, balanced)
  ptfrac_X_4l = pT_X / (pT_X + pT_4l)
  2026-10-29: ptbal/ptfrac LEAK m4l when the model also sees raw jet pT and pT_4l/m4l (m4l = pT_j(1-f)/f / (pT_4l/m4l),
  exact). Mass-normalised versions (needs m4l): ptbalm_X_4l / ptfracm_X_4l = the same with pT_4l -> M_REF * pT_4l/m4l
  (M_REF = 125 GeV only sets the scale). They are functions of pT_X, pT_4l/m4l and dphi(X, 4l) only, so they add no m4l
  information beyond those inputs (second-brain Notes/hczz-mva-new-inputs-fs-training-2026-10-28.md 12.5).
Events without the jet get NOJET for its features.
"""
import numpy as np

NOJET = -1.0
ANGLE_FEATURES = ["ang_cos_theta_star", "ang_cos_theta1", "ang_cos_theta2", "ang_phi", "ang_phi1"]
JET_FEATURES = ["kin_dphi_j_4l", "kin_deta_j_4l", "kin_ptbal_j_4l", "kin_ptfrac_j_4l",
                "kin_deta_cj_4l", "kin_ptbal_cj_4l", "kin_ptfrac_cj_4l"]
MNORM_FEATURES = ["kin_ptbalm_j_4l", "kin_ptfracm_j_4l", "kin_ptbalm_cj_4l", "kin_ptfracm_cj_4l"]  # need m4l
M_REF = 125.0
ALL_FEATURES = ANGLE_FEATURES + JET_FEATURES + MNORM_FEATURES


def _p4(pt, eta, phi, m):
    px, py, pz = pt * np.cos(phi), pt * np.sin(phi), pt * np.sinh(eta)
    e = np.sqrt(px ** 2 + py ** 2 + pz ** 2 + m ** 2)
    return np.stack([e, px, py, pz], axis=-1)


def _boost(p, b):
    """boost four-vectors p (n,4) by velocity b (n,3) (into the frame moving with -b... i.e. rest frame of b)"""
    b2 = np.sum(b * b, axis=-1)
    b2 = np.clip(b2, 0.0, 1.0 - 1e-12)
    g = 1.0 / np.sqrt(1.0 - b2)
    bp = np.sum(b * p[:, 1:], axis=-1)
    g2 = np.where(b2 > 0, (g - 1.0) / np.where(b2 > 0, b2, 1.0), 0.0)
    sp = p[:, 1:] + (g2 * bp)[:, None] * b - (g * p[:, 0])[:, None] * b
    return np.concatenate([(g * (p[:, 0] - bp))[:, None], sp], axis=-1)


def _unit(v):
    n = np.linalg.norm(v, axis=-1, keepdims=True)
    return v / np.where(n > 0, n, 1.0)


def decay_angles(lep_pt, lep_eta, lep_phi, lep_mass, lep_charge):
    lep_pt, lep_eta, lep_phi, lep_mass, lep_charge = (np.asarray(x, dtype=np.float64) for x in
                                                      (lep_pt, lep_eta, lep_phi, lep_mass, lep_charge))
    p = [_p4(lep_pt[:, i], lep_eta[:, i], lep_phi[:, i], lep_mass[:, i]) for i in range(4)]
    # charge ordering inside each Z: first = negative lepton
    def order(a, b, qa):
        neg_first = (qa < 0)[:, None]
        return np.where(neg_first, a, b), np.where(neg_first, b, a)
    q11, q12 = order(p[0], p[1], lep_charge[:, 0])
    q21, q22 = order(p[2], p[3], lep_charge[:, 2])
    X = q11 + q12 + q21 + q22
    bX = X[:, 1:] / X[:, :1]
    # to the 4l rest frame
    q11, q12, q21, q22 = (_boost(v, bX) for v in (q11, q12, q21, q22))
    beam = _boost(np.tile([1.0, 0.0, 0.0, 1.0], (len(X), 1)), bX)
    nz = _unit(beam[:, 1:])
    q1, q2 = q11 + q12, q21 + q22
    q1h = _unit(q1[:, 1:])
    n1 = _unit(np.cross(q11[:, 1:], q12[:, 1:]))
    n2 = _unit(np.cross(q21[:, 1:], q22[:, 1:]))
    nsc = _unit(np.cross(nz, q1h))
    cos_ts = np.sum(q1h * nz, axis=-1)
    dot12 = np.clip(-np.sum(n1 * n2, axis=-1), -1, 1)
    phi = np.sign(np.sum(q1h * np.cross(n1, n2), axis=-1)) * np.arccos(dot12)
    dot1sc = np.clip(np.sum(n1 * nsc, axis=-1), -1, 1)
    phi1 = np.sign(np.sum(q1h * np.cross(n1, nsc), axis=-1)) * np.arccos(dot1sc)
    # theta1: in the Z1 rest frame, angle of the negative lepton to -direction of Z2
    b1 = q1[:, 1:] / q1[:, :1]
    l1z1, q2z1 = _boost(q11, b1), _boost(q2, b1)
    cos_t1 = -np.sum(_unit(l1z1[:, 1:]) * _unit(q2z1[:, 1:]), axis=-1)
    b2 = q2[:, 1:] / q2[:, :1]
    l2z2, q1z2 = _boost(q21, b2), _boost(q1, b2)
    cos_t2 = -np.sum(_unit(l2z2[:, 1:]) * _unit(q1z2[:, 1:]), axis=-1)
    return {"ang_cos_theta_star": cos_ts, "ang_cos_theta1": cos_t1, "ang_cos_theta2": cos_t2,
            "ang_phi": phi, "ang_phi1": phi1}


def _dphi(a, b):
    d = np.abs(a - b) % (2 * np.pi)
    return np.where(d > np.pi, 2 * np.pi - d, d)


def jet_kinematics(pt4l, eta4l, phi4l, j_pt, j_eta, j_phi, cj_pt, cj_eta, cj_phi, suffix="",
                   keys_j=("dphi", "deta", "ptbal", "ptfrac"), keys_cj=("deta", "ptbal", "ptfrac")):
    pt4l, eta4l, phi4l = (np.asarray(x, dtype=np.float64) for x in (pt4l, eta4l, phi4l))
    out = {}
    for tag, (pt, eta, phi), keys in (("j", (j_pt, j_eta, j_phi), keys_j),
                                      ("cj", (cj_pt, cj_eta, cj_phi), keys_cj)):
        pt, eta, phi = (np.nan_to_num(np.asarray(x, dtype=np.float64), nan=0.0) for x in (pt, eta, phi))
        has = pt > 0
        sx, sy = pt4l * np.cos(phi4l) + pt * np.cos(phi), pt4l * np.sin(phi4l) + pt * np.sin(phi)
        den = np.where(pt4l + pt > 0, pt4l + pt, 1.0)
        v = {"dphi": _dphi(phi, phi4l), "deta": np.abs(eta - eta4l),
             "ptbal": np.hypot(sx, sy) / den, "ptfrac": pt / den}
        for k in keys:
            out[f"kin_{k}{suffix}_{tag}_4l"] = np.where(has, v[k], NOJET)
    return out


def compute_all(lep_pt, lep_eta, lep_phi, lep_mass, lep_charge, pt4l, eta4l, phi4l,
                j_pt, j_eta, j_phi, cj_pt, cj_eta, cj_phi, m4l=None):
    """m4l: needed for MNORM_FEATURES (left out of the result when None; requesting them then fails loudly)."""
    d = decay_angles(lep_pt, lep_eta, lep_phi, lep_mass, lep_charge)
    d.update(jet_kinematics(pt4l, eta4l, phi4l, j_pt, j_eta, j_phi, cj_pt, cj_eta, cj_phi))
    keys = ANGLE_FEATURES + JET_FEATURES
    if m4l is not None:
        m4l = np.asarray(m4l, dtype=np.float64)
        if not np.all(m4l > 0):
            raise ValueError("hczz_extra_features: m4l missing or <= 0 for some events")
        pt4l_ref = M_REF * np.asarray(pt4l, dtype=np.float64) / m4l
        d.update(jet_kinematics(pt4l_ref, eta4l, phi4l, j_pt, j_eta, j_phi, cj_pt, cj_eta, cj_phi, suffix="m",
                                keys_j=("ptbal", "ptfrac"), keys_cj=("ptbal", "ptfrac")))
        keys = ALL_FEATURES
    return {k: np.nan_to_num(d[k].astype(np.float32), nan=NOJET) for k in keys}
