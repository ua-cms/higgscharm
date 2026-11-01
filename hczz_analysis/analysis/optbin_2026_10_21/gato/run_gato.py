#!/usr/bin/env python3
"""Cross-check of the DP-optimised p_sig binning with gato-hep 0.2.1 (gato_sigmoid_model, 1D).
gato maximises sqrt(sum Z_i^2) with Z_i = Asimov(S,B) (no MC-stat term) through sigmoid-smoothed
boundaries, plus penalties for low B and high MC-stat uncertainty. The learnt hard edges are then
scored with the DP objective (Asimov with sigma_b, Cowan) and the DP constraints.
Usage: python3 run_gato.py <events.npz> <out.json> [--half]"""
import sys, json, zlib, numpy as np, tensorflow as tf
sys.path.insert(0, "/eos/user/s/snandaku/Analysis/optbin_2026_10_21/gato/lib")
from gatohep.models import gato_sigmoid_model
from gatohep.losses import low_bkg_penalty, high_bkg_uncertainty_penalty
from gatohep.utils import LearningRateScheduler, SteepnessScheduler
NPZ, OUT = sys.argv[1:3]; HALF = "--half" in sys.argv
EPOCHS, KS, SEEDS, LAMS = 400, [8, 10], [1, 2, 3, 4, 5], [0.0, 1.0]
ev = np.load(NPZ)
BKG = ["qqZZ", "ggZZ", "ggH", "VBF", "WminusH", "WplusH", "ZH", "ttH", "HPlusB", "ZX"]
get = lambda p: (ev["zx_score"], ev["zx_weight"]) if p == "ZX" else (ev[p + "|score"], ev[p + "|weight"])
data = {p: get(p) for p in ["Signal"] + BKG}
def split(d, part):  # same split as optimise_binning.py
    out = {}
    for p, (s, w) in d.items():
        m = np.random.default_rng(zlib.crc32(p.encode())).random(len(s)) < 0.5
        m = m if part == 0 else ~m
        out[p] = (s[m], 2 * w[m])
    return out
opt = split(data, 0) if HALF else data
evl = split(data, 1) if HALF else data
T = {p: {"NN_output": tf.constant(s[:, None], tf.float32), "weight": tf.constant(w, tf.float32)} for p, (s, w) in opt.items()}

def z2(s, b, v):  # identical to optimise_binning.py (DSYS = 0)
    if b <= 0 or s <= 0: return 0.0
    if v <= 0: return 2 * ((s + b) * np.log(1 + s / b) - s)
    t1 = (s + b) * np.log((s + b) * (b + v) / (b * b + (s + b) * v))
    t2 = b * b / v * np.log(1 + v * s / (b * (b + v)))
    return max(2 * (t1 - t2), 0.0)
def score(edges, d):
    H = lambda p, sq=False: np.histogram(d[p][0], bins=edges, weights=d[p][1] ** 2 if sq else d[p][1])[0]
    s = H("Signal"); b = sum(H(p) for p in BKG); v = sum(H(p, True) for p in BKG); zx = H("ZX")
    ok = bool(np.all(b >= 1.0) and np.all(np.sqrt(np.clip(v, 0, None)) / b <= 0.20) and np.all(zx >= 0))
    return float(sum(z2(*x) for x in zip(s, b, v))), ok, b.tolist(), (np.sqrt(np.clip(v, 0, None)) / b).tolist()
# reference = 20 qqZZ quantiles on the full sample
qs, qw = data["qqZZ"]; o = np.argsort(qs); c = np.cumsum(np.abs(qw[o])); c /= c[-1]
qe = np.interp(np.linspace(0, 1, 21), c, qs[o]); qe[0], qe[-1] = 0, 1
Zref = np.sqrt(score(qe, opt)[0])  # loss normalisation only

class G(gato_sigmoid_model):
    def __init__(self, k, steep=50.0):
        super().__init__(variables_config=[{"name": "NN_output", "bins": k, "range": (0.0, 1.0)}], global_steepness=steep)
    def call(self, d):
        z, b, bw2 = self.get_differentiable_significance(d, signal_labels=["Signal"], return_details=True)
        return -z["Signal"] / Zref, b, bw2

res = {"ref_sumZ2_eval": score(qe, evl)[0], "half": HALF, "runs": []}
print(f"ref 20 qqZZ quantiles: sumZ2(eval) {res['ref_sumZ2_eval']:.4e}", flush=True)
for k in KS:
    for lam in LAMS:
        for seed in SEEDS:
            tf.random.set_seed(seed); np.random.seed(seed)
            m = G(k); optm = tf.keras.optimizers.RMSprop(learning_rate=0.1)
            lrs = LearningRateScheduler(optm, lr_initial=0.1, lr_final=0.001, total_epochs=EPOCHS, mode="cosine")
            sts = SteepnessScheduler(m, t_initial=50.0, t_final=10000.0, total_epochs=EPOCHS, mode="cosine")
            @tf.function
            def step():
                with tf.GradientTape() as tape:
                    loss, b, bw2 = m.call(T)
                    tot = loss + lam * (low_bkg_penalty(b, threshold=1.0) + high_bkg_uncertainty_penalty(bw2, b, rel_threshold=0.2))
                g = tape.gradient(tot, m.trainable_variables); optm.apply_gradients(zip(g, m.trainable_variables))
                return loss
            for e in range(EPOCHS):
                lrs.update(e); sts.update(e); loss = step()
            edges = [0.0] + sorted(float(x) for x in m.calculate_boundaries().numpy()) + [1.0]
            zo = score(edges, opt)[0]; ze, ok, b, rel = score(edges, evl)
            r = {"K": k, "lam": lam, "seed": seed, "gato_Z_soft": float(-loss.numpy() * Zref), "sumZ2_opt": zo, "sumZ2_eval": ze,
                 "dp_constraints_ok": ok, "edges": edges, "B": b, "relunc": rel}
            res["runs"].append(r)
            print(f"K{k} lam{lam} s{seed}: sumZ2 opt {zo:.4e} eval {ze:.4e} ok {ok} minB {min(b):.2f} maxrel {max(rel):.2f} "
                  f"edges {' '.join(f'{x:.4f}' for x in edges[1:-1])}", flush=True)
json.dump(res, open(OUT, "w"), indent=1)
