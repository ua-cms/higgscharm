"""2026-10-12: copy a stitched card to <card>_pdfcH.txt with Signal's pdf_gg entry moved to its own
pdf_cH lnN (same text rewrite as the builder's --pdf-ch). Value: official H->gg 4FS FxFx NNPDF31 nf_4
mc_hessian (LHA 325500), hessian sum of 100 eigenvectors: 7.4% inclusive, 6.0% (>=1 c-hadron, T=10),
8.0% (0 c-hadron); in-window signal is ~80% >=1c -> 6.4%, rounded up to 1.065.
usage: add_pdfch.py card.txt [value]"""
import sys
card, val = sys.argv[1], (sys.argv[2] if len(sys.argv) > 2 else "1.065")
lines = open(card).read().split("\n")
procs = next(l.split()[1:] for l in lines if l.startswith("process") and not l.split()[1].lstrip("-").isdigit())
isig = procs.index("Signal")
out, done = [], False
for l in lines:
    tok = l.split()
    if tok and tok[0] == "pdf_gg":
        vals = tok[2:]; assert vals[isig] != "-"; vals[isig] = "-"
        out.append(f"{tok[0]:<25}{tok[1]:<8}" + "".join(f"{v:<12}" for v in vals).rstrip())
        cc = ["-"] * len(procs); cc[isig] = val
        out.append(f"{'pdf_cH':<25}{'lnN':<8}" + "".join(f"{v:<12}" for v in cc).rstrip())
        done = True; continue
    out.append(l)
assert done
o = card.replace(".txt", "_pdfcH.txt")
open(o, "w").write("\n".join(out)); print("wrote", o)
