import numpy as np
import awkward as ak


def add_lhepdf_weight(events, weights_container):
    """
    Add PDF, αs, and combined (PDF⊕αs) systematic weight variations to the Weights container.

    Handles both standard (103 weights) and reduced (101 weights, no αs) PDF sets.

    This function follows the CMS MC Contact Report (Antonis Agapitos et al., B2G Meeting, 14 July 2020):

        δ_pdf   = sqrt( Σ_{k=1}^{100} (σ^(k) - σ^(0))² )
        δ_αs    = |σ^(102) - σ^(101)| / 2   [if αs weights exist]
        δ_total = sqrt( δ_pdf² + δ_αs² )

    where:
      • mem = 0       → central PDF (αs = 0.118)
      • mem = 1–100   → PDF eigenvector members (MC-Hessian: δ_pdf = quadrature sum, no 1/(N-1))
      • mem = 101–102 → αs variations (αs = 0.116, 0.120)

    The LHEPdfWeight branch contains ratios (w_var / w_nominal) for LHA IDs 306000–306102.
    Some samples (e.g. ST_s-channel_4f_leptonDecays) were produced with only 101 members
    and therefore lack the αs variations.

    The function computes and registers:
      • "PDFweight"       → PDF uncertainty only
      • "AlphaSweight"    → αs uncertainty only (if available)
      • "PDFAlphaSweight" → combined PDF⊕αs uncertainty (if available)

    References:
      - CMS MC Contact Report v3, slides 4–5 & 25–28:
        https://indico.cern.ch/event/938672/contributions/3943718/attachments/2073936/3482265/MC_ContactReport_v3.pdf
      - PDF4LHC recommendations, arXiv:1510.03865
    """
    if "LHEPdfWeight" in events.fields:
        pdfweights = events.LHEPdfWeight
        n_members = ak.num(pdfweights)

        # Case 2: 103 members → PDF + αs
        if ak.all(n_members == 103):
            # PDF
            w0 = pdfweights[:, 0]  # central (mem=0)
            w_all = pdfweights[:, 1:101]  # mem=1..100
            diffs = (w_all - w0[:, None]) ** 2
            # MC-Hessian set (official samples LHA 325300-325402, private H+c 325500-325600, checked
            # 2026-11-04 in the NanoAOD branch titles): PDF4LHC15 Hessian prescription, quadrature sum of
            # the 100 eigenvector deviations (arXiv:1510.03865 eq. 20), NO 1/(N-1) -- that is the MC-replica
            # RMS and made delta_pdf sqrt(99) too small (ZZto4L [105,160): 0.41% instead of 4.1%,
            # HIG-24-013 +3.1/-3.4%). The 2026-08-03 '/99' was added for the private H+c/H+b samples,
            # whose members 1-100 are normalised differently from member 0 (all ~1.7-2.3 vs 1.0), so
            # their delta is unusable with any normalisation (excluded in the datacard builders).
            delta_pdf = np.sqrt(ak.sum(diffs, axis=1))

            w_up_pdf = 1 + delta_pdf
            w_down_pdf = 1 - delta_pdf

            # αs: genuinely SIGNED shift, unlike the ~100-member PDF envelope
            # above. Taking np.abs() here (as an earlier version did) forces
            # w_up_alpha >= 1 and w_down_alpha <= 1 for every event of every
            # process unconditionally, destroying the true per-event sign of
            # the alphaS variation -- confirmed 2026-08-19 via qqZZ/Other_Higgs
            # templates moving in lockstep (Up always up, Down always down),
            # which a genuine independent-process physics effect would not do.
            w_as_low = pdfweights[:, 101]
            w_as_high = pdfweights[:, 102]
            delta_alpha = 0.5 * (w_as_high - w_as_low)
            w_up_alpha = 1 + delta_alpha
            w_down_alpha = 1 - delta_alpha

            # PDF + αs
            delta_total = np.sqrt(delta_pdf**2 + delta_alpha**2)
            w_up_total = 1 + delta_total
            w_down_total = 1 - delta_total

        # Case 3: 101 members → PDF only, αs = 1
        elif ak.all(n_members == 101):
            w0 = pdfweights[:, 0]
            w_all = pdfweights[:, 1:101]
            diffs = (w_all - w0[:, None]) ** 2
            # MC-Hessian set (official samples LHA 325300-325402, private H+c 325500-325600, checked
            # 2026-11-04 in the NanoAOD branch titles): PDF4LHC15 Hessian prescription, quadrature sum of
            # the 100 eigenvector deviations (arXiv:1510.03865 eq. 20), NO 1/(N-1) -- that is the MC-replica
            # RMS and made delta_pdf sqrt(99) too small (ZZto4L [105,160): 0.41% instead of 4.1%,
            # HIG-24-013 +3.1/-3.4%). The 2026-08-03 '/99' was added for the private H+c/H+b samples,
            # whose members 1-100 are normalised differently from member 0 (all ~1.7-2.3 vs 1.0), so
            # their delta is unusable with any normalisation (excluded in the datacard builders).
            delta_pdf = np.sqrt(ak.sum(diffs, axis=1))

            w_up_pdf = 1 + delta_pdf
            w_down_pdf = 1 - delta_pdf

            print("skiping AlphaS systematic Weight")
            w_up_alpha = ak.ones_like(delta_pdf)
            w_down_alpha = ak.ones_like(delta_pdf)
            w_up_total = ak.ones_like(delta_pdf)
            w_down_total = ak.ones_like(delta_pdf)

        # Case 4: other number of members → skip PDF weights
        else:
            print(f"Unexpected number of LHEPdfWeight members: {ak.to_list(n_members[:5])}, skipping PDF/AlphaS systematic Weight")
            delta_pdf = np.ones(len(events))
            w_up_pdf = np.ones(len(events))
            w_down_pdf = np.ones(len(events))
            w_up_alpha = np.ones(len(events))
            w_down_alpha = np.ones(len(events))
            w_up_total = np.ones(len(events))
            w_down_total = np.ones(len(events))

    else:
        print("No LHEPdf Weights in dataset, skiping PDF/AlphaS systematic Weight")
        delta_pdf = np.ones(len(events))
        w_up_pdf = np.ones(len(events))
        w_down_pdf = np.ones(len(events))
        w_up_alpha = np.ones(len(events))
        w_down_alpha = np.ones(len(events))
        w_up_total = np.ones(len(events))
        w_down_total = np.ones(len(events))

    weights_container.add(
        "lhe_pdf",
        weight=ak.ones_like(delta_pdf),
        weightUp=w_up_pdf,
        weightDown=w_down_pdf,
    )
    weights_container.add(
        "lhe_alphaS",
        weight=ak.ones_like(delta_pdf),
        weightUp=w_up_alpha,
        weightDown=w_down_alpha,
    )
    weights_container.add(
        "lhe_pdf_alphaS",
        weight=ak.ones_like(delta_pdf),
        weightUp=w_up_total,
        weightDown=w_down_total,
    )
