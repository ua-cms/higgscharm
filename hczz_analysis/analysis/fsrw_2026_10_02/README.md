# fsrw_2026_10_02: H+c cards with the FS.pdf stitch applied by reweighting the 4FS sample

Signal: 4FS FxFx H->ZZ->4l, reweighted per event to the 3FS+4FS stitch (FS.pdf, T. Bevilacqua).
- The weights come from the official Run 3 H->gg 3FS / 4FS FxFx samples (our rederivation, identical to C. Gupta's).
- The total is normalised to NNLO, 68.2 fb.
- FS uncertainty: `CMS_FS_cH_massless1c` and `CMS_FS_cH_T10` shape nuisances.
- Everything else is identical to the 09-25 perdsnorm reference (335.0 / 328.0).

Background: second-brain `Notes/hczz-fs-3fs-gridpack-mismatch-2026-10-02.md` (sections 7–9).

## What runs, and does it survive closing the terminal?

| step | how it was started | survives logout? |
|---|---|---|
| `pipeline.sh` (waits for its inputs, then weights → 3 cards → limits → impacts) | `setsid nohup` on lxplus9108, 2026-10-02 11:40 | **yes**: detached from the terminal (own session, no hangup signal) |
| MVA scoring (input to the pipeline) | finished before the terminal could close | n/a |

What a detached job still needs:
- **A Kerberos ticket**, for EOS/AFS access. The current one is valid until **2026-10-03 12:11**; the pipeline needs about 6–8 h. Check it with `klist`. If it is about to expire, run `kinit` on the same node (`ssh lxplus9108.cern.ch`); the running job picks up the renewed ticket.
- **A grid proxy**, for xrootd: `/tmp/x509up_u178308`, valid until 2026-10-10.
- **The node staying up.** lxplus nodes are occasionally rebooted, and very large memory use can get the process killed (the 09-29 run died when two builds ran in parallel). If that happens, use `resume.sh` (below).

## Check progress (from any lxplus node)

```bash
P=/eos/user/s/snandaku/Analysis/fsrw_2026_10_02
cat $P/pipeline.log                       # step-by-step timestamps, "BUILD FAILED", "ALL DONE"
grep "Expected 50" $P/combine_*.out       # the limits (r at 95% CL, median expected)
ssh lxplus9108.cern.ch 'ps -u $USER -o pid,etime,cmd | grep -E "pipeline|resume|create_datacards|combine" | grep -v grep'
```

Card dirs (each has `build.log`, the datacard, the ROOT histograms, `stitch_summary.json`, and `limit_<tag>.log` once done), under `/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs/`:
- `combine_run3_100150_ctag2d_fsrw_withzxCRmerged_jecshifts_automcstats_2026_10_02`: **nominal** (with Z+X)
- `combine_run3_100150_ctag2d_fsrw_nozx_jecshifts_automcstats_2026_10_02`: without Z+X
- `combine_run3_100150_ctag2d_fsrw_nofssyst_withzxCRmerged_jecshifts_automcstats_2026_10_02`: with Z+X but without the FS nuisances, to measure their effect

## If it stopped (node reboot, ticket expired, killed): resume

`resume.sh` repeats the steps of `pipeline.sh` and **skips everything already finished**:
- the scoring, if its log has `EXIT=0`;
- the weights, if they already use 4 eras;
- a card, if its `build.log` has `[Done]`;
- a limit, if `limit_<tag>.log` exists;
- the impacts, if their json exists.

It is safe to run repeatedly. First make sure no old pipeline is still running (the `ps` command above). Then:

```bash
ssh lxplus9108.cern.ch            # or any lxplus9 node
kinit                             # if klist shows no valid ticket
P=/eos/user/s/snandaku/Analysis/fsrw_2026_10_02
setsid nohup bash $P/resume.sh > $P/resume.log 2>&1 < /dev/null &
exit                              # safe: the job keeps running
```
Then follow `resume.log` instead of `pipeline.log`. If a card shows `BUILD FAILED`, read the end of its `build.log`, fix the cause, delete that card dir (or just its `build.log`), and resume again.

## Files here

- `pipeline.sh`: the original run, launched 11:40. Do not edit it while it runs: bash reads scripts incrementally.
- `resume.sh`: the resumable version of the same steps.
- `run_combine.sh`: limits / impacts in a clean environment (CMSSW_14_1_0_pre4; work dir `/tmp/snandaku/fsrw_2026_10_02/` on the node that runs it).
- `combine_<tag>.out`: combine output per card.

Inputs:
- weights: `/eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02/stitch_weights.{json,md}`;
- signal: `outputs/hplusc_mva_4class_ctag2d_fschad_scored_v1` (scored from `..._jecshifts_fschad`, condor clusters 16336338–43);
- builder: `combine/scripts/create_datacards_ctag2d_100150_stitched.py --signal-mode 4fs_rw` (uncommitted).
