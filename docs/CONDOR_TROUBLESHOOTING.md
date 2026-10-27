# Condor Job Troubleshooting Guide

## Job Failure Investigation: hplusc_mva_4class_CR (April 2026)

### Summary
- **Total jobs:** 589
- **Failed:** 585 (99%)
- **Succeeded:** 1

### Root Causes

Jobs failed during coffea preprocessing when accessing remote ROOT files via XRootD. Two main error types:

#### 1. XRootD Timeout Errors (369 occurrences)
```
OSError: XRootD error: [ERROR] Operation expired
in file root://eoscms.cern.ch//eos/cms/store/mc/...
```

#### 2. Access Denied Errors (120 occurrences)
```
FileNotFoundError: [3010] Access denied
root://se01.indiacms.res.in//dpm/indiacms.res.in/...
```

### Problematic Sites Identified

| Site | Hostname | Files | Issue |
|------|----------|-------|-------|
| T2_IN_TIFR | se01.indiacms.res.in | 350 | Access denied |
| T2_CH_CERN | eoscms.cern.ch | 371 | Timeout errors |
| T2_UA_KIPT | cms-se0.kipt.kharkov.ua | 78 | Connectivity |
| T2_RU_JINR | lcgsedr01.jinr.ru | 88 | Connectivity |

### Fix Applied

Removed problematic sites from whitelist files:
- `analysis/filesets/2022postEE_sites.yaml`
- `analysis/filesets/2022preEE_sites.yaml`
- `analysis/filesets/2023preBPix_sites.yaml`
- `analysis/filesets/2023postBPix_sites.yaml`

Sites removed:
- **T2_IN_TIFR** (India)
- **T2_UA_KIPT** (Ukraine)
- **T2_RU_JINR** (Russia)

### How to Regenerate Dataset Discovery

After updating sites.yaml, regenerate the dataset discovery:

```bash
cd /afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm

# Ensure valid proxy
voms-proxy-init --voms cms -valid 192:00

# Regenerate for each year
python3 fetch.py -y 2022postEE
python3 fetch.py -y 2022preEE
python3 fetch.py -y 2023preBPix
python3 fetch.py -y 2023postBPix
```

### Why Sites May Fail Intermittently

1. **Site maintenance/outages** - Grid sites have scheduled and unscheduled downtime
2. **Authorization changes** - Sites may update access policies
3. **Network routing** - Connectivity to remote sites (especially non-European) can be unstable from CERN
4. **Replica availability** - CMS replicas are dynamic; files get added/removed
5. **Load balancing** - Round-robin strategy distributes load but may hit overloaded sites

### Checking Job Logs

Job logs location:
```
/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/condor/logs/<workflow>/<year>/<sample>/
```

Files:
- `*.log` - Condor job lifecycle events
- `*.err` - stderr (Python tracebacks)
- `*.out` - stdout

Quick diagnostics:
```bash
# Count failed jobs (exit code 1)
find condor/logs/<workflow>/ -name "*.log" -exec grep -l "return value 1" {} \; | wc -l

# Count successful jobs (exit code 0)
find condor/logs/<workflow>/ -name "*.log" -exec grep -l "return value 0" {} \; | wc -l

# Find XRootD timeout errors
grep -r "Operation expired" condor/logs/<workflow>/ | wc -l

# Find access denied errors
grep -r "Access denied" condor/logs/<workflow>/ | wc -l
```

### Analyzing Dataset Discovery Sites

Check which sites are in your dataset discovery:
```python
import json
from collections import Counter

with open('dataset_discovery_2022postEE.json') as f:
    data = json.load(f)

site_counts = Counter()
for dataset, info in data.items():
    if 'files' in info:
        for fpath in info['files'].keys():
            if fpath.startswith('root://'):
                site = fpath.split('/')[2]
                site_counts[site] += 1

for site, count in site_counts.most_common():
    print(f'{count:5d}  {site}')
```

### Recommended Reliable Sites

Tier-1 sites (generally most reliable):
- T1_US_FNAL_Disk
- T1_DE_KIT_Disk
- T1_UK_RAL_Disk
- T1_FR_CCIN2P3_Disk
- T1_IT_CNAF_Disk
- T1_ES_PIC_Disk

Reliable Tier-2 sites:
- T2_US_* (US sites generally reliable)
- T2_DE_DESY
- T2_CH_CSCS
- T2_BE_IIHE
- T2_UK_London_IC

### Alternative: Use Global Redirector

Instead of direct site URLs, use the CMS global redirector which handles failover automatically:
```
root://cms-xrd-global.cern.ch//store/...
```

This requires modifying the dataset discovery or submit scripts to use the redirector prefix.

---
*Last updated: April 7, 2026*
