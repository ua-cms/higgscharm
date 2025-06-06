### JERC files

JEC and JER tarballs were obtained from [here](https://github.com/cms-jet/JECDatabase/tree/master/tarballs) and [here](https://github.com/cms-jet/JRDatabase/tree/master/tarballs), respectively. Full documentation [here](https://cms-jerc.web.cern.ch/Recommendations/)

**Note:** Filenames were changed: 
* We added the extensions `jec`, `jr`,`jersf`, and `junc` to the JEC, JER, JER Scale Factors, and JERC uncerntainties files, respectively. These extensions need to be added in order to use the [Coffea jetmet tools](https://coffeateam.github.io/coffea/modules/coffea.jetmet_tools.html).     
* We also substracted the first underscore in the filenames to match the name criteria of the jetmet tools: filenames must contain 5 words (excluding underscores) 

### Lumi file

To generate the `lumi2022.csv` file we used [brilcalc](https://twiki.cern.ch/twiki/bin/view/CMS/BrilcalcQuickStart), the official tool for calculating CMS luminosity.

```
# connect to lxplus
ssh <your_username>@lxplus.cern.ch

# load the environment
source /cvmfs/cms-bril.cern.ch/cms-lumi-pog/brilws-docker/brilws-env

# Run brilcalc
brilcalc lumi -b "STABLE BEAMS" --normtag=/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json -u /pb --byls --output-style csv -i Cert_Collisions2022_355100_362760_Golden.txt > lumi2022.csv
```