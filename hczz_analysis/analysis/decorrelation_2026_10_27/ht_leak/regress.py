import glob, numpy as np, awkward as ak
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.model_selection import train_test_split
fs=[]
for era in ['2022postEE','2023preBPix']:
    fs+=sorted(glob.glob(f'/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_massinv/{era}/ZZto4L*/base/*.parquet'))[:150]
cols=['zz_mass_inclusive','lepton_pt','lepton_eta','lepton_phi','z_pt','z_eta','z_phi','pT_4l','eta_4l','phi_4l','jet_pt','jet_eta','jet_phi','jet_ht','cjet_HT','n_cjet','weight_nominal']
A=ak.concatenate([ak.from_parquet(f,columns=cols) for f in fs])
m=ak.to_numpy(A.zz_mass_inclusive); s=(m>=105)&(m<160); A=A[s]; m=m[s]
print('qqZZ events 105-160:',len(m),' m4l std %.2f GeV'%m.std())
def pad(x,n): return ak.to_numpy(ak.fill_none(ak.pad_none(x,n,clip=True),0.))
L=dict(lpt=pad(A.lepton_pt,4),leta=pad(A.lepton_eta,4),lphi=pad(A.lepton_phi,4),zpt=pad(A.z_pt,2),zeta=pad(A.z_eta,2),zphi=pad(A.z_phi,2),
       pt4=ak.to_numpy(A.pT_4l)[:,None],eta4=ak.to_numpy(A.eta_4l)[:,None],phi4=ak.to_numpy(A.phi_4l)[:,None],
       jpt=pad(A.jet_pt,3),jeta=pad(A.jet_eta,3),jphi=pad(A.jet_phi,3),ht=ak.to_numpy(A.jet_ht)[:,None],cht=ak.to_numpy(A.cjet_HT)[:,None],ncj=ak.to_numpy(A.n_cjet)[:,None])
M=m[:,None]
def X(scale_lep, scale_jet, jets=True, ht=True):
    f=[L['lpt']/M if scale_lep else L['lpt'],L['leta'],L['lphi'],L['zpt']/M if scale_lep else L['zpt'],L['zeta'],L['zphi'],
       L['pt4']/M if scale_lep else L['pt4'],L['eta4'],L['phi4'],L['ncj']]
    if jets: f+=[L['jpt']/M if scale_jet else L['jpt'],L['jeta'],L['jphi']]
    if ht: f+=[L['ht']/M if scale_jet else L['ht'],L['cht']/M if scale_jet else L['cht']]
    return np.hstack(f)
nj=ak.to_numpy(ak.num(A.jet_pt))
cases=[('no-m4l (leptons absolute)',X(0,0)),
       ('massinvlep: leptons/m4l, jets+HT absolute',X(1,0)),
       ('massinvlep without HT, cjet_HT',X(1,0,ht=False)),
       ('massinvlep without any jet input',X(1,0,jets=False,ht=False)),
       ('massinv: leptons/m4l, jets+HT /m4l',X(1,1))]
for name,x in cases:
    for sub,mask in [('all',np.ones(len(m),bool)),('>=1 jet',nj>=1)]:
        xt,xv,yt,yv=train_test_split(x[mask],m[mask],test_size=0.3,random_state=1)
        r=HistGradientBoostingRegressor(max_iter=400,learning_rate=0.1,random_state=1).fit(xt,yt)
        res=r.predict(xv)-yv; R2=1-res.var()/yv.var()
        print('%-45s %-8s R2 %.3f  resid RMS %.2f GeV (m4l std %.2f)'%(name,sub,R2,res.std(),yv.std()))
