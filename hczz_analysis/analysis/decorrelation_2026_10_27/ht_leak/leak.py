import glob, numpy as np, awkward as ak
fs=sorted(glob.glob('/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_massinv/2022postEE/ZZto4L*/base/*.parquet'))[:40]
cols=['zz_mass_inclusive','pT_4l','phi_4l','z1_pt','z1_l1_pt','z1_l1_phi','z1_l2_pt','z1_l2_phi','jet_pt','jet_phi','jet_ht','weight_nominal']
A=ak.concatenate([ak.from_parquet(f,columns=cols) for f in fs])
m=ak.to_numpy(A.zz_mass_inclusive); w=ak.to_numpy(A.weight_nominal)
sel=(m>=105)&(m<160)
A=A[sel]; m=m[sel]; w=w[sel]
print('events 105-160:',len(m))
# 1) Z pT leak: z1_pt / |pT(l1)+pT(l2)|
px=A.z1_l1_pt*np.cos(A.z1_l1_phi)+A.z1_l2_pt*np.cos(A.z1_l2_phi); py=A.z1_l1_pt*np.sin(A.z1_l1_phi)+A.z1_l2_pt*np.sin(A.z1_l2_phi)
r=ak.to_numpy(A.z1_pt/np.hypot(px,py)); print('Z1: |z1_pt/|sum lep pT| - 1| < 0.1%%: %.3f'%np.mean(np.abs(r-1)<1e-3))
# so with leptons scaled and z_pt absolute: m_est = z1_pt / |sum(lep_pt/m4l)| = m4l exactly
# 2) jet balance: m_est = |sum jet pT| / (pT_4l/m4l)
nj=ak.to_numpy(ak.num(A.jet_pt))
jx=ak.to_numpy(ak.sum(A.jet_pt*np.cos(A.jet_phi),axis=1)); jy=ak.to_numpy(ak.sum(A.jet_pt*np.sin(A.jet_phi),axis=1))
pt4=ak.to_numpy(A.pT_4l)
for cut in [1]:
  s=(nj>=cut)&(pt4>20)
  mest=np.hypot(jx,jy)[s]/(pt4[s]/m[s]); rel=mest/m[s]-1
  q=np.percentile(rel,[16,50,84]); print('jets>=1, pT4l>20 (frac of evts %.2f): m_est/m4l-1 median %.2f, 68%% [%.2f,%.2f]'%(s.mean(),q[1],q[0],q[2]))
  print('  corr(m_est,m4l)=%.3f'%np.corrcoef(mest,m[s])[0,1])
# 3) direct: correlation of absolute jet quantities with m4l
s=nj>=1
lj=ak.to_numpy(ak.firsts(A.jet_pt))[s]
for name,x in [('lead jet pT',lj),('jet_ht',ak.to_numpy(A.jet_ht)[s]),('pT_4l',pt4[s])]:
  print('%-12s corr with m4l %.3f ; corr of x/m4l with m4l %.3f'%(name,np.corrcoef(x,m[s])[0,1],np.corrcoef(x/m[s],m[s])[0,1]))
# binned mean of lead jet pT vs m4l
b=np.arange(105,161,11); idx=np.digitize(m[s],b)-1
print('mean lead-jet pT per m4l bin',[round(float(np.average(lj[idx==i])),1) for i in range(5)])
print('mean pT_4l per m4l bin',[round(float(np.average(pt4[s][idx==i])),1) for i in range(5)])
