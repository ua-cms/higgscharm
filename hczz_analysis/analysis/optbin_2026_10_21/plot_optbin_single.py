"""Optimised 10-bin p_sig templates, single panel (AN Signal Extraction figure, 2026-10-22). Usage: plot_optbin_single.py <optbin card dir>"""
import uproot,sys,numpy as np,matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
import mplhep as hep; hep.style.use('CMS')
fig,(a,r)=plt.subplots(2,1,figsize=(10,10),gridspec_kw={'height_ratios':[3,1]},sharex=True)
OH=['VBF','WminusH','WplusH','ZH','ttH','HPlusB']
f=uproot.open(sys.argv[1]+'/histograms_mva_with_zx.root'); v=lambda k:f[k].values()
n=len(v('h_Signal')); x=np.arange(n+1)
st=[('Other Higgs',sum(v('h_'+p) for p in OH),'#e76f51'),('Gluon fusion',v('h_ggH'),'#f4a261'),(r'$gg\to ZZ$',v('h_ggZZ'),'#e9c46a'),
    ('Z+X',np.clip(v('h_ZX'),0,None),'#2a9d8f'),(r'$q\bar{q}\to ZZ$',v('h_qqZZ'),'#457b9d')]
bot=np.zeros(n)
for lab,h,c in st: a.bar(x[:-1],h,width=1,bottom=bot,align='edge',color=c,label=lab); bot+=h
s=v('h_Signal'); a.step(x,np.append(s*200,s[-1]*200),where='post',color='k',lw=2,label=r'H+c $\times$200')
a.set_ylabel('Events / bin'); a.set_ylim(0,bot.max()*1.55); a.legend(fontsize=13,loc='upper left',ncol=2)
a.text(0.97,0.95,'Optimised: 10 bins\n$r<427$ (31.3 pb)',transform=a.transAxes,ha='right',va='top',fontsize=16)
hep.cms.label('Work in progress',data=False,lumi=61.9,com=13.6,ax=a,fontsize=15)
r.step(x,np.append(s/bot,s[-1]/bot[-1])*1e3,where='post',color='k',lw=2); r.set_ylabel(r'S/B $\times10^3$',fontsize=15)
r.set_xlabel(r'$p_{\rm sig}$ bin'); r.set_xlim(0,n)
plt.tight_layout(); plt.savefig('figs/hczz_optbin10_psig_templates.pdf'); plt.savefig('figs/hczz_optbin10_psig_templates.png',dpi=90)
