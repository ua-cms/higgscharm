"""Nominal (20 qqZZ-quantile bins) vs optimised (10 bins) p_sig templates. Usage: plot_binning_compare.py <nominal card dir> <optbin card dir>"""
import uproot,sys,numpy as np,matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
import mplhep as hep; hep.style.use('CMS')
fig,axs=plt.subplots(2,2,figsize=(18,11),gridspec_kw={'height_ratios':[3,1]},sharex='col')
OH=['VBF','WminusH','WplusH','ZH','ttH','HPlusB']
for col,(d,title) in enumerate([(sys.argv[1],'Nominal: 20 q$\\bar{q}$ZZ-quantile bins\n$r<456$ (33.4 pb)'),(sys.argv[2],'Optimised: 10 bins\n$r<427$ (31.3 pb)')]):
    f=uproot.open(d+'/histograms_mva_with_zx.root'); v=lambda k:f[k].values()
    n=len(v('h_Signal')); x=np.arange(n+1)
    st=[('Other Higgs',sum(v('h_'+p) for p in OH),'#e76f51'),('Gluon fusion',v('h_ggH'),'#f4a261'),(r'$gg\to ZZ$',v('h_ggZZ'),'#e9c46a'),
        ('Z+X',np.clip(v('h_ZX'),0,None),'#2a9d8f'),(r'$q\bar{q}\to ZZ$',v('h_qqZZ'),'#457b9d')]
    a,r=axs[0,col],axs[1,col]; bot=np.zeros(n)
    for lab,h,c in st: a.bar(x[:-1],h,width=1,bottom=bot,align='edge',color=c,label=lab); bot+=h
    s=v('h_Signal'); a.step(x,np.append(s*200,s[-1]*200),where='post',color='k',lw=2,label=r'H+c $\times$200')
    a.set_ylabel('Events / bin'); a.set_ylim(0,bot.max()*1.55); a.legend(fontsize=12,loc='upper left',ncol=2)
    a.text(0.97,0.95,title,transform=a.transAxes,ha='right',va='top',fontsize=16)
    hep.cms.label('Work in progress',data=False,lumi=61.9,com=13.6,ax=a,fontsize=14)
    r.step(x,np.append(s/bot,s[-1]/bot[-1])*1e3,where='post',color='k',lw=2); r.set_ylabel(r'S/B $\times10^3$',fontsize=15)
    r.set_xlabel(r'$p_{\rm sig}$ bin'); r.set_xlim(0,n)
plt.tight_layout(); plt.savefig('hczz_binning_nominal_vs_optimised.pdf'); plt.savefig('hczz_binning_nominal_vs_optimised.png',dpi=90)
