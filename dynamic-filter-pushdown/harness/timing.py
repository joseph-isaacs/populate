import subprocess, time, sys, json, os, re
W=os.path.dirname(os.path.abspath(__file__))
binary=sys.argv[1]; out=sys.argv[2]
qs=[('tpch',f'dfr/benchmarks/queries/q{i}.sql') for i in range(1,23)]+[('tpcds',f'dfr/datafusion/core/tests/tpc-ds/{i}.sql') for i in range(1,100)]
res={}
MODES=sys.argv[3].split(',') if len(sys.argv)>3 else ['off','on']
for bench,q in qs:
    for mode in MODES:
        setup=open(f'cli/setup_{bench}.sql').read()
        if mode=='on': setup='SET datafusion.execution.parquet.pushdown_filters = true;\n'+setup
        sql=re.sub(r'--[^\n]*','',open(q).read())
        path=f'{W}/cli/t_{mode}.sql'; open(path,'w').write(setup+'\n'+sql)
        best=None
        for rep in range(3):
            t=time.time(); p=subprocess.run([binary,'-q','--format','csv','-f',path],capture_output=True,timeout=300); dt=time.time()-t
            if p.returncode!=0 or b'rror' in p.stderr[:2000]: dt=None; break
            best=dt if best is None else min(best,dt)
        res.setdefault(f'{bench}/{os.path.basename(q)}',{})[mode]=best
json.dump(res,open(out,'w'),indent=0)
