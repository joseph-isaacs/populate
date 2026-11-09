import json, collections, os
ENGINES = [e for e in ['duckdb', 'datafusion', 'dfcli-base', 'dfcli-new']
           if os.path.exists(f'results/{e}_tpch.jsonl')]
exec(open('why2.py').read().split('for f in sys.argv')[0])
TYPEBLOCK = {'MARK','LEFT','FULL','ANTI','RIGHT_ANTI','SINGLE','Right','Full','RightMark','RightAnti','LeftAnti'}
print('| engine | bench | equi hash joins | pushed | n/a (empty build / unknown) | blocked by join type | blocked by plan shape | TopK pushed | min/max-agg pushed |')
print('|---|---|---|---|---|---|---|---|---|')
for b in ['tpch','tpcds','clickbench']:
  for e in ENGINES:
    c=collections.Counter()
    for l in open(f'results/{e}_{b}.jsonl'):
      r=json.loads(l)['r']
      if not r['ok']: continue
      for p in r['producers']:
        if p['kind']=='join':
          if not p['equi']: c['nonequi']+=1; continue
          c['joins']+=1
          if p['pushed']: c['pushed']+=1; continue
          k=cat(p)
          if 'empty' in k or 'unknown' in k: c['na']+=1
          elif p['join_type'] in TYPEBLOCK or 'full' in k: c['type']+=1
          else: c['shape']+=1
        else:
          c[p['kind']]+=1; c[p['kind']+'_p']+=p['pushed']
    elig=c['joins']-c['na']
    pct=f" ({100*c['pushed']/elig:.0f}%)" if elig else ''
    print(f"| {e} | {b} | {c['joins']} | {c['pushed']}/{elig}{pct} | {c['na']} | {c['type']} | {c['shape']} | {c['topk_p']}/{c['topk']} | {c['minmax_agg_p']}/{c['minmax_agg']} |")
