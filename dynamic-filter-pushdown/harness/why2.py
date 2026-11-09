import json, sys, collections
AGG={'HASH_GROUP_BY','PERFECT_HASH_GROUP_BY','UNGROUPED_AGGREGATE','AggregateExec'}
WIN={'WINDOW','STREAMING_WINDOW','BoundedWindowAggExec','WindowAggExec'}
UN={'UNION','UnionExec','InterleaveExec'}
J=lambda x: 'JOIN' in x or x.endswith('JoinExec') or x in ('CROSS_PRODUCT',)
def cat(p):
    if str(p.get('build_rows')) in ('0',): return 'build side empty at runtime'
    if p.get('probe_info_missing') and p.get('paths') and all(x is None for x in p['paths']): return 'unknown: profiler lost scan info (pipeline cut short)'
    paths=[x for x in p['paths']]
    if p.get('join_type','').upper().replace('_','') in ('FULL',): return 'full outer join (unsafe)'
    if all(x is None for x in paths): return 'probe key is derived (agg/expr/CTE/alias)'
    path=next(x for x in paths if x)[1:]
    if any(x in AGG for x in path): return 'probe key crosses aggregate'
    if any(x in WIN for x in path): return 'probe key crosses window'
    if any(x in UN for x in path): return 'probe key crosses union'
    if any(J(x) for x in path): return 'probe key crosses another join'
    return 'direct path to scan, still not pushed'
for f in sys.argv[1:]:
    c=collections.Counter(); ex=collections.defaultdict(set)
    for l in open(f):
        r=json.loads(l); q=r['q']; r=r['r']
        if not r['ok']: continue
        for p in r['producers']:
            if p['kind']!='join' or not p['equi'] or p['pushed']: continue
            k=(p['join_type'], cat(p)); c[k]+=1; ex[k].add(q)
    print(f)
    for k,v in sorted(c.items(), key=lambda x:-x[1]): print(f'  {v:4d}', k, sorted(ex[k])[:10])
