import collections,csv,json,pathlib,statistics,sys
root=pathlib.Path(sys.argv[1]); samples=json.load(open(root/'before-profile.json'))
top=collections.Counter();total=breaker=0
for event in samples['recording']['events']:
 frames=(event['values'].get('stackTrace') or {}).get('frames',[])
 names=[f['method']['type']['name']+'.'+f['method']['name'] for f in frames]
 if any('AsyncHashJoinGroupByRecordCursorFactory.aggregate' in n for n in names):
  total+=1;top.update(names[:1]);breaker+=any('CircuitBreaker' in n for n in names)
print('jdk.ExecutionSample with fused reducer ancestor:',total)
print('Explicit CircuitBreaker ancestor:',breaker)
for method,count in top.most_common():print(count,method)
print('Java sampling includes inlining/native attribution limitations; use phase timers, API semantics and controlled breaker comparisons alongside these samples.')
for path in sorted(root.glob('*primary.txt')):
 lines=path.read_text().splitlines();header=next((l for l in lines if l.startswith('arm,repetition,')),None)
 if header is None:continue
 rows=list(csv.DictReader([header]+[l for l in lines if l.startswith('candidate,')]))
 print(path.name)
 for rep in [0,1]:
  part=[r for r in rows if int(r['repetition'])==rep]
  if not part:continue
  phases=['elapsed_ns','build_ns','init_ns','probe_ns','merge_ns']
  print(rep,' '.join(f'{p}={statistics.median(int(r[p]) for r in part)/1e6:.6f}ms' for p in phases),
        'remaining_ms='+str(statistics.median(int(r['elapsed_ns'])-sum(int(r[p]) for p in phases[1:]) for r in part)/1e6))
