import pathlib, shutil, subprocess, csv, io
root=pathlib.Path('/tmp/questdb-task9f/validator-guards');root.mkdir(exist_ok=True)
for arm,marker in [('reference','noop diagnostic reference only'),('candidate','active throttle=2000000 timeout=unlimited fd=-1')]:
 d=root/arm;d.mkdir(exist_ok=True)
 for case in pathlib.Path('benchmarks/parallel-hash-join-group-by-recovery-cases.txt').read_text().splitlines():
  s=pathlib.Path('docs/parallel-hash-join-group-by-v1',case+'.txt').read_text()
  (d/(case+'.txt')).write_text('# breaker='+marker+'\n'+s)
cmd=['python3','benchmarks/compare-hash-join-group-by-recovery.py',str(root/'reference'),str(root/'candidate'),str(root/'comparison.csv')]
def check(name,success=False):
 r=subprocess.run(cmd,capture_output=True,text=True)
 assert (r.returncode==0)==success,(name,r.stdout,r.stderr)
 print(name+': PASS')
check('identical validated matrices',True)
p=root/'candidate'/'inner.txt';original=p.read_text()
p.rename(p.with_suffix('.held'));check('missing case');p.with_suffix('.held').rename(p)
p.write_text(original.replace('# breaker=active throttle=2000000 timeout=unlimited fd=-1','# breaker=noop diagnostic reference only'));check('no-op candidate');p.write_text(original)
p.write_text(original.replace('# reference_groups=120','# reference_groups=119'));check('different ordered result');p.write_text(original)
lines=original.splitlines();header=next(x for x in lines if x.startswith('arm,repetition,'));idx=header.split(',').index('elapsed_ns')
new=[]
for line in lines:
 if line.startswith('candidate,'):
  fields=line.split(',');fields[idx]=str(int(fields[idx])*2);line=','.join(fields)
 new.append(line)
p.write_text('\n'.join(new)+'\n');check('over ten percent slowdown');p.write_text(original)
removed=False;new=[]
for line in lines:
 if line.startswith('candidate,') and not removed:removed=True;continue
 new.append(line)
p.write_text('\n'.join(new)+'\n');check('missing measured sample');p.write_text(original)
