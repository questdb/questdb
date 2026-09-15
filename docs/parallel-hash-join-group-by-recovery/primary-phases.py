from pathlib import Path
import csv, statistics
root=Path('/tmp/questdb-task9f')
files=[(p.stem,p) for p in sorted((root/'recovery/controls').glob('*.txt'))]
files += [('reference-matrix',root/'recovery/reference/primary-w4.txt'),('final-candidate',root/'final-candidate/primary-w4.txt')]
rows=[]
for variant,path in files:
    lines=path.read_text().splitlines()
    header=next(line for line in lines if line.startswith('arm,repetition,'))
    samples=list(csv.DictReader([header]+[line for line in lines if line.startswith('candidate,')]))
    for repetition in sorted({s['repetition'] for s in samples}):
        part=[s for s in samples if s['repetition']==repetition]
        assert len(part)==10,(variant,repetition,len(part))
        result={'variant':variant,'repetition':repetition,'samples':len(part)}
        for phase in ['elapsed','build','init','probe','merge']:
            result[phase+'_median_ms']=statistics.median(int(s[phase+'_ns']) for s in part)/1e6
        result['remaining_median_ms']=statistics.median(int(s['elapsed_ns'])-sum(int(s[p+'_ns']) for p in ['build','init','probe','merge']) for s in part)/1e6
        rows.append(result)
with Path('docs/parallel-hash-join-group-by-recovery/primary-phases.csv').open('w') as dest:
    writer=csv.DictWriter(dest,fieldnames=list(rows[0]),lineterminator='\n');writer.writeheader();writer.writerows(rows)
