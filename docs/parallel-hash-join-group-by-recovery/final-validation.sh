#!/bin/bash
set -euo pipefail
cd /home/puzpuzpuz/projects/questdb
recovery_test_suites=$(python3 - <<'PY'
import csv
with open('docs/parallel-hash-join-group-by-semantics/regressions.csv') as source:
    print(','.join(row['suite'].rsplit('.', 1)[-1] for row in csv.DictReader(source)))
PY
)
printf 'mvn -pl core test -P build-rust-library,qdbr-release -Dtest=%q\n' "$recovery_test_suites" > /tmp/questdb-task9f/final-validation-commands.txt
mvn -pl core test -P build-rust-library,qdbr-release -Dtest="$recovery_test_suites" > /tmp/questdb-task9f/final-regressions.txt 2>&1
python3 - <<'PY'
import csv
from pathlib import Path
import xml.etree.ElementTree as ET
with open('docs/parallel-hash-join-group-by-semantics/regressions.csv') as source:
    suites = [r['suite'] for r in csv.DictReader(source)]
with open('/tmp/questdb-task9f/final-regressions.csv', 'w') as out:
    w = csv.writer(out, lineterminator='\n')
    w.writerow(['suite','tests','failures','errors','skipped','seconds'])
    for suite in suites:
        root=ET.parse(Path('core/target/surefire-reports')/('TEST-'+suite+'.xml')).getroot()
        w.writerow([suite]+[root.attrib[k] for k in ('tests','failures','errors','skipped','time')])
PY
mvn -pl benchmarks -am package -P build-rust-library,qdbr-release -DskipTests -Dmaven.test.skip=true > /tmp/questdb-task9f/final-build.txt 2>&1
cp benchmarks/target/benchmarks.jar /tmp/questdb-task9f/final.jar
git diff HEAD -- core benchmarks > /tmp/questdb-task9f/final-source.patch
sha256sum /tmp/questdb-task9f/final.jar /tmp/questdb-task9f/final-source.patch > /tmp/questdb-task9f/final-artifacts.sha256
bash benchmarks/parallel-hash-join-group-by-allocation.sh /tmp/questdb-task9f/final-allocation > /tmp/questdb-task9f/final-allocation.txt 2>&1
pattern="^($(paste -sd '|' benchmarks/parallel-hash-join-group-by-recovery-cases.txt))$"
CASE_PATTERN="$pattern" BENCHMARK_JAR=/tmp/questdb-task9f/final.jar BENCHMARK_REVISION=12ff320ae7+recovery-final BREAKER_MODE=active bash benchmarks/parallel-hash-join-group-by-v1.sh /tmp/questdb-task9f/final-candidate > /tmp/questdb-task9f/final-candidate.txt 2>&1
python3 benchmarks/compare-hash-join-group-by-recovery.py /tmp/questdb-task9f/recovery/reference /tmp/questdb-task9f/final-candidate /tmp/questdb-task9f/final-comparison.csv > /tmp/questdb-task9f/final-comparison.txt 2>&1
