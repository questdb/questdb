from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET
pom = ET.parse('core/pom.xml')
ns = {'p': 'http://maven.apache.org/POM/4.0.0'}
argline = ' '.join(pom.find('p:properties/p:argLine', ns).text.split())
args = argline + ' -XX:-UseTLAB -XX:FlightRecorderOptions=stackdepth=128 -XX:StartFlightRecording=settings=/tmp/questdb-task9f/allocation-profile.jfc,filename=/tmp/questdb-task9f/build-allocation.jfr,dumponexit=true'
with Path('/tmp/questdb-task9f/build-allocation-profile.txt').open('w') as output:
    result = subprocess.run(['mvn', '-pl', 'core', 'test', '-P', 'build-rust-library,qdbr-release', '-Dtest=IntHashJoinBuildTest#testReusableBuildGrowthDoesNotAllocateHeap', '-DargLine='+args], stdout=output, stderr=subprocess.STDOUT)
print('profile exit', result.returncode)
with Path('/tmp/questdb-task9f/build-allocation.json').open('w') as output:
    subprocess.run(['jfr', 'print', '--json', '--events', 'jdk.ObjectAllocationOutsideTLAB', '--stack-depth', '128', '/tmp/questdb-task9f/build-allocation.jfr'], stdout=output, check=True)
