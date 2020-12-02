#
# Full sequence of commands to benchmark influxdb and questdb on FreeBSD (ran on patricks desktop)
#
sudo portinstall influxdb
mkdir -p ~/tmp/go/src/github.com/timescale/
cd ~/tmp/go/src/github.com/timescale/
git clone git@github.com:questdb/tsbs.git
cd ~/tmp/go/src/github.com/timescale/tsbs/
git checkout questdb-tsbs-load 
GOPATH=~/tmp/go go build -v ./...
GOPATH=~/tmp/go go test -v github.com/timescale/tsbs/cmd/tsbs_load_questdb
GOPATH=~/tmp/go go install -v ./...
# Reduced data set
#~/tmp/go/bin/tsbs_generate_data --use-case="iot" --seed=123 --scale=4000 --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-01-01T01:00:00Z" --log-interval="10s" --format="influx" > /tmp/data
# Full data set
~/tmp/go/bin/tsbs_generate_data --use-case="iot" --seed=123 --scale=4000 --timestamp-start="2016-01-01T00:00:00Z" --timestamp-end="2016-01-04T00:00:00Z" --log-interval="10s" --format="influx" > /tmp/data
cat /tmp/data | ~/tmp/go/bin/tsbs_load_influx 
~/tmp/go/bin/tsbs_load_questdb -help
cat /tmp/data | ~/tmp/go/bin/tsbs_load_questdb 

