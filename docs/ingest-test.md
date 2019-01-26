# Running a long continuous ingest test

Running continuous ingest for long periods on a cluster is a good way to
validate an Accumulo release.  This document outlines one possible way to do
that.  The commands below show how to start different types of ingest into 
multiple tables.  These commands assume the docker image was created for 
accumulo-testing and that docker swarm is available on the cluster.

```bash
# Number of nodes in cluster
NUM_NODES=10

# Start lots of processes writing to a single table as fast as possible
docker run --network="host" accumulo-testing cingest createtable
docker service create --network="host" --replicas $NUM_NODES --name ci \
   accumulo-testing cingest ingest \
   -o test.ci.ingest.pause.enabled=false

# Write data with pauses.  Should cause tablets to not have data in some write
# ahead logs.  But there is still enough write pressure to cause minor
# compactions.
#
# Some of the write ahead log recovery bugs fixed in 1.9.1 and 1.9.2 were not
# seen with continuous writes.
#
for i in $(seq 1 $NUM_NODES); do
  TABLE="cip_$i"
  docker run --network="host" accumulo-testing cingest createtable \
     -o test.ci.common.accumulo.table=$TABLE \
     -o test.ci.common.accumulo.num.tablets=$(( $NUM_NODES * 4 ))
  docker service create --network="host" --replicas 1 --name $TABLE \
     accumulo-testing cingest ingest \
     -o test.ci.common.accumulo.table=$TABLE \
     -o test.ci.ingest.pause.enabled=true
done

# Write very small amounts of data with long pauses in between.  Should cause
# data to be spread across lots of write ahead logs.  So little data is written
# that minor compactions may not happen because of writes.  If Accumulo does
# nothing then this will result in tablet servers having lots of write ahead
# logs.
#
# https://github.com/apache/accumulo/issues/854
#
for FLUSH_SIZE in 97 101 997 1009 ; do
  TABLE="cip_small_$FLUSH_SIZE"
  docker run --network="host" accumulo-testing cingest createtable \
     -o test.ci.common.accumulo.table=$TABLE \
     -o test.ci.common.accumulo.num.tablets=$(( $NUM_NODES * 2 ))
  docker service create --network="host" --replicas 1 --name $TABLE \
     accumulo-testing cingest ingest \
     -o test.ci.common.accumulo.table=$TABLE \
     -o test.ci.ingest.pause.enabled=true \
     -o test.ci.ingest.pause.wait.min=1 \
     -o test.ci.ingest.pause.wait.max=3 \
     -o test.ci.ingest.entries.flush=$FLUSH_SIZE
done
```

After starting the ingest, consider starting the agitator.  Testing with and
without agitation is valuable.  Let the ingest run for a period of 12 to 36
hours. Then stop it as follows.


```bash
# stop the agitator if started

# stop all docker services (assuming its only ingest started above, otherwise do not run)
docker service rm $(docker service ls -q)
```

After ingest stops verify the data.

```bash
# run verification map reduce jobs
mkdir -p logs
accumulo shell -u root -p secret -e tables | grep ci | while read table ; do
  nohup ./bin/cingest verify \
      -o test.ci.common.accumulo.table=$table \
      -o test.ci.verify.output.dir=/tmp/$table-verify \
          &> logs/verify_$table.log &
done
```


