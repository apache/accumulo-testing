# Running a bulk ingest test

Continous ingest supports bulk ingest in addition to live ingest. A map reduce
job that generates rfiles using the tables splits can be run.  This can be run
in a loop like the following to continually bulk import data.

```bash
# create the ci table if necessary
./bin/cingest createtable

# Optionally, consider lowering the split threshold to make splits happen more 
# frequently while the test runs.  Choose a threshold base on the amount of data
# being imported and the desired number of splits.
# 
#   accumulo shell -u root -p secret -e 'config -t ci -s table.split.threshold=32M'

for i in $(seq 1 10); do
   # run map reduce job to generate data for bulk import
   ./bin/cingest bulk /tmp/bt/$i
   # ask accumulo to import generated data
   echo -e "table ci\nimportdirectory /tmp/bt/$i/files true" | accumulo shell -u root -p secret
done
./bin/cingest verify
```

Another way to use this in test is to generate a lot of data and then bulk import it all at once as follows.

```bash
for i in $(seq 1 10); do
  ./bin/cingest bulk /tmp/bt/$i
done

# Optionally, copy data before importing.  This can be useful in debugging problems.
hadoop distcp hdfs://$NAMENODE/tmp/bt hdfs://$NAMENODE/tmp/bt-copy

for i in $(seq 1 10); do
  (
    echo table ci
    echo "importdirectory /tmp/bt/$i/files true"
  ) | accumulo shell -u root -p secret
  sleep 5
done

./bin/cingest verify
```

Bulk ingest could be run concurrently with live ingest into the same table.  It
could also be run while the agitator is running.

After bulk imports complete, could run the following commands in the Accumulo shell
to see if there are any BLIP (bulk load in progress) or load markers.  There should
not be any.

```
scan -t accumulo.metadata -b ~blip -e ~blip~
scan -t accumulo.metadata -c loaded
```

Additionally check that no rfiles exists in the source dir.

```bash
hadoop fs -ls -R /tmp/bt | grep rf
```

The referenced counts output by `cingest verify` should equal :

```
test.ci.bulk.map.task * (test.ci.bulk.map.nodes -1) * num_bulk_generate_jobs
``` 

The unreferenced counts output by `cingest verify` should equal :

```
test.ci.bulk.map.task * num_bulk_generate_jobs
``` 

Its possible the counts could be slightly smaller because of collisions. However collisions 
are unlikely with the default settings given there are 63 bits of randomness in the row and 
30 bits in the column.  This gives a total of 93 bits of randomness per key.

