# Running a bulk ingest test

Continous ingest supports bulk ingest in addition to live ingest. A map reduce
job that generates rfiles using the tables splits can be run.  This can be run
in a loop like the following to continually bulk import data.

```bash
# create the ci table if necessary
./bin/cingest createtable

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

