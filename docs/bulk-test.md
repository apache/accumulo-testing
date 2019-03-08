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

(
  echo "table ci"
  for i in $(seq 1 10); do
    echo "importdirectory /tmp/bt/$i/files true"
  done
) | accumulo shell -u root -p secret
./bin/cingest verify
```

Bulk ingest could be run concurrently with live ingest into the same table.  It
could also be run while the agitator is running.

