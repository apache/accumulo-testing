# Garbage Collection Simulation (GCS)

GCS is a test suite that generates random data in a way that is similar to the
Accumulo garbage collector.  This test has a few interesting properties.  First
it generates data at a much higher rate than the garbage collector would on a
small system, simulating a much larger system.  Second, it has a much more
complex read and write pattern than continuous ingest that involve multiple
processes writing, reading, and deleting data.  Third, the random data is
verifiable like continuous ingest.  At any point the test can be stopped and
the data verified.  This test will not generate as much data as continuous
ingest.  The test will reach a steady state in terms of the number of entries
stored in Accumulo.  The size of this steady state is determined by the number
of generators running and the setting `test.gcs.maxActiveWork`, increasing
either will increase the steady state size.

## Data Types

This test has the following types of data that are stored in a single accumulo table.

 * **Item** : An item is something that should be deleted, unless it is referenced.
   Each item is part of a group.  Items correspond to files and groups
   correspond to bulk imports, in the Accumulo GC.
 * **Item reference** : A reference to an item that should prevent it from
   being deleted.  An item can have multiple item references.
 * **Group reference** : A reference to a group that should prevent the
   deletion of any items in a group.  This corresponds to blip markers in the
   Accumulo GC.
 * **Deletion candidate** : An entry that signifies an item is a candidate for deletion.

## Invariants

Hopefully the test data never violates the following rules

 * An Item should always be referenced by an Item reference, group reference or
   a deletion candidate.  There is one exception to this, items with a value of
  `NEW`.  Its ok for new items to be unreferenced.
 * An Item reference should always have a corresponding item.

## Executable components

The test has the following executable components.

 * **setup** : creates and configures table
 * **generator** : continually generates items, references, and candidates.
   These are generated randomly and spaced out over time, interleaving
   unrelated entries. The generator should never create data that violates the
   test invariants.  Multiple generators can be run concurrently.
 * **collector** : continually scans the data looking for unreferenced
   candidates to delete.  Should only run one at a time.
 * **verifier** :  This processes checks the table to ensure the test
   invariants have not been violated.  Before running this, the generator and
   collector processes should be stopped.

Running `./bin/gcs` will print help that shows how to run these processes.

Below is simple script that runs a test scenario.

```bash
./bin/gcs setup

for i in $(seq 1 10); do
  ./bin/gcs generate &
done

./bin/gcs collect &

sleep 12h

pkill -f gcs
./bin/gcs verify
```
