.PHONY: all test clean

all: test

test:
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseSnapshotGC.cfg RoseSnapshotGC.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseSnapshotGCRetention.cfg RoseSnapshotGC.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseMetadata.cfg RoseMetadata.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageReplica.cfg RoseStorage.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageDisk.cfg RoseStorage.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageEC.cfg RoseStorage.tla

clean:
	rm -rf states
	rm -f *.bin
