.PHONY: all test test-txn test-snapshots test-prefix mutations snapshot-coverage snapshot-mutations prefix-mutations prefix-liveness clean

all: test

test: test-txn test-snapshots test-prefix
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseMetadata.cfg RoseMetadata.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageReplica.cfg RoseStorage.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageDisk.cfg RoseStorage.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageEC.cfg RoseStorage.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageMultiNode.cfg RoseStorage.tla

clean:
	rm -rf states
	rm -f *.bin

test-txn:
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseTxnCommit.cfg RoseTxnCommit.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseTxnCommitGlobal.cfg RoseTxnCommit.tla

mutations: test-txn
	python3 check_txn_mutations.py

test-snapshots:
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseSnapshotGC.cfg RoseSnapshotGC.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseSnapshotGCRetention.cfg RoseSnapshotGC.tla

snapshot-coverage:
	python3 check_snapshot_coverage.py

snapshot-mutations: test-snapshots
	python3 check_snapshot_mutations.py

test-prefix:
	java -Xmx512m -XX:+UseParallelGC -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RosePrefixRecovery.cfg RosePrefixRecovery.tla
	java -Xmx512m -XX:+UseParallelGC -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RosePrefixRecoveryLive.cfg RosePrefixRecoveryLive.tla

prefix-mutations: test-prefix
	python3 check_prefix_mutations.py

prefix-liveness: test-prefix
	python3 check_prefix_liveness.py
