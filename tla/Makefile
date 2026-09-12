.PHONY: all test test-txn test-snapshots test-prefix mutations snapshot-coverage snapshot-mutations prefix-mutations prefix-liveness check-fast check-large test-evidence clean

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

test-evidence:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest test_check_models.py

check-fast: test-evidence
	python3 check_models.py --tier fast

check-large: test-evidence
	python3 check_models.py --tier large --timeout 43200

.PHONY: test-retry-retention retry-mutations

test-retry-retention:
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseRetryRetention.cfg RoseRetryRetention.tla
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseRetryRetentionLive.cfg RoseRetryRetention.tla

retry-mutations: test-retry-retention
	python3 check_retry_mutations.py

.PHONY: test-maintenance maintenance-mutations

test-maintenance:
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseMaintenance.cfg RoseMaintenance.tla
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseMaintenanceLive.cfg RoseMaintenance.tla

maintenance-mutations: test-maintenance
	python3 check_maintenance_mutations.py

.PHONY: test-repair-epochs repair-mutations

test-repair-epochs:
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseRepairEpoch.cfg RoseRepairEpoch.tla
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseRepairEpochLive.cfg RoseRepairEpoch.tla

repair-mutations: test-repair-epochs
	python3 check_repair_mutations.py

.PHONY: test-repair-recovery repair-recovery-mutations

test-repair-recovery:
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseRepairRecovery.cfg RoseRepairRecovery.tla
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseRepairRecoveryLive.cfg RoseRepairRecovery.tla

repair-recovery-mutations: test-repair-recovery
	python3 check_repair_recovery_mutations.py

.PHONY: test-relocation-outcomes relocation-mutations

test-relocation-outcomes:
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseRelocationOutcome.cfg RoseRelocationOutcome.tla
	java -Xmx512m -cp ../tla2tools.jar tlc2.TLC -workers 2 -deadlock -config RoseRelocationOutcomeLive.cfg RoseRelocationOutcome.tla

relocation-mutations: test-relocation-outcomes
	python3 check_relocation_mutations.py
