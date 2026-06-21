.PHONY: all test clean

all: test

test:
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageReplica.cfg RoseStorage.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageDisk.cfg RoseStorage.tla
	java -cp ../tla2tools.jar tlc2.TLC -workers 4 -deadlock -config RoseStorageEC.cfg RoseStorage.tla

clean:
	rm -rf states
	rm -f *.bin
