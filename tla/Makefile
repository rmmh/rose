.PHONY: all test clean

all: test

test:
	java -cp ../tla2tools.jar tlc2.TLC -config Rose2PC.cfg Rose2PC.tla
	java -cp ../tla2tools.jar tlc2.TLC -config RoseSingleNode.cfg Rose2PC.tla
	java -cp ../tla2tools.jar tlc2.TLC -config RoseECMulti.cfg RoseEC.tla
	java -cp ../tla2tools.jar tlc2.TLC -config RoseECSingle.cfg RoseEC.tla

clean:
	rm -rf states
	rm -f *.bin
