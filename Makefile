test:
	cd simulator; go test
	cd scaffolding; python3 simulator_test.py
	cd generator; python3 generator_test.py