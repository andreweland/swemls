test:
	cd simulator; go test
	cd scaffolding; python3 simulator_test.py
	python3 generator/generator_test.py