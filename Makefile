PYTHON ?= python3
export PYTHONPATH := src

.PHONY: demo serve test lint reproduce-compounding reproduce-resume clean

demo:
	$(PYTHON) -m baton demo

serve:
	$(PYTHON) -m baton serve --static-dir docs

test:
	$(PYTHON) -m unittest discover -s tests -v

lint:
	$(PYTHON) -m compileall -q src tests scripts
	$(PYTHON) scripts/lint.py

reproduce-compounding:
	$(PYTHON) scripts/reproduce_compounding.py

reproduce-resume:
	$(PYTHON) scripts/reproduce_resume.py

clean:
	$(PYTHON) -m baton clean
