#!/bin/sh
python -m pep8 . --max-line-length=120 && nosetests rabix/tests
