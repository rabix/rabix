#!/bin/sh
python -m pep8 . --max-line-length=100 && nosetests -e integration rabix/tests
