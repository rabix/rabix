#!/bin/sh
nosetests rabix/tests && python -m pep8 . --max-line-length=120
