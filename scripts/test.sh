#!/bin/sh
python -m pep8 . --max-line-length=100 && \
nosetests -e integration --with-doctest --doctest-options=+IGNORE_EXCEPTION_DETAIL rabix
