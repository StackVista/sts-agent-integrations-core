#!/bin/bash

set -e

echo "This scripts removes directories from the virtualenv to avoid clashes between directories the stackstate agent exposes and packages in the virtualenv"

set -x

rm -rf $(python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")/tests

set +x

echo "Done"