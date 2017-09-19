#!/bin/bash

set -e

echo "This scripts removes directories from the virtualenv to avoid clashes between directories the stackstate agent exposes and packages in the virtualenv"

rm -rf venv/lib/python2.7/site-packages/tests

echo "Done"