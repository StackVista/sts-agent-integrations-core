#!/bin/bash

function check {
  diff $1 $2 || vimdiff $2 $1
}

for i in mesos_master_topology splunk_event splunk_topology; do
  check ../sts-agent/checks.d/$i.py $i/check.py
  check ../sts-agent/conf.d/$i.yaml.example $i/conf.yaml.example
  check ../sts-agent/tests/checks/mock/test_$i.py $i/test_$i.py
  for j in `ls ../sts-agent/tests/checks/fixtures/$i` ; do
    check ../sts-agent/tests/checks/fixtures/$i/$j $i/ci/fixtures/$j
  done
done
