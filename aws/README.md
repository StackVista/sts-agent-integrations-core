# Aws Integration

## Overview

Get metrics from aws service in real time to:

* Visualize and monitor aws states
* Be notified about aws failovers and events.

## Installation

Install the `dd-check-aws` package manually or with your favorite configuration manager

## Configuration

Edit the `aws.yaml` file to point to your server and port, set the masters to monitor

## Validation

When you run `datadog-agent info` you should see something like the following:

    Checks
    ======

        aws
        -----------
          - instance #0 [OK]
          - Collected 39 metrics, 0 events & 7 service checks

## Compatibility

The aws check is compatible with all major platforms
