# Agent Check: ServiceNow

## Overview

The ServiceNow check imports all CI(configuration items) as components and their relations with other CIs into StackState.

## Setup
### Installation

The ServieNow check is packaged with the Agent, so simply [install the Agent](https://app.datadoghq.com/account/settings#agent) and configure your ServiceNow instance.


### Configuration

Create a file `servicenow.yaml` in the Agent's `conf.d` directory:

```
init_config:

instances:
  - url: "https://<devID>.service-now.com" # The instance URL of servicenow
    basic_auth:
       user: example_user # basic auth user
       password: example_password # basic auth password

```

Restart the Agent to start sending ServiceNow component & relations to StackState.

### Validation

Run the Agent's `info` subcommand and look for `servicenow` under the Checks section:

```
  Checks
  ======
    [...]

    servicenow
    -------
      - instance #0 [OK]
      - Collected 0 metrics, 0 events, 1 service check, 1 topologies

    [...]
```

## Compatibility

The servicenow check is compatible with all major platforms.

## Data Collected
### Metrics

The ServiceNow check does not include any metrics at this time.

### Events
The ServiceNow check does not include any event at this time.

### Topology

The ServiceNow check sends all the CIs as components and sends all the CIs dependency as relations to stackstate. This topology information is gathered by StackState.

### Service Checks

**servicenow.can_connect**:

Returns CRITICAL if the Agent cannot connect to the configured `servicenow_instance_url`, otherwise OK.


## Points to Note

* Make sure the ServiceNow instance is up and running because this check or integration will not work if an instance is not online.
* Sometimes the relation between different CIs exist in ServiceNow and that particular CI(Configuration Item) itself doesn't exist in the ServiceNow and is a dummy in the relationship. So it's quite possible to get an error in StackState Synchronizations.
