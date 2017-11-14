# XL Deploy check

## Overview

The StackState Agent can collect topology and events from XL Deploy, including:

* Hosts, containers and middleware topology including relations
* Deployment events

## Setup
### Installation

The XL Deploy check is included in the StackState Agent package, so simply [install the Agent](https://app.datadoghq.com/account/settings#agent) on your XL Deploy servers.

### Configuration
#### Connect the Agent

Create a basic `xl-deploy.yaml` in the Agent's `conf.d` directory to connect it to the XL Deploy server:

```
init_config:

instances:
  - server: localhost
    user: xld-agent
    pass: <YOUR_CHOSEN_PASSWORD>
    port: <YOUR_XL_DEPLOY_PORT> # e.g. 4516
```

Restart the Agent to start sending XL Deploy metrics to StackState.

### Validation

Run the Agent's `info` subcommand and look for `xl-deploy` under the Checks section:

```
  Checks
  ======

    [...]

    xl-deploy
    -----
      - instance #0 [OK]
      - Collected 168 metrics, 0 events & 1 service check

    [...]
```

If the status is not OK, see the Troubleshooting section.

## Compatibility

The XL Deploy integration is supported on versions 6.0 and up

## Data Collected
### Events
The XL Deploy check includes deployment events describing deployments that have occurred in XL Deploy:

```
[{'event_type': 'deployment',
  'host': '10.0.0.1',
  'msg_text': 'Deployment of PetClinic-war 1.0',
  'msg_title': 'Deployment of PetClinic-war 1.0',
  'source_type_name': 'deployment',
  'tags': '{ "application": "PetClinic-war", "version": "1.0", "affects": "Infrastructure/localhost/file01", "environment": "LOCAL" }',
  'timestamp': u'2017-09-23T11:53:44.337+0000'},
 {'event_type': 'deployment',
  'host': '10.0.0.1',
  'msg_text': 'Deployment of PetClinic-war 1.0',
  'msg_title': 'Deployment of PetClinic-war 1.0',
  'source_type_name': 'deployment',
  'tags': '{ "application": "PetClinic-war", "version": "1.0", "affects": "Infrastructure/10.0.0.1/tc-server/vh2/petclinic", "environment": "TEST" }',
  'timestamp': u'2017-11-13T15:20:56.502+0000'},
 {'event_type': 'deployment',
  'host': '10.0.0.1',
  'msg_text': 'Deployment of ZooKeeper 1.0',
  'msg_title': 'Deployment of ZooKeeper 1.0',
  'source_type_name': 'deployment',
  'tags': '{ "application": "ZooKeeper", "version": "1.0", "affects": "Infrastructure/10.0.0.1/tc-server/vh1/zookeeper", "environment": "TEST" }',
  'timestamp': u'2017-11-13T15:21:24.768+0000'}]
```
