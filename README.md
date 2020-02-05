# prm-gp2gp-data-pipeline

This repo contains the data pipeline responsible for deriving GP2GP metrics from data sources such as NMS.

## Running

### Extracting Spine data from NMS

Run the following query for the desired time range and save the results as a csv.

```
index="spine2-live" service="gp2gp"
| search interactionID="urn:nhs:names:services:gp2gp/*"
| fields _time, conversationID, GUID, interactionID, fromNACS, toNACS, messageRef, jdiEvent
| fields - _raw
```

## Developing

Common development workflows are defined in the `tasks` script.

These generally work by running a command in a virtual environment configured via `tox.ini`.

### Prerequisites

- Python 3
- [Tox](https://tox.readthedocs.io/en/latest/#)

### Running the tests

`./tasks test`

### Running tests, linting, and type checking

`./tasks validate`

### Auto Formatting

`./tasks format`

### Dependency Scanning

`./tasks check-deps`
