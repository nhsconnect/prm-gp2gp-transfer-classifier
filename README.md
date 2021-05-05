# prm-gp2gp-data-pipeline

This repo contains the data pipeline responsible for deriving GP2GP metrics from data sources such as NMS.

## Running

### Extracting Spine data from NMS

Run the following query for the desired time range and save the results as a csv.

```
index="spine2vfmmonitor" service="gp2gp" logReference="MPS0053d"
| table _time, conversationID, GUID, interactionID, messageSender, messageRecipient, messageRef, jdiEvent, toSystem, fromSystem
```

## Developing

Common development workflows are defined in the `tasks` script.

### Prerequisites

- Python 3.9
- Pipenv
- [Docker](https://www.docker.com/get-started)

### Running the unit and integration tests

`./tasks test`

### Running the end to end tests

`./tasks e2e-test`

### Running tests, linting, and type checking

`./tasks validate`

### Running tests, linting, and type checking in a docker container

This will run the validation commands in the same container used by the GoCD pipeline.

`./tasks dojo-validate`

### Auto Formatting

`./tasks format`

### Dependency Scanning

`./tasks check-deps`

### ODS Data

If you need ODS codes and names of all active GP practices
[please check this out.](https://github.com/nhsconnect/prm-gp2gp-ods-downloader#readme)

### Data Platform Pipeline

This pipeline will derive GP2GP metrics and metadata for practices produced by the ODS Portal Pipeline. It does this by performing a number of transformations on GP2GP messages provided by NMS.

The following examples show how to run this pipeline. Run `platform-metrics-pipeline --help` for usage details.

#### Outputting to local directory

Example: `platform-metrics-pipeline --month 1 --year 2021 --organisation-list-file "data/organisation-list.json" --input-files "data/jan.csv.gz,data/feb.csv.gz" --output-directory "data"`

#### Outputting to S3 bucket

Example: `platform-metrics-pipeline --month 1 --year 2021 --organisation-list-file "data/organisation-list.json" --input-files "data/jan.csv.gz,data/feb.csv.gz" --output-bucket "example-bucket"`

- When outputting to AWS ensure the environment has the appropriate access.
- Note this will use the year and month as part of the s3 key structure, as well 'v2' (data pipeline output version). 

