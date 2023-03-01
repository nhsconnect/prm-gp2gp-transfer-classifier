# prm-gp2gp-transfer-classifier

This repo contains the transfer classifier (part of the data pipeline) responsible for deriving GP2GP transfer data.
The transfer outcome is categorised by inferring the outcome of each GP2GP transfer using the logs produced by SPINE.

SICBL = Sub Integrated Care Board Location

## Running

The data pipeline can be installed via `python setup.py install`, or packaged into a docker container via `docker build`.
Alternatively, you can download one of the docker containers already published to ECR.

The main code entrypoint is via `python -m prmdata.pipeline.main`.

### Extracting Spine data from Splunk

Run the following query for the desired time range and save the results as a csv.

```
index="spine2vfmmonitor" service="gp2gp" logReference="MPS0053d"
| table _time, conversationID, GUID, interactionID, messageSender, messageRecipient, messageRef, jdiEvent, toSystem, fromSystem
```

### Configuration

#### Date range options
- When START_DATETIME and END_DATETIME are **not** passed, then it will output one transfer parquet file for yesterday's midnight minus the number of cutoff days.
- When START_DATETIME and END_DATETIME are both passed (which both must be at midnight), then the data retrieved will be from the daily spine exporter output, and it will output a daily transfer parquet file for each date within the date range.
Example of ISO-8601 datetime that is specified for START_DATETIME or END_DATETIME - "2022-01-19T00:00:00Z".

#### Environment variables

Configuration is achieved via the following environment variables:


| Environment variable        | Description                                                                                                                                                       | 
|-----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| OUTPUT_TRANSFER_DATA_BUCKET | Bucket to write categorised transfers.                                                                                                                            |
| INPUT_SPINE_DATA_BUCKET     | Bucket to read raw spine logs from.                                                                                                                               |
| START_DATETIME              | Optional ISO-8601 datetime specifying start of date range of transfer classification, on a daily basis.                                                           |
| END_DATETIME                | Optional ISO-8601 datetime specifying end of date range of transfer classification, on a daily basis.                                                             |
| S3_ENDPOINT_URL             | Optional argument specifying which S3 to connect to.                                                                                                              |
| CONVERSATION_CUTOFF_DAYS    | Optional argument specifying how many days to classify a transfer. Defaults to 0 (which means there is no cutoff).                                                |


## Developing

Common development workflows are defined in the `tasks` script.

This project is written in Python 3.9.

### Recommended developer environment

- [pyenv](https://github.com/pyenv/pyenv) to easily switch Python versions.
- [Pipenv](https://pypi.org/project/pipenv/) to manage dependencies and virtual environments.
- [dojo](https://github.com/kudulab/dojo) and [Docker](https://www.docker.com/get-started)
  to run test suites in the same environment used by the CI/CD server.

#### Installing pyenv
```
brew install pyenv
```

#### Configure your shell's environment for Pyenv

```
For zsh:
echo 'eval "$(pyenv init --path)"' >> ~/.zprofile
echo 'eval "$(pyenv init -)"' >> ~/.zshrc
```

#### Install new python and set as default

```
pyenv install 3.9.6
pyenv global 3.9.6
```

#### Installing pipenv and updating pip

In a new shell, run the following:
```
python -m pip install -U pipenv
python -m pip install -U "pip>=23.0.1"
```

#### Build a dev env

In a new shell, in the project directory run:

```
./tasks devenv
```

This will create a python virtual environment containing all required dependencies.

#### Configure SDK

To find out the path of this new virtual environment, run:

```
pipenv --venv
```

Now you can configure the IDE. The steps for IntelliJ are following:
1. Go to `File -> Project Structure -> SDK -> Add SDK -> Python SDK -> Existing environments`
2. Click on three dots, paste the virtual environment path from before, and point to the python binary.
   The path should look like this: `/Users/janeDoe/.local/share/virtualenvs/prm-spine-exporter-NTBCQ41T/bin/python3.9`

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

- If this fails when running outside of Dojo, see [troubleshooting section](#Troubleshooting)


### Troubleshooting

#### Checking dependencies fails locally due to pip

If running `./tasks check-deps` fails due to an outdated version of pip, yet works when running it in dojo (i.e. `./tasks dojo-deps`), then the local python environment containing pipenv may need to be updated (using pyenv instead of brew - to better control the pip version).
Ensure you have pyenv installed (use `brew install pyenv`).
Perform the following steps:

1. Run `brew uninstall pipenv`
2. Run the steps listed under [Install new python and set as default](#install-new-python-and-set-as-default) and [Installing pipenv and updating pip](#installing-pipenv-and-updating-pip)
3. Now running `./tasks check-deps` should pass.

#### Python virtual environments

If you see the below notice when trying to activate the python virtual environment, run `deactivate` before trying again.

> Courtesy Notice: Pipenv found itself running within a virtual environment, so it will automatically use that environment, instead of creating its own for any project. You can set PIPENV_IGNORE_VIRTUALENVS=1 to force pipenv to ignore that environment and create its own instead. You can set PIPENV_VERBOSITY=-1 to suppress this warning.
