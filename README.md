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

- Python 3.9. Use [pyenv](https://github.com/pyenv/pyenv) to easily switch Python versions.
- [Pipenv](https://pypi.org/project/pipenv/). Install by running `python -m pip install pipenv`
- [Docker](https://www.docker.com/get-started) - version 3.1.0 or higher
- [dojo](https://github.com/kudulab/dojo)

#### Installing the correct versions of pip and python

Ensure you are not within a virtual environment (run `deactivate` if you are in one)

1. Run `pyenv install 3.9.4`
2. Follow step 3 from [here](https://github.com/pyenv/pyenv#basic-github-checkout)
3. Run `pyenv global 3.9.4`
4. For the following steps open another terminal.
5. Run `python -m pip install pipenv` to install pipenv using the updated python environment.
6. Run `python -m pip install -U "pip>=21.1"`
   - `pyenv global` should output the specific python version specified rather than `system`.
   - Both `python --version` and `pip --version` should point to the versions you have specified.
   - `ls -l $(which pipenv)` should output `.../.pyenv/shims/pipenv` rather than `...Cellar...` (which is a brew install).

#### Python virtual environment

From the base directory of the project, create a python3 virtual environment by running `./tasks devenv`, then to activate it run `pipenv shell`.

To deactivate the virtual environment run `deactivate`.

To remove the virtual environment and clear the cache, run `pipenv --rm && pipenv --clear`.

Run the following commands in the virtual environment:

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

- If this fails when running outside of Dojo, see [troubleshooting section](### Troubleshooting)

### ODS Data

If you need ODS codes and names of all active GP practices
[please check this out.](https://github.com/nhsconnect/prm-gp2gp-ods-downloader#readme)

### Data Platform Pipeline

This pipeline will derive GP2GP metrics and metadata for practices produced by the ODS Portal Pipeline. It does this by performing a number of transformations on GP2GP messages provided by NMS.

### Troubleshooting

#### Checking dependencies fails locally due to pip

If running `./tasks check-deps` fails due to an outdated version of pip, yet works when running it in dojo (i.e. `./tasks dojo-deps`), then the local python environment containing pipenv may need to be updated (using pyenv instead of brew - to better control the pip version).
Ensure you have pyenv installed (use `brew install pyenv`).
Perform the following steps:

1. Run `brew uninstall pipenv`
2. Run the steps listed under [Installing correct version of pip and python](#installing-correct-version-of-pip-and-python)
3. Now running `./tasks check-deps` should pass.

#### Python virtual environments

If you see the below notice when trying to activate the python virtual environment, run `deactivate` before trying again.

> Courtesy Notice: Pipenv found itself running within a virtual environment, so it will automatically use that environment, instead of creating its own for any project. You can set PIPENV_IGNORE_VIRTUALENVS=1 to force pipenv to ignore that environment and create its own instead. You can set PIPENV_VERBOSITY=-1 to suppress this warning.
