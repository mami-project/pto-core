# pto-core

## Requirements
- MongoDB
- Hadoop
- Apache Spark

## Installation
Clone the repository
```
git clone https://github.com/mami-project/pto-core.git
```
and install using pip:
```
pip install -e pto-core/
```
For running multiple instances of different versions in parallel you should install pto-core in seperate python virtual environments.

## Configuration
The programs are launched with an arbitrary number of paths to json files.
All json file are automatically merged togheter (the latter overwriting values of the former).
It is advised to have a base config `base.json` and an environment-specific config file for each environment.

See conf/base.json for an example base configuration file.

Each instance needs to run on it's own databases. A configuration file can be created with the following
command (add `--help` for more information about options). In addition it prints out the necessary MongoDB
commands for creating the database users and their permissions. Where `NAME` is the name you want to give the new environment and
`PATH` is an absolute path the analyzer modules will be stored. See a lot more options by running it with `--help`.
Using ptocore-createconfig is not mandatory, see conf/prod.json for an example how it will look like.
```
ptocore-createconfig <NAME> <PATH>
```

## Running
The observatory core consists of four python programs each of them run separately and don't have direct connections
to each other (only through the database). You should make sure that only one instance
of each program runs with the same configuration.

```
ptocore-sensor <base>.json <env.json>
ptocore-supervisor <base>.json <env.json>
ptocore-validator <base>.json <env.json>
```

The administrative RESTful API is powered by flask. A standalone server is started with the following lines.
The first line is only needed if you use a python virtual environment and want to start the service with a shell script.
(Don't forget to change paths according to your needs.)
```
source venv/bin/activate
export FLASK_APP=ptocore.admin
export FLASK_DEBUG=1
export PTO_CONFIG_FILES=/path/to/conf/base.json:/path/to/conf/env.json
flask run --host=0.0.0.0 --port=33525
```


