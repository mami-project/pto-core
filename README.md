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
For running multiple instances in parallel you should install pto-core in seperate python virtual environments.

## Configuration
Each instance needs to run on it's own databases. A configuration file can be created with the following command (add `--help` for more information about options). In addition it prints out the necessary MongoDB commands for creating the database users and their permissions.
```
python -m ptocore.createconfig <name> <outputdir>
```

## Running
The observatory core consists of four python programs each of them run separately and don't have direct connections to each other (only through the database). You should make sure that only one instance of each program runs (using the same configuration).

```
python -m ptocore.sensor <outputdir>/<name>.json
python -m ptocore.supervisor <outputdir>/<name>.json
python -m ptocore.validator <outputdir>/<name>.json
```
