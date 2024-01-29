# SkyPIE Precomputation in Hydroflow

## Setup

The easiest setup is via the dev. container: Clone this repo., open in VS-code, re-open in container. This route only requires finalizing the solver setup. Alternatively, you can set up manually as described below.

### Solver setup

The second precomputation phase (filtering non-optimal decisions via redundancy elimination) requires an ILP solver.
Mosek is the default choice, which is an commercial solver that offers [free academic licenses](https://www.mosek.com/products/academic-licenses/). [Here is the Mosek setup guide](). Experiments for SIGMOD used this solver.

Alternatively, you can deinstall Mosek (the Python package) to fallback to CVXPY which automatically discovers installed solvers and picks one of these, [see setup of cvxpy](https://www.cvxpy.org/install/).

### Manual setup
1.  Setup Rust, e.g., via rustup (for Linux/Mac: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
2. [Follow Dockerfile for further setup steps ...](.devcontainer/Dockerfile)

## Usage

**Precomputation has to be executed from the root of this repo.** Hydro requires access to the setup Rust project in this repo and requires the dev infrastructure also for running the precomputation.

### Run predefined precomputation

**Precomputation is executed for predefined precomputation experiments** (scenarios) via: `python3 -m deploy --experiment [experiment name]`.

The predefined precomputation experiments can be listed via: `python3 -m deploy --list-experiments`.

Precomputation parameters can be customized via command line arguments, see `python3 -m deploy --help`.

**Precomputed oracles are stored in the [results](./results/) folder** of this repo (if not customized).
Each experiment has a subdirectory, where their precomputed oracles are stored in a directory hierarchy reflecting scenario parameters (cloud regions, object store types, replication factor range) and precomputation parameters.

### Specify new precomputation experiments

**New precomputation experiments can be specified in [deploy/experiments](./deploy/experiments/).**
First, create a new file in this directory. Then implement a builder function that instantiates a list of experiments with the desired parameters. Then create a dictionary of named experiments, with a name as a string key and the builder function as value. Finally, register your named experiments by extending the [\_\_init\_\_.py](./deploy/experiments/__init__.py) file.

### Run custom precomputation

Custom precomputation can be executed via command line arguments. Rather than giving an experiment name, all experiment parameters have to be specified as arguments, see `python3 -m deploy --help`.

## Utility Packages for SkyPIE Oracle

The Python package for querying the SkyPIE Oracle has utility packages from this repo:

- [Baselines](./baselines/): High performance implementation of baselines in Rust with Python bindings.
- [Serialization](./proto_messages/): Serialization of SkyPIE oracle for Rust and Python via ProtoBuf.

### Install in current machine

```
python3 -m pip install -e baselines
python3 -m pip install -e proto_messages
```

### Build packages
The script [util/build_python_packages.sh](./util/build_python_packages.sh) builds the utility packages for Python >=3.7 and your current environment (OS and HW architecture).
The resulting Python Wheels in [target/wheels](./target/wheels/) than can be installed in any compatible environment.