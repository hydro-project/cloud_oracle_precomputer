# SkyPIE Precomputation in Hydroflow

## Solver setup

Mosek is the default choice, if not installed there is an automatic fallback to CVXPY and its preferred solver.

Experiments were done with the commercial Mosek solver. Mosek offers free academic liecenses.
Alternative (free) solvers are supported via cvxpy, [see setup of cvxpy](https://www.cvxpy.org/install/).