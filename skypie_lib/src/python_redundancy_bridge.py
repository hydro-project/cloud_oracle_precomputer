import subprocess
import sys
from skypie_redundancy_elimination import redundancyElimination, redundancyEliminationClarkson
from skypie_redundancy_elimination.util import EnhancedJSONEncoder, createOptimizer
import numpy as np

def load_args(*, dsize=1000, use_clarkson=False, optimizerThreads=1, verbose_=0, optimizer="PrimalSimplex"):
    """
    Shim for loading the arguments for redundancy elimination (hiding legacy setup from Rust code)
    """
    global algoArgs, optimizerType, useClarkson, verbose

    #verbose_ = 3

    # Ignore args: torchDeviceRayShooting: str, normalize: bool, optimizerThreads: int, nonnegative: bool, optimizerType: str
    
    optimizers = [ "lrs", "ILP", "PrimalSimplex", "InteriorPoint", "Free"]
    assert optimizer in optimizers, f"Optimizer {optimizer} not in {optimizers}"

    #optimizer = "InteriorPoint"

    args = {
        "useClarkson": ["True" if  use_clarkson else "False"],
        "useGPU": [False],
        "torchDeviceRayShooting": "cpu",
        #"device_query": "cpu",
        #"device_check": "cpu",
    }

    algoArgs = {
        "torchDeviceRayShooting": "cpu",
        #"normalize": "False", # Ignore normalization
        "optimizerThreads": optimizerThreads,
        "nonnegative": True,
        "verbose": verbose_
    }

    if optimizer == "lrs" and optimizerThreads > 1:
        print("WARN: lrs does not support multithreading, setting optimizerThreads has not effect!")

    # Load details of optimizer configuration
    optimizerType = createOptimizer(optimizer=optimizer, args=args)[0]
    optimizerType = optimizerType.custom_copy(dsize=dsize)

    algoArgs["optimizerType"] = optimizerType

    if optimizerType.useGPU == False and True:
        algoArgs["torchDeviceRayShooting"] = "cpu"
    elif optimizerType.useGPU and "cuda" in algoArgs.get("torchDeviceRayShooting", ""):
        pass

        #import torch
        #if not torch.cuda.is_available():
        #    raise RuntimeError("cuda is unavailable, but requested for rayshooting")

    #return algoArgs

    # Initialize optimizer once
    #algoArgs = load_args()
    optimizerType = algoArgs["optimizerType"]
    useClarkson = optimizerType.useClarkson
    del algoArgs["optimizerType"]
    verbose = algoArgs.get("verbose", 0)

    assert optimizerType.useClarkson == use_clarkson, f"Optimizer {optimizer} useClarkson {useClarkson} != {use_clarkson}"
    assert useClarkson == use_clarkson, f"Optimizer {optimizer} useClarkson {useClarkson} != {use_clarkson}"

"""
let optimizer_type = r#"
            {
                "type": "intpnt",
                "useClarkson": true,
                "useGPU": false,
                "name": "MosekOptimizerType.InteriorPoint_Clarkson_iter0_dsize1000",
                "implementation": 1,
                "implementationArgs": {
                    "device_query": "cuda:1",
                    "device_check": "cuda:1"
                },
                "iteration": 0,
                "dsize": 1000,
                "strictReplication": true
            }
            "#
            .to_string();
"""
import json
def get_optimizer_json():
    global optimizerType
    return (optimizerType.name, json.dumps(optimizerType, cls=EnhancedJSONEncoder))

def redundancy_elimination(inequalities: np.array):
    """
    Wrapper for redundancy elimination, picking implementation according to loaded arguments.
    """
    # , useClarkson: bool, algoArgs: Dict[Any, Any]

    global algoArgs, optimizerType, useClarkson, verbose
    
    #timerLocal = Timer()
    timerLocal = None
    diff = inequalities.shape[0]

    if verbose > 0:
        print("redundancy_elimination", inequalities.shape, flush=True)

    if optimizerType.type == "Pareto" and False: # Ignore pareto
        tensor = torch.from_numpy(algoArgs["inequalities"][:,1:-1].copy())
        #tensor *= -1
        resOther = compute_pareto_frontier(tensor, device=algoArgs["torchDeviceRayShooting"], math=False)
        res3 = [r for r in resOther[:diff]]
        pass
    elif optimizerType.type == "lrs":
        #res3 = redundancy_elimination_lrs(inequalities)
        res3 = redundancy_elimination_cdd(inequalities)
        res3 = res3[:diff]
    else:
        if useClarkson:
            res3 = redundancyEliminationClarkson(inequalities=inequalities, optimizerType=optimizerType.type, timer=timerLocal, **algoArgs)
            res3 = [r for (r, _) in res3[:diff]]
        else:
            res3 = redundancyElimination(inequalities=inequalities, optimizerType=optimizerType.type, timer=timerLocal, **algoArgs)
            res3 = [r for (r, _) in res3[:diff]]

    if verbose > 0:
        print(f"Redundancy elim. result:{res3}")

    nonredundant = [pos for pos, r in enumerate(res3) if r == True]

    if verbose > 0:
        sys.stdout.flush()

    return nonredundant

from fractions import Fraction
def redundancy_elimination_lrs(inequalities: np.array):
    """
    Experimental integration of exact redundancy elimination of lrslib (using rational numbers)
    """

    global verbose

    # Start redund as subprocess
    redund = subprocess.Popen(["redund"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    # Write format header into stdin of redund
    redund.stdin.write(
f"""SkyPIE's Redundancy Elimination Polytope
H-representation
begin
{inequalities.shape[0]+inequalities.shape[1]-1} {inequalities.shape[1]} rational
""")

    # Write out lrs format: multiply by -1 and convert to string fractions
    for row in inequalities:
        for x in row:
            fraction_string = str(Fraction(x*-1).limit_denominator(1000000))
            redund.stdin.write(f"{fraction_string} ")
        redund.stdin.write("\n")

    n = inequalities.shape[1]
    for i in range(1, n):
        redund.stdin.write("0 ")
        for j in range(1, n):
            if i != j:
                redund.stdin.write("0 ")
            else:
                redund.stdin.write("1 ")
        redund.stdin.write("\n")
            

    redund.stdin.write("end\n")
    redund.stdin.close()

    if verbose > 1:
        print("redundancy_elimination_lrs running for: ", inequalities.shape, flush=True)

    # Read stdout line by line to find redundant rows
    redundant_rows = []
    found = False
    for line in redund.stdout.readlines():
        if verbose > 1:
            print(line, end="")
        if found and line == "\n":
            found = False
            break
        if found:
            try:
                redundant_rows.append(int(line)-1)
            except:
                pass
        elif line.endswith("redundant row(s) found:\n"):
            found = True
        else:
            found = False

    # Wait for the process to finish and get its output
    stderr = redund.stderr.read()
    #stdout, stderr = redund.communicate()

    # Close the process when finished
    redund.wait()

    # Print the output
    if stderr:
        print(stderr)

    # Return flags whether the rows are non-redundant
    return [i not in redundant_rows for i in range(inequalities.shape[0])]

import tempfile
cdd_temp_file = tempfile.NamedTemporaryFile(delete=True, mode="w+")
def redundancy_elimination_cdd(inequalities: np.array):
    """
    Experimental integration of exact redundancy elimination of cddlib (using mixed precision)
    """

    global verbose

    m = inequalities.shape[0]
    n = inequalities.shape[1]

    m += n-1 # Add non negative constraints

    # Create an input file
    #input_file_path = os.path.join(os.path.dirname(__file__), "redundancy_elimination_cdd_input.txt")
    
    # Write format header into stdin of redund
    cdd_temp_file.write(
f"""SkyPIE's Redundancy Elimination Polytope
H-representation
nonnegative
begin
{m} {n} rational
""")

    # Write out lrs format: multiply by -1 and convert to string fractions
    for row in inequalities:
        for x in row:
            fraction_string = str(Fraction(x*-1).limit_denominator(1000000))
            cdd_temp_file.write(f"{fraction_string} ")
        cdd_temp_file.write("\n")

    for i in range(1, n):
        cdd_temp_file.write("0 ")
        for j in range(1, n):
            if i != j:
                cdd_temp_file.write("0 ")
            else:
                cdd_temp_file.write("1 ")
        cdd_temp_file.write("\n")
        
    cdd_temp_file.write("end\n")

    if verbose > 1:
        print("redundancy_elimination_lrs running for: ", inequalities.shape, flush=True)

        print(cdd_temp_file.read())

    cdd_temp_file.flush()

    
    # Start redund as subprocess
    #redund = subprocess.Popen(["redund"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    redund = subprocess.Popen(["/home/vscode/cddlib/cddlib/src/redundancies"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
    redund.stdin.write(cdd_temp_file.name)
    redund.stdin.close()

    # Read stdout line by line to find redundant rows
    redundant_rows = []
    found = False
    for line in redund.stdout.readlines():
        if verbose > 1:
            print(line, end="")
        if line.startswith("redundant rows:"):
            redundant_rows = [int(x)-1 for x in line.split(" ")[2:-1]]
            break

    # Wait for the process to finish and get its output
    stderr = redund.stderr.read()
    #stdout, stderr = redund.communicate()

    # Close the process when finished
    redund.wait()

    # Print the output
    if stderr:
        print(stderr)

    # Return flags whether the rows are non-redundant
    return [i not in redundant_rows for i in range(inequalities.shape[0])]

def redundancy_elimination_dummy(inequalities: np.array):
    print("redundancy_elimination_test", inequalities.shape, flush=True)
    return [i for i in range(inequalities.shape[0])]

def redundancy_elimination_test():
    coefficients = [
        [0.5, 1.0], # Non redundant
        [1.0, 0.5], # Non redundant
        [2.0, 2.0]  # Redundant
    ]
    inequalities = np.array([
        [0] + [ c*-1 for c in coefficients_i ] + [1] for coefficients_i in coefficients
    ])
    coefficients = np.array(coefficients)

    nonredundant = redundancy_elimination(inequalities)

    expected = [0, 1]
    assert nonredundant == expected, f"Expected {expected}, got {nonredundant}"

    print("redundancy_elimination passed", flush=True)

    # Test LRS as well
    #redundancy_elimination_lrs(inequalities)
    #assert nonredundant == expected, f"Expected {expected}, got {nonredundant}"

    #print("redundancy_elimination_lrs passed", flush=True)

if __name__ == "__main__":
    load_args(use_clarkson=False)
    print(get_optimizer_json())
    redundancy_elimination_test()

    # lrs
    #load_args(use_clarkson=False, optimizer="lrs")
    #print(get_optimizer_json())
    #redundancy_elimination_test()