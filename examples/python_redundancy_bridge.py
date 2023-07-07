from skypie import *
import numpy as np

def load_args():
    # Ignore args: torchDeviceRayShooting: str, normalize: bool, optimizerThreads: int, nonnegative: bool, optimizerType: str
    
    #optimizers = [ "lrs", "ILP", "PrimalSimplex", "InteriorPoint", "Free"]

    optimizer = "InteriorPoint"

    args = {
        "useClarkson": [False],
        "useGPU": [False],
        "torchDeviceRayShooting": "cpu",
        #"device_query": "cpu",
        #"device_check": "cpu",
    }

    algoArgs = {
        "torchDeviceRayShooting": "cpu",
        #"normalize": "False", # Ignore normalization
        "optimizerThreads": 1,
        "nonnegative": True,
        "verbose": 0
    }

    # Load details of optimizer configuration
    optimizerType = createOptimizer(optimizer=optimizer, args=args)[0]

    algoArgs["optimizerType"] = optimizerType

    if optimizerType.useGPU == False and True:
        algoArgs["torchDeviceRayShooting"] = "cpu"
    elif optimizerType.useGPU and "cuda" in algoArgs.get("torchDeviceRayShooting", ""):
        pass

        #import torch
        #if not torch.cuda.is_available():
        #    raise RuntimeError("cuda is unavailable, but requested for rayshooting")

    return algoArgs

# Initialize optimizer once
algoArgs = load_args()
optimizerType = algoArgs["optimizerType"]
useClarkson = optimizerType.useClarkson
del algoArgs["optimizerType"]

def redundancy_elimination(inequalities: np.array):
    print("redundancy_elimination", inequalities.shape)
    print("inequalities", inequalities)
    # , useClarkson: bool, algoArgs: Dict[Any, Any]

    global algoArgs, optimizerType, useClarkson
    
    timerLocal = Timer()
    diff = inequalities.shape[0]

    if optimizerType.type == "Pareto" and False: # Ignore pareto
        tensor = torch.from_numpy(algoArgs["inequalities"][:,1:-1].copy())
        #tensor *= -1
        resOther = compute_pareto_frontier(tensor, device=algoArgs["torchDeviceRayShooting"], math=False)
        res3 = [r for r in resOther[:diff]]
        pass
    else:
        if useClarkson:
            res3 = redundancyEliminationClarkson(inequalities=inequalities, optimizerType=optimizerType.type, timer=timerLocal, **algoArgs)
            res3 = [r for (r, _) in res3[:diff]]
        else:
            res3 = redundancyElimination(inequalities=inequalities, optimizerType=optimizerType.type, timer=timerLocal, **algoArgs)
            res3 = [r for (r, _) in res3[:diff]]

    nonredundant = [pos for pos, r in enumerate(res3) if r == True]

    return nonredundant

def redundancy_elimination_test():
    coefficients = [
        #[-3.0],
        #[-2.0]
        #[3, 0.5], # f_0(x,y) = 3x + .5y
        [0.5, 3], # f_1(x,y) = .5x + 3y
        [1.5, 1.5], # f_2(x,y) = 1.5x + 1.5y
        [2.5, 2.5] # f_2(x,y) = 1.5x + 1.5y
    ]
    inequalities = np.array([
        [0] + [ c*-1 for c in coefficients_i ] + [1] for coefficients_i in coefficients
    ])
    coefficients = np.array(coefficients)

    nonredundant = redundancy_elimination(inequalities)

    expected = [0, 1]
    assert nonredundant == expected, f"Expected {expected}, got {nonredundant}"

if __name__ == "__main__":
    redundancy_elimination_test()