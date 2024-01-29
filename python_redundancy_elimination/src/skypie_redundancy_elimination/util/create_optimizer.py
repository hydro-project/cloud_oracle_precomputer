import torch

from .Enums import OracleType, OptimizerType, MosekOptimizerType
from typing import Any, Dict, List

def createOptimizer(*, optimizer: str, args: Dict[str, Any]) -> List[OptimizerType]:
    optimizers = []
    for useClarkson in args["useClarkson"]:
        useClarkson = useClarkson == "True"
        for useGPU in args["useGPU"]:
            implementation = OracleType.PYTORCH
            additional_impl_args = {}

            useGPU = useGPU == "True"
            if "lrs" == optimizer:
                implementation = OracleType.PYTORCH
            elif "LegoStore" == optimizer:
                implementation = OracleType.MOSEK
                additional_impl_args["access_cost_heuristic"] = True
            elif "ILP" == optimizer:
                implementation = OracleType.MOSEK
                # Mosek does not support GPU or Clarkson!
                if useGPU or useClarkson:
                    continue
            elif optimizer == OracleType.KMEANS.value:
                implementation = OracleType.KMEANS
            elif optimizer == OracleType.PROFIT.value:
                implementation = OracleType.PROFIT
            elif "PrimalSimplex" == optimizer:
                optimizer = MosekOptimizerType.PrimalSimplex
                implementation = OracleType.PYTORCH
            elif "InteriorPoint" == optimizer:
                optimizer = MosekOptimizerType.InteriorPoint
                implementation = OracleType.PYTORCH
            elif "Free" == optimizer:
                optimizer = MosekOptimizerType.Free
                implementation = OracleType.PYTORCH

            optimizers.append(OptimizerType(type=optimizer, useClarkson=useClarkson, useGPU=useGPU, implementation=implementation, implementationArgs=setImplementationArgs(implementation=implementation, args=args, additional_impl_args=additional_impl_args)))

    return optimizers

def setImplementationArgs(*, implementation: "OracleType", args: Dict[str, Any], additional_impl_args : Dict[str, Any] = {}) -> Dict[str, Any]:
    implementationArgs = {}
    if implementation == OracleType.PYTORCH:
        precision = args.get("precision", "float64")
        dataType = torch.float64
        if precision == "float32":
            dataType = torch.float32
        elif precision == "float16":
            dataType = torch.float16
        elif precision == "bfloat16":
            dataType = torch.bfloat16
        implementationArgs["data_type"] = dataType

        if "torchDeviceRayShooting" in args:
            implementationArgs["device_query"] = args["torchDeviceRayShooting"]
            #implementationArgs["device_check"] = args["torchDeviceRayShooting"]
        if "device_query" in args:
            implementationArgs["device_query"] = args["torchDeviceRayShooting"]
        
        if implementationArgs.get("device_query", "PLACEHOLDER") == "mps":
            implementationArgs["data_type"] = torch.float32

    elif implementation == OracleType.MOSEK:

        implementationArgs["networkPriceFileName"] = args.get("networkPriceFile", PACKAGE_RESOURCES.networkPriceFileName)
        implementationArgs["storagePriceFileName"] = args.get("storagePriceFile", PACKAGE_RESOURCES.storagePriceFileName)

        if 'noStrictReplication' in args:
            implementationArgs["strictReplication"]=not args['noStrictReplication']
        if 'minReplicationFactor' in args:
            implementationArgs["minReplicationFactor"] = args["minReplicationFactor"]

        implementationArgs["network_latency_file"] = args.get("network_latency_file", None)
        implementationArgs["latency_slo"] = args.get("latency_slo", None)
    elif implementation == OracleType.KMEANS:
        if not "networkPriceFile" in args or not "storagePriceFile" in args:
            raise ValueError("Network and storage price files must be provided for Kmeans optimizer.")
        implementationArgs["minReplicationFactor"] = args["minReplicationFactor"]
        implementationArgs["networkPriceFileName"] = args["networkPriceFile"]
        implementationArgs["storagePriceFileName"] = args["storagePriceFile"]
        implementationArgs["strictReplication"]=not args['noStrictReplication']
        if "max_iterations" in args:
            implementationArgs["max_iterations"] = args["max_iterations"]
        if "threshold" in args:
            implementationArgs["threshold"] = args["threshold"]
            
    elif implementation == OracleType.PROFIT:
        if not "networkPriceFile" in args or not "storagePriceFile" in args:
            raise ValueError("Network and storage price files must be provided for Profit-based optimizer.")
        implementationArgs["networkPriceFileName"] = args["networkPriceFile"]
        implementationArgs["storagePriceFileName"] = args["storagePriceFile"]

    implementationArgs["threads"] = args.get("threads", 0)

    implementationArgs.update(additional_impl_args)

    return implementationArgs