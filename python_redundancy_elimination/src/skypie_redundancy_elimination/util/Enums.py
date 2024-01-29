from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Any, Dict

class NormalizationType(Enum):
    No = 0
    log10All = 1
    log10ColumnWise = 2
    standardScoreColumnWise = 3
    Mosek = 4 # Mosek's scaling

class MosekOptimizerType(str, Enum):
    Free = "free"
    InteriorPoint = "intpnt"
    PrimalSimplex = "primalSimplex"    
    # "free", "intpnt", "conic", "primalSimplex", "dualSimplex", "freeSimplex", "mixedInt"

    @staticmethod
    def from_str(label):
        if label in ('Free', 'free'):
            return MosekOptimizerType.Free
        elif label in ('InteriorPoint', 'intpnt'):
            return MosekOptimizerType.InteriorPoint
        elif label in ('PrimalSimplex', 'primalSimplex'):
            return MosekOptimizerType.PrimalSimplex
        else:
            return label
        
class OracleType(Enum):
    NONE=0
    SKYPIE = "SkyPIE"
    PYTORCH = "SkyPIE"
    MOSEK = "ILP"
    ILP = "ILP"
    KMEANS = "Kmeans"
    PROFIT = "Profit-based"
    CANDIDATES = "Candidates"

@dataclass
class OptimizerType:
    type: "MosekOptimizerType|str"
    useClarkson: bool = False
    useGPU: bool = False
    name: str = "" #field(init=False)
    implementation: OracleType = OracleType.NONE
    implementationArgs: Dict[str, Any] = field(default_factory=dict)
    iteration: "None|int" = None
    dsize: "None|int" = None
    strictReplication: bool = field(default=True)

    def __post_init__(self):
        self.name = str(self.type)

        if self.useClarkson:
            self.name += "_Clarkson"

        if self.useGPU:
            self.name += "_GPU"

        if self.iteration is not None:
            self.name += f"_iter{self.iteration}"

        if self.dsize is not None:
            self.name += f"_dsize{self.dsize}"

        if "strictReplication" in self.implementationArgs:
            self.strictReplication = self.implementationArgs["strictReplication"]

        """ if self.strictReplication:
            self.name += "_strictReplication"
        else:
            self.name += "_noStrictReplication" """

    def get_parameters(self) -> Dict[str, Any]:
        return {"Optimizer" : self.name,
            "Type": str(self.type),
            "Clarkson": self.useClarkson,
            "GPU": self.useGPU,
            "Iteration": self.iteration,
            "Division Size": self.dsize,
            "Strict Replication": self.strictReplication
        }

    def custom_copy(self, **kwargs) -> "OptimizerType":
        otDict = asdict(self)
        del otDict["name"]
        otDict.update(kwargs)
        return OptimizerType(**otDict)