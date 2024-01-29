import json
import torch
import numpy as np
from dataclasses import is_dataclass, asdict
from .Enums import OracleType

class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if is_dataclass(o):
            return asdict(o)
        elif isinstance(o, OracleType):
            return o.value
        elif isinstance(o, np.integer):
            return int(o)
        elif isinstance(o, np.floating):
            return float(o)
        elif isinstance(o, np.ndarray):
            return o.tolist()
        elif isinstance(o, set):
            return list(o)
        elif isinstance(o, torch.dtype):
            return o.__str__()
        else:
            return super().default(o)