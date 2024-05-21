import os
from dataclasses import dataclass, field

@dataclass
class Experiment:
    replication_factor: int
    region_selector: str
    redundancy_elimination_workers: int
    replication_factor_max: int = None
    batch_size: int = 400
    output_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), "experiments"))
    hydro_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), "skypie_lib"))
    data_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), "data"))
    profile: str = "release"
    object_store_selector: str = ""
    experiment_name: "str|None" = None
    experiment_dir_full: str = "" # This is set in __post_init__
    optimizer: str = "PrimalSimplex"
    use_clarkson: bool = False
    output_candidates: bool = False
    latency_slo: float = None
    latency_file: str = None

    def __post_init__(self):
        
        # Create a translation table that replaces all unfriendly characters with -
        unfriendly_chars = ["|", "*", " ", "(", ")", "[", "]", "{", "}", ":", ";", ",", ".", "<", ">", "/", "\\", "?", "'", "\"", "\n", "\t", "\r", "\v", "\f"]
        translation_table = str.maketrans({c: "-" for c in unfriendly_chars})

        clarkson = "use_clarkson" if self.use_clarkson else "no_clarkson"

        # Use the translation table to replace all unfriendly characters
        friendly_region = self.region_selector.translate(translation_table)
        friendly_object_store = self.object_store_selector.translate(translation_table)
        if len(friendly_object_store) > 0:
            friendly_region_and_object_store = f"{friendly_region}-{friendly_object_store}"
        else:
            friendly_region_and_object_store = friendly_region

        friendly_latency_slo = f"latency_slo-{str(self.latency_slo).translate(translation_table)}" if self.latency_slo is not None else ""

        # Create the name of the experiment
        paths = ([self.experiment_name] if self.experiment_name is not None else []) + \
            ([friendly_latency_slo] if self.latency_slo is not None else []) + \
            [friendly_region_and_object_store, f"{self.replication_factor}-{self.replication_factor_max or self.replication_factor}", str(self.redundancy_elimination_workers), str(self.batch_size), str(self.optimizer), clarkson]
        self.experiment_dir_full = os.path.join(self.output_dir, *paths)

    def copy(self, **kwargs):
        kwargs = {**self.__dict__, **kwargs}
        return Experiment(**kwargs)
    
    def as_args(self,*, key, args):
        return [self.copy(**{key:a}) for a in args]

    def as_replication_factors(self, min_replication_factor, max_replication_factor):
        return self.as_args(key="replication_factor", args=range(min_replication_factor, max_replication_factor + 1))
        #return [ self.copy(replication_factor=r) for r in range(min_replication_factor, max_replication_factor + 1)]