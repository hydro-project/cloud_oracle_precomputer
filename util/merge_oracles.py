import os
from dataclasses import dataclass
from typing import List
import skypie_proto_messages as m
from skypie.load_proto import select_scenario

@dataclass
class MergeInput:
    path: str
    output_path_prefix: str
    replace_with_candidates: bool = False

def merge_oracles(*, oracles: List[MergeInput], output_path: str, output_file: str, scenario_path = "tier_advise/replication_factor/relative=0/relative=0"):
    new_wrapper = None
    new_oracle = None
    for input_oracle in oracles:
        print(f"Merging {input_oracle.path}")
        wrapper = m.load_wrapper(input_oracle.path)

        """ 
        combine_py(&mut self, other: &Self, original_path: &str, output_path: &str, output_path_suffix: &str, replace_with_candidates: bool)
        """
        
        
        if new_wrapper is not None:
            print(f"Combining {input_oracle.path}")
            os.makedirs(os.path.join(output_path, input_oracle.output_path_prefix), exist_ok=True)
            new_wrapper.combine_py(wrapper, os.path.dirname(input_oracle.path), output_path, input_oracle.output_path_prefix, input_oracle.replace_with_candidates)
        else:
            new_wrapper = wrapper

    new_wrapper.save_py(os.path.join(output_path, output_file))

merge_oracles_input = [
    MergeInput(output_path_prefix="", replace_with_candidates=False, path="/home/vscode/sky-pie-precomputer/results/precomputation_scaling_lrs/aws/1/80/200/lrs/no_clarkson/stats.proto.bin"),
    MergeInput(output_path_prefix="aws/2", replace_with_candidates=False, path="/home/vscode/sky-pie-precomputer/results/precomputation_scaling_lrs/aws/2/80/200/lrs/no_clarkson/stats.proto.bin"),
    MergeInput(output_path_prefix="aws/3", replace_with_candidates=False, path="/home/vscode/sky-pie-precomputer/results/precomputation_scaling/aws/3/80/200/PrimalSimplex/no_clarkson/stats.proto.bin"),
    MergeInput(output_path_prefix="aws/4", replace_with_candidates=False, path="/home/vscode/sky-pie-precomputer/results/precomputation_scaling/aws/4/80/200/PrimalSimplex/no_clarkson/stats.proto.bin"),
    #MergeInput(output_path_prefix="aws/5", replace_with_candidates=False, path="/home/vscode/sky-pie-precomputer/results/precomputation_scaling/aws/5/80/200/PrimalSimplex/no_clarkson/stats.proto.bin"),
]

merge_oracles(oracles=merge_oracles_input, output_path="/home/vscode/sky-pie-precomputer/results/precomputation_real_trace/aws", output_file="stats")

