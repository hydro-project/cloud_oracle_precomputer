import skypie_proto_messages as m
import os
import pandas as pd

def get_key(data, key: str):
    if key.startswith("relative"):
        # Get n-th item
        pos = int(key.split("=")[1])
        key = list(data.keys())[pos]
    return data[key]

def load_scenario(*, stats_file: str, scenario_path: str):
    scenario_data = m.load_wrapper(stats_file)

    scenario = None
    paths = scenario_path.split("/")

    if paths[0] == "tier_advise" and paths[1] == "replication_factor":
        tier_adivse_data = scenario_data.tier_advise
        
        replication_factor_data = get_key(tier_adivse_data.replication_factor, paths[2])
        run = get_key(replication_factor_data.runs, paths[3])
        
        scenario = run
    else:
        raise Exception(f"Invalid scenario path: {scenario_path}")
    
    return scenario

def decision_stats(*, stats_file: str, scenario_path: str, threads: int=10):
    # Load the scenario
    scenario = load_scenario(stats_file=stats_file, scenario_path=scenario_path)

    # Cound the number of candidate decisions
    path = os.path.dirname(stats_file)
    files = [os.path.join(path, f) for f in scenario.candidate_partitions]
    no_candidates = m.count_decisions_parallel([f for f in files if os.path.isfile(f)], threads)

    # Count the number of optimal decisions
    optimizer_name = list(scenario.optimal_partitions_by_optimizer.keys())[0]
    optimal_partition_files = scenario.optimal_partitions_by_optimizer[optimizer_name].optimal_partitions
    no_optimal_partitions = m.count_decisions_parallel([os.path.join(path, f) for f in optimal_partition_files], threads)

    res = {
        "Enumerator Time (ns)": scenario.enumerator_time_ns,
        "Partition Time (ns)": scenario.partitioner_time_ns,
        "Max. Replication Factor": scenario.max_replication_factor,
        "Min. Replication Factor": scenario.min_replication_factor,
        "No. Candidates": no_candidates,
        "No. Optimal Partitions": no_optimal_partitions,
        "No. App Regions": scenario.no_app_regions,
        "No. Object Stores": scenario.no_object_stores,
        "No. Dimensions": scenario.no_dimensions,
        "Optimizer Name:": optimizer_name,
        "Optimizer Type": scenario.optimal_partitions_by_optimizer[optimizer_name].optimizer_type,
    }

    return res

def decision_stats_to_json(*, stats_dir: str, scenario_path: str="tier_advise/replication_factor/relative=0/relative=0", threads: int=10, stats_file_name: str="stats.proto.bin"):

    # Load all stats files in the directory
    stats = []
    for dirpath, _, filenames in os.walk(stats_dir):
        for filename in filenames:
            if stats_file_name == filename:
                stats_file = os.path.join(dirpath, filename)
                stats.append(decision_stats(stats_file=stats_file, scenario_path=scenario_path, threads=threads))

    # Convert to dataframe
    df = pd.DataFrame(stats)

    print(df)
    df.to_json(f"{stats_dir}/decision_stats.json")

stats_dir = "/home/vscode/sky-pie-precomputer/results/batch_size_scaling"
decision_stats_to_json(stats_dir=stats_dir)
