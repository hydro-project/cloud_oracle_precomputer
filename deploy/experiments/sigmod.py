import os
from deploy import Experiment

def build_real_trace_experiments():
    
    fixed_args = dict(
        output_dir = os.path.join(os.getcwd(), "results", "precomputation_real_trace"),
        batch_size = 200,
        redundancy_elimination_workers = 80,
        replication_factor = 1,
        replication_factor_max = 4,
        optimizer="PrimalSimplex",
        use_clarkson=False,
        #profile= "dev"
    )
    
    return [Experiment(region_selector="aws", **fixed_args)]

def build_scaling_experiments():
    min_replication_factor = 1
    max_replication_factor = 5
    fixed_args = dict(
        output_dir = os.path.join(os.getcwd(), "results", "precomputation_scaling"),
        batch_size = 200,
        redundancy_elimination_workers = 80,
        replication_factor = 0,
        optimizer="PrimalSimplex",
        use_clarkson=False,
        #profile= "dev"
    )
    
    scaling = Experiment(region_selector="aws-eu", object_store_selector="General Purpose", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
        Experiment(region_selector="aws-eu", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
        Experiment(region_selector="aws", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
        Experiment(region_selector="azure", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
        Experiment(region_selector="azure|aws", **fixed_args).as_replication_factors(min_replication_factor, min(4, max_replication_factor))

    # Order by replication factor
    scaling.sort(key=lambda e: e.replication_factor)

    # Also save candidates of small experiments
    for e in scaling:
        e.output_candidates = e.region_selector == "aws-eu"

    return scaling

def build_scaling_experiments_lrs():
    fixed_args = dict(
        output_dir = os.path.join(os.getcwd(), "results", "precomputation_scaling_lrs"),
        batch_size = 200,
        redundancy_elimination_workers = 80,
        #redundancy_elimination_workers = 1,
        replication_factor = 0,
        #optimizer="PrimalSimplex",
        optimizer="lrs",
        use_clarkson=False,
        #profile= "dev"
    )
    scaling = Experiment(region_selector="aws-eu", object_store_selector="General Purpose", **fixed_args).as_replication_factors(1, 5) + \
        Experiment(region_selector="aws-eu", **fixed_args).as_replication_factors(1, 5) + \
        Experiment(region_selector="aws", **fixed_args).as_replication_factors(1, 2) + \
        Experiment(region_selector="azure", **fixed_args).as_replication_factors(1, 2) + \
        Experiment(region_selector="azure|aws", **fixed_args).as_replication_factors(1, 2)

    # Order by replication factor
    scaling.sort(key=lambda e: e.replication_factor)

    return scaling

def build_scaling_experiment_candidates():

    experiments = build_scaling_experiments_lrs()

    for e in experiments:
        e.output_dir = os.path.join(os.getcwd(), "results", "precomputation_scaling_small_candidates")
        e.output_candidates = True
        e.optimizer = "PrimalSimplex"

    return experiments

def build_precomputation_batching_experiments(large=False):
    fixed_args = dict(
        output_dir = os.path.join(os.getcwd(), "results", "batch_size_scaling"),
        redundancy_elimination_workers = 80,
        region_selector="aws|azure",
        object_store_selector="",
        replication_factor=3,
        optimizer="PrimalSimplex",
        use_clarkson=False,
        output_candidates=True,
        #profile="dev"
    )
    batch_sizes = [200, 500, 1000]

    if large:
        fixed_args["output_dir"] = os.path.join(os.getcwd(), "results", "batch_size_scaling_large")
        fixed_args["replication_factor"] = 5

    return [Experiment(**fixed_args, batch_size=b) for b in batch_sizes]

def build_cpu_scaling_experiments():
    fixed_args = dict(
        output_dir = os.path.join(os.getcwd(), "results", "cpu_scaling"),
        #replication_factor=5,
        replication_factor=3,
        batch_size=200,
        optimizer="PrimalSimplex",
        use_clarkson=False,
        region_selector="aws",
        object_store_selector="",
        #profile="dev"
    )
    worker_numbers = [(20 * i)-2 for i in [4, 2, 1]]
    batch_size_scaling = [Experiment(redundancy_elimination_workers=w,**fixed_args) for w in worker_numbers]

    return batch_size_scaling

def build_accuracy_small_experiments():
    fixed_args = dict(
        output_dir = os.path.join(os.getcwd(), "results", "accuracy"),
        replication_factor=2,
        batch_size=200,
        #optimizer="lrs",
        use_clarkson=False,
        #region_selector="aws",
        #object_store_selector="General Purpose",
        redundancy_elimination_workers=10,
        #profile="dev"
    )
    optimizers = ["PrimalSimplex", "InteriorPoint", "lrs"]
    # Full replication range and list of optimizers for aws
    batch_size_scaling = [Experiment(optimizer=o, **fixed_args).as_replication_factors(1, 5) for o in optimizers]
    # Replication up to 2 and lrs for the rest
    max_replication_factor = 2
    batch_size_scaling.append(Experiment(region_selector="aws-eu", object_store_selector="General Purpose",optimizer="lrs", **fixed_args).as_replication_factors(1, max_replication_factor))
    batch_size_scaling.append(Experiment(region_selector="aws-eu",optimizer="lrs", **fixed_args).as_replication_factors(1, max_replication_factor))
    batch_size_scaling.append(Experiment(region_selector="azure",optimizer="lrs", **fixed_args).as_replication_factors(1, max_replication_factor))
    batch_size_scaling.append(Experiment(region_selector="azure|azure",optimizer="lrs", **fixed_args).as_replication_factors(1, max_replication_factor))
    
    # Flatten the list of lists to a list of elements
    flattened_list = [item for sublist in batch_size_scaling for item in sublist]

    return flattened_list

named_experiments = {
    "scaling": build_scaling_experiments,
    "real_trace": build_real_trace_experiments,
    #"scaling_lrs": build_scaling_experiments_lrs,
    #"scaling_candidates": build_scaling_experiment_candidates,
    "batch_size": build_precomputation_batching_experiments,
    #"cpu_scaling": build_cpu_scaling_experiments,
    #"accuracy": build_accuracy_small_experiments,
}