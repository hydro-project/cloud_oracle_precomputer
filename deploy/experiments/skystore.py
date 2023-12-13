import os
from deploy import Experiment

def build_skystore_experiments(*, latency_slos=[2.0, 4.0, 8.0], replication_factors=[1,2,3,5,8], region_selectors=None):
    fixed_args = dict(
        experiment_dir = os.path.join(os.getcwd(), "results", "skystore"),
        batch_size = 200,
        redundancy_elimination_workers = 80,
        #redundancy_elimination_workers = 1,
        #replication_factor = 0,
        optimizer="PrimalSimplex",
        use_clarkson=False,
        #profile= "dev",
        latency_file = os.path.join(os.getcwd(), "data", "latency_41943040.csv")
    )

    scaling = []

    for latency_slo in latency_slos:
        for region_selector in region_selectors:
            scaling += Experiment(region_selector=region_selector, **fixed_args, latency_slo=latency_slo, replication_factor=0).as_args(key="replication_factor", args=replication_factors)
    
    #scaling = Experiment(replication_factor=0, region_selector="aws", **fixed_args, latency_slo=latency_slos[-1]).as_args(key="replication_factor", args=replication_factors)
    #scaling = Experiment(replication_factor=0, region_selector="aws", **fixed_args).as_args(key="replication_factor", args=[1])
        #[Experiment(region_selector="gcp", **fixed_args, replication_factor=1)] #.as_args(key="latency_slo", args=latency_slos)
        #Experiment(region_selector="aws", **fixed_args, replication_factor=8).as_args(key="latency_slo", args=latency_slos) + \
        #Experiment(region_selector="aws", **fixed_args, replication_factor=10).as_args(key="latency_slo", args=latency_slos)
        #Experiment(region_selector="aws", **fixed_args, replication_factor=1).as_args(key="latency_slo", args=latency_slos) + \
        #Experiment(region_selector="aws", **fixed_args, replication_factor=3).as_args(key="latency_slo", args=latency_slos) + \
        #Experiment(region_selector="aws", **fixed_args, replication_factor=5).as_args(key="latency_slo", args=latency_slos)
        #Experiment(region_selector="azure", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
        #Experiment(region_selector="azure|aws", **fixed_args).as_replication_factors(min_replication_factor, min(4, max_replication_factor))

    return scaling

named_experiments = {
    "slos_aws": lambda: build_skystore_experiments(latency_slos=[8.0], replication_factors=[1,2,3,5,8], region_selectors=["aws"]),
    "no_slo": lambda: build_skystore_experiments(latency_slos=[None], replication_factors=[1,2,3,5,8], region_selectors=["aws"]),
}