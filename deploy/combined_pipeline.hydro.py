import asyncio
import hydro
import os
import argparse
from dataclasses import dataclass, field
import json
from typing import Any
import sys

@dataclass
class Tee:
    file: Any
    stdout: Any = field(init=False)

    def __post_init__(self):
        #self.file = open(name, mode)
        self.stdout = sys.stdout
        sys.stdout = self
    def close(self):
        self.flush()
        sys.stdout = self.stdout
        self.file.close()
    def write(self, data):
        self.file.write(data)
        self.stdout.write(data)
    def flush(self):
        self.file.flush()

def create_scale_up_service(deployment, *args, num_scale_up, display_id, kwargs_instances=dict(), **kwargs):
    """
    Creates a scale-up service by generating 'n' identical instances of a sercice, i.e.,HydroflowCrate.

    Parameters:
    - deployment: The deployment object used to create the scale-out service.
    - *args: Variable length argument list to be passed to the HydroflowCrate constructor.
    - num_scale_up: The number of instances to be created for the scale-up service.
    - display_id: The display ID prefix for each instance. The display ID of each instance will be in the format "{display_id}:{i}",
                  where i is the index of the instance.
    - **kwargs: Keyword arguments to be passed to the HydroflowCrate constructor.

    Returns:
    - A generator that yields the created instances of HydroflowCrate.

    Example usage:
    ```
    deployment = Deployment()
    scale_out_service = create_scale_up_service(deployment, arg1, arg2, num_scale_out=3, display_id="my service", kwarg1=val1, kwarg2=val2)
    for instance in scale_out_service:
        # Do something with each instance
    ```
    """

    for i in range(num_scale_up):
        yield deployment.HydroflowCrate(
            *args,
            display_id=f"{display_id}:{i}",
            **kwargs,
            **(kwargs_instances.get(i, {}))
        )

def send_to_demux(src_service, dest_services):
    """
    Sends data from the source service to a demultiplexer, which distributes the data to multiple destination services.
    It defines consecutive indexes for the destination services starting from 0, in the order they are passed in the list.

    Parameters:
    - src_service: The source service that provides the data to be sent.
    - dest_services: A list of destination services that will receive the data.

    Returns:
    None

    Example:
    send_to_demux(source_service, [destination_service1, destination_service2, destination_service3])
    """

    src_service.ports.output.send_to(hydro.demux({
        i: s.ports.input.merge() for i, s in enumerate(dest_services)
    }))

@dataclass
class Experiment:
    replication_factor: int
    region_selector: str
    redundancy_elimination_workers: int
    batch_size: int = 400
    experiment_dir: str = field(default_factory=lambda: os.path.join(os.getcwd(), "experiments"))
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
            [friendly_region_and_object_store, str(self.replication_factor), str(self.redundancy_elimination_workers), str(self.batch_size), str(self.optimizer), clarkson]
        self.experiment_dir_full = os.path.join(self.experiment_dir, *paths)

    def copy(self, **kwargs):
        kwargs = {**self.__dict__, **kwargs}
        return Experiment(**kwargs)
    
    def as_args(self,*, key, args):
        return [self.copy(**{key:a}) for a in args]

    def as_replication_factors(self, min_replication_factor, max_replication_factor):
        return self.as_args(key="replication_factor", args=range(min_replication_factor, max_replication_factor + 1))
        #return [ self.copy(replication_factor=r) for r in range(min_replication_factor, max_replication_factor + 1)]

async def precomputation(*, e: Experiment):
    print("Running experiment:", e)

    num_workers = 2 + e.redundancy_elimination_workers
    num_workers = num_workers - 1 # Write choices services is currently not sending a done signal

    args = {
        "region-selector": e.region_selector,
        "object-store-selector": e.object_store_selector,
        "replication-factor": e.replication_factor,
        #"output-file-name": "/dev/null",
        "batch-size": e.batch_size,
        "network-file": f"{e.data_dir}/network_cost_v2.csv",
        "object-store-file": f"{e.data_dir}/storage_pricing.csv",
        "redundancy-elimination-workers": e.redundancy_elimination_workers,
        #"output_candidates": ""
        "experiment-name": e.experiment_dir_full,
        "influx-host": "flaminio.millennium.berkeley.edu",
        "num-workers": num_workers,
        "optimizer": e.optimizer,
    }

    if e.latency_slo is not None and e.latency_file is not None:
        args["latency-slo"] = e.latency_slo
        args["latency-file"] = e.latency_file


    # Convert args to a list of strings with --key=value format
    args = [f"--{key}={value}" for key, value in args.items()]
    
    if e.use_clarkson:
        args.append("--use-clarkson")

    # Worker specific args
    optimal_policies_name_prefix = "optimal"
    candidate_policies_name_prefix = "candidates"
    output_file_extension = "proto.bin"

    kwargs_instances={
        i: {"args":(
            args + [
                '--worker-id', f"{i}",
                '--executor-name', f"candidate_executor_{i}",
                '-o', os.path.join(e.experiment_dir_full, f'{optimal_policies_name_prefix}_{i}.{output_file_extension}')]
            )
        } for i in range(e.redundancy_elimination_workers)
    }

    if e.output_candidates:
        #"--output-candidates-file-name", os.path.join(e.experiment_dir_full, f"{candidate_policies_name_prefix}_{i}.{output_file_extension}")
        for i in range(e.redundancy_elimination_workers):
            kwargs_instances[i]["args"] += ["--output-candidates-file-name", os.path.join(e.experiment_dir_full, f"{candidate_policies_name_prefix}_{i}.{output_file_extension}")]

    deployment = hydro.Deployment()

    localhost = deployment.Localhost()

    write_choices_service = deployment.HydroflowCrate(
        src=e.hydro_dir,
        profile=e.profile,
        example="write_choices_simple_demux_launch",
        on=localhost,
        display_id="write_choices",
        args=args + ['--worker-id', "10000"]
    )

    logging_service = deployment.HydroflowCrate(
        src=e.hydro_dir,
        profile=e.profile,
        example="logger_launch",
        on=localhost,
        display_id="logger",
        args=args + [
            '--worker-id', "10001",
            '-o', os.path.join(e.experiment_dir_full, f'{optimal_policies_name_prefix}.{output_file_extension}'),
            "--output-candidates-file-name", os.path.join(e.experiment_dir_full, f"{candidate_policies_name_prefix}.{output_file_extension}")
        ]
    )
    
    candidates_service = [s for s in create_scale_up_service(deployment,
        num_scale_up=e.redundancy_elimination_workers,
        profile=e.profile,
        src=e.hydro_dir,
        example="candidate_and_reduce_launch",
        on=localhost,
        display_id="candidate_reduce",
        kwargs_instances=kwargs_instances
        )]
    
    python_receiver_service = deployment.CustomService(localhost, external_ports=[])

    ## Connect named ports of services
    # Sender service's "output" port to receiver service's "input" port
    send_to_demux(write_choices_service, candidates_service)

    # Send all timing information of the candidate services to the logging service
    for s in candidates_service:
        s.ports.time_output.send_to(logging_service.ports.time_input.merge())
        s.ports.count_output.send_to(logging_service.ports.count_input.merge())
        s.ports.done_output.send_to(logging_service.ports.done_input.merge())

    # Send timing information of the write choices service to the logging service
    write_choices_service.ports.time_output.send_to(logging_service.ports.time_input.merge())

    python_receiver_port = python_receiver_service.client_port()
    logging_service.ports.done_output.send_to(python_receiver_port)
    
    # Deploy and start, blocking until deployment is complete
    await deployment.deploy()

    # Create directory for experiment
    os.makedirs(e.experiment_dir_full, exist_ok=True)
    
    # Write experiment parameters to json file
    with open(os.path.join(e.experiment_dir_full, "experiment.json"), "w") as f:
        json.dump(e.__dict__, f, indent=4)

    # Direct stdout of this python script to a file
    log_file = os.path.join(e.experiment_dir_full, "experiment.log")
    tee = Tee(open(log_file, "w"))
    
    await deployment.start()

    receiver_connection = await (await python_receiver_port.server_port()).into_source()

    # Wait for the logging service to finish
    async for raw_msg in receiver_connection:
        msg = bytes(raw_msg)
        print(msg, raw_msg)
        if raw_msg == [42]:
            break

    # Terminate deployment
    print("DONE!")

    tee.close()

def get_args(args):
    parser = argparse.ArgumentParser(description="Run the SkyPie precomputer.")
    parser.add_argument("--replication-factor", type=int, default=3, help="The replication factor to use for the precomputation.")
    parser.add_argument("--redundancy-elimination-workers", type=int, default=10, help="The number of workers to use for the redundancy elimination.")
    parser.add_argument("--region-selector", type=str, default="", help="The region selector to use for the precomputation.")
    parser.add_argument("--experiment-name", type=str, default="", help="The name of the experiment.")
    parser.add_argument("--object-store-selector", type=str, help="The region selector to use for the precomputation.")
    parser.add_argument("--batch-size", type=int, help="The batch size to use for the precomputation.")
    parser.add_argument("--hydro-dir", type=str, help="The directory of the SkyPie precomputer hydroflow project.")
    parser.add_argument("--data-dir", type=str, help="The data directory of the supplemental files.")
    parser.add_argument("--experiment-dir", type=str, help="The base directory to store the experiment results.")
    parser.add_argument("--profile", type=str, help="The compiler profile to use, e.g., dev or release.")
    parser.add_argument("--latency-slo", type=float, help="The latency SLO to use for the precomputation.")

    return parser.parse_args(args=args)

def build_scaling_experiments():
    min_replication_factor = 1
    max_replication_factor = 5
    fixed_args = dict(
        experiment_dir = os.path.join(os.getcwd(), "results", "precomputation_scaling"),
        batch_size = 200,
        redundancy_elimination_workers = 80,
        replication_factor = 0,
        optimizer="PrimalSimplex",
        use_clarkson=False,
        #profile= "dev"
    )
    
    scaling = Experiment(region_selector="aws-eu", object_store_selector="General Purpose", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
        Experiment(region_selector="aws-eu", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) #+ \
        #Experiment(region_selector="aws", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
        #Experiment(region_selector="azure", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
        #Experiment(region_selector="azure|aws", **fixed_args).as_replication_factors(min_replication_factor, min(4, max_replication_factor))

    # Order by replication factor
    scaling.sort(key=lambda e: e.replication_factor)

    # Also save candidates of small experiments
    for e in scaling:
        e.output_candidates = e.region_selector == "aws-eu"

    return scaling

def build_scaling_experiments_lrs():
    fixed_args = dict(
        experiment_dir = os.path.join(os.getcwd(), "results", "precomputation_scaling_lrs"),
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
        e.experiment_dir = os.path.join(os.getcwd(), "results", "precomputation_scaling_small_candidates")
        e.output_candidates = True
        e.optimizer = "PrimalSimplex"

    return experiments

def build_precomputation_batching_experiments(large=False):
    fixed_args = dict(
        experiment_dir = os.path.join(os.getcwd(), "results", "batch_size_scaling"),
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
        fixed_args["experiment_dir"] = os.path.join(os.getcwd(), "results", "batch_size_scaling_large")
        fixed_args["replication_factor"] = 5

    return [Experiment(**fixed_args, batch_size=b) for b in batch_sizes]

def build_cpu_scaling_experiments():
    fixed_args = dict(
        experiment_dir = os.path.join(os.getcwd(), "results", "cpu_scaling"),
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
        experiment_dir = os.path.join(os.getcwd(), "results", "accuracy"),
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

def build_skystore_experiments(*, latency_slos=[2.0, 4.0, 8.0]):
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
    
    scaling = Experiment(replication_factor=0, region_selector="aws", **fixed_args, latency_slo=latency_slos[-1]).as_args(key="replication_factor", args=[1,2,3,5,8])
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
    "skystore": build_skystore_experiments,
    "scaling": build_scaling_experiments,
    "scaling_lrs": build_scaling_experiments_lrs,
    "scaling_candidates": build_scaling_experiment_candidates,
    "batch_size": build_precomputation_batching_experiments,
    "cpu_scaling": build_cpu_scaling_experiments,
    "accuracy": build_accuracy_small_experiments,
}

async def main(argv):

    if len(argv) > 0:
        if argv[0] in named_experiments:
            experiments = named_experiments[argv[0]]()
        else:
            exp_args = {k:v for k,v in get_args(argv).__dict__.items() if v}
            experiments = [Experiment(**exp_args)]
    else:
        #experiments = build_precomputation_batching_experiments(large=False)
        #experiments = build_precomputation_batching_experiments(large=True)
        #experiments = build_cpu_scaling_experiments()
        #experiments = build_scaling_experiments()
        #experiments = build_accuracy_small_experiments()
        #experiments = build_scaling_experiment_candidates()
        experiments = build_precomputation_batching_experiments()
        #experiments = build_skystore_experiments()

    print("Running experiments:", len(experiments))
    for experiment in experiments:
        await precomputation(e=experiment)
        
        # Cool down
        await asyncio.sleep(5)

if __name__ == "__main__":
    import sys
    import hydro.async_wrapper
    hydro.async_wrapper.run(main, sys.argv[1:])