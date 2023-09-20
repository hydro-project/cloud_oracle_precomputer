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
    data_dir: str = field(default_factory=os.getcwd)
    profile: str = "release"
    object_store_selector: str = ""
    name: "str|None" = None
    experiment_dir_full: str = "" # This is set in __post_init__
    optimizer: str = "InteriorPoint",
    use_clarkson: bool = False

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

        # Create the name of the experiment
        paths = ([self.name] if self.name is not None else []) + \
            [friendly_region_and_object_store, str(self.replication_factor), str(self.redundancy_elimination_workers), str(self.batch_size), str(self.optimizer), clarkson]
        self.experiment_dir_full = os.path.join(self.experiment_dir, *paths)

    def copy(self, **kwargs):
        kwargs = {**self.__dict__, **kwargs}
        return Experiment(**kwargs)


    def as_replication_factors(self, min_replication_factor, max_replication_factor):
        return [ self.copy(replication_factor=r) for r in range(min_replication_factor, max_replication_factor + 1)]

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
                '-o', os.path.join(e.experiment_dir_full, f'{optimal_policies_name_prefix}_{i}.{output_file_extension}'),
                "--output-candidates-file-name", os.path.join(e.experiment_dir_full, f"{candidate_policies_name_prefix}_{i}.{output_file_extension}")]
            )
        } for i in range(e.redundancy_elimination_workers)
    }

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
    parser.add_argument("--replication-factor", type=int, required=True, help="The replication factor to use for the precomputation.")
    parser.add_argument("--redundancy-elimination-workers", type=int, required=True, help="The number of workers to use for the redundancy elimination.")
    parser.add_argument("--region-selector", type=str, required=True, help="The region selector to use for the precomputation.")
    parser.add_argument("--experiment-name", type=str, required=True, help="The name of the experiment.")
    parser.add_argument("--object-store-selector", type=str, help="The region selector to use for the precomputation.")
    parser.add_argument("--batch-size", type=int, help="The batch size to use for the precomputation.")
    parser.add_argument("--hydro-dir", type=str, help="The directory of the SkyPie precomputer hydroflow project.")
    parser.add_argument("--data-dir", type=str, help="The data directory of the supplemental files.")
    parser.add_argument("--experiment-dir", type=str, help="The base directory to store the experiment results.")
    parser.add_argument("--profile", type=str, help="The compiler profile to use, e.g., dev or release.")

    return parser.parse_args(args=args)

async def main(argv):

    if len(argv) > 0:
        experiments = [Experiment(**get_args(argv).__dict__)]
    else:
        min_replication_factor = 1
        max_replication_factor = 5
        fixed_args = dict(
            experiment_dir = os.path.join(os.getcwd(), "results", "precomputation_scaling"),
            batch_size = 400,
            redundancy_elimination_workers = 60,
            replication_factor = 0,
            #profile= "dev"
        )
        #scaling = Experiment(region_selector="aws-eu", object_store_selector="General Purpose", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
            #Experiment(region_selector="aws-eu", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
            #Experiment(region_selector="aws", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor) + \
            #Experiment(region_selector="azure", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor)
        scaling = Experiment(region_selector="azure|aws", **fixed_args).as_replication_factors(min_replication_factor, max_replication_factor)

        experiments = scaling

    # Accuracy drill down with small problem settings
    fixed_args = dict(
        experiment_dir = os.path.join(os.getcwd(), "results", "batch_size_scaling"),
        #redundancy_elimination_workers = 30,
        redundancy_elimination_workers = 60,
        replication_factor=0,
        #profile="dev"
    )
    batch_sizes = [200, 500, 1000]
    use_clarkson = [False]
    object_selectors = [""]
    optimizers = ["PrimalSimplex"]
    #object_selectors = ["General Purpose", ""]
    #optimizers = ["PrimalSimplex", "InteriorPoint"]
    batch_size_scaling = [Experiment(region_selector="aws", object_store_selector=o_s, **fixed_args, batch_size=b, optimizer=opt, use_clarkson=c).as_replication_factors(5, 5) for b in batch_sizes for o_s in object_selectors for opt in optimizers for c in use_clarkson]

    # flatten list of experiments
    experiments = [e for sublist in batch_size_scaling for e in sublist]

    print("Running experiments:", len(experiments))
    for experiment in experiments:
        await precomputation(e=experiment)
        
        # Cool down
        await asyncio.sleep(5)

if __name__ == "__main__":
    import sys
    import hydro.async_wrapper
    hydro.async_wrapper.run(main, sys.argv[1:])