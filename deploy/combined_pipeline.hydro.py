import hydro
import os
from datetime import datetime

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

async def main(args):

    #profile = "dev"
    profile = "release" # Use default profile
    base_dir = "/home/vscode/sky-pie-precomputer"
    replication_factor = 3
    region_selector = "aws|azure"
    batch_size = 200
    redundancy_elimination_workers = 1
    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    experiment_name = f"experiments/experiment-{now}"
    
    print(args)
    if len(args) > 0 and args[0] == "local":
        redundancy_elimination_workers = 1
        #replication_factor = 3
    else:
        redundancy_elimination_workers = 200
        #replication_factor = 5

    args = {
        "region-selector": region_selector,
        "replication-factor": replication_factor,
        #"output-file-name": "/dev/null",
        "batch-size": batch_size,
        "network-file": f"{base_dir}/network_cost_v2.csv",
        "object-store-file": f"{base_dir}/storage_pricing.csv",
        "redundancy-elimination-workers": redundancy_elimination_workers,
        #"output_candidates": ""
        "experiment-name": experiment_name,
        "influx-host": "flaminio.millennium.berkeley.edu"
    }

    # Convert args to a list of strings with --key=value format
    args = [f"--{key}={value}" for key, value in args.items()]

    # Worker specific args
    optimal_policies_name_prefix = "optimal"
    optimal_policies_file_extension = "jsonl"

    kwargs_instances={
        i: {"args":(
            args + [
                '--executor-name', f"candidate_executor_{i}",
                '-o', f'{experiment_name}/{optimal_policies_name_prefix}_{i}.{optimal_policies_file_extension}',
                "--output-candidates-file-name", f"{experiment_name}/candidates_{i}.jsonl"]
            )
        } for i in range(redundancy_elimination_workers)
    }

    # Create directory for experiment
    os.makedirs(experiment_name)

    deployment = hydro.Deployment()

    localhost = deployment.Localhost()

    write_choices_service = deployment.HydroflowCrate(
        src="./skypie_lib",
        profile=profile,
        example="write_choices_simple_demux_launch",
        on=localhost,
        display_id="write_choices",
        args=args + ['-o', f"{optimal_policies_name_prefix}.{optimal_policies_file_extension}"]
    )

    logging_service = deployment.HydroflowCrate(
        src="./skypie_lib",
        profile=profile,
        example="logger_launch",
        on=localhost,
        display_id="logger",
        args=args
    )
    
    candidates_service = [s for s in create_scale_up_service(deployment,
        num_scale_up=redundancy_elimination_workers,
        profile=profile,
        src="./skypie_lib",
        example="candidate_and_reduce_launch",
        on=localhost,
        display_id="candidate_reduce",
        kwargs_instances=kwargs_instances
        )]

    ## Connect named ports of services
    # Sender service's "output" port to receiver service's "input" port
    send_to_demux(write_choices_service, candidates_service)

    # Send all timing information of the candidate services to the logging service
    for s in candidates_service:
        s.ports.time_output.send_to(logging_service.ports.input.merge())

    # Deploy and start, blocking until deployment is complete
    await deployment.deploy()

    await deployment.start()

    # Wait for user input to terminate
    input("Press enter to terminate...")

if __name__ == "__main__":
    import sys
    import hydro.async_wrapper
    hydro.async_wrapper.run(main, sys.argv[1:])
