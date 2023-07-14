import hydro
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
    deployment = hydro.Deployment()

    localhost = deployment.Localhost()

    redundancy_elimination_workers = 1

    now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    experiment_name = f"experiment-{now}"
    args = {
        "region-selector": "aws",
        "replication-factor": "2",
        #"output-file-name": "/dev/null",
        "batch-size": "10",
        "network-file": "/Users/tbang/git/sky-pie-precomputer/network_cost_v2.csv",
        "object-store-file": "/Users/tbang/git/sky-pie-precomputer/storage_pricing.csv",
        "redundancy-elimination-workers": redundancy_elimination_workers,
        #"output_candidates": ""
        "experiment-name": experiment_name
    }
    # Convert args to a list of strings with --key=value format
    args = [f"--{key}={value}" for key, value in args.items()]

    write_choices_service = deployment.HydroflowCrate(
        src=".",
        #example="write_choices_simple_launch",
        example="write_choices_simple_demux_launch",
        on=localhost,
        display_id="write_choices",
        args=args
    )
    
    candidates_service = [s for s in create_scale_up_service(deployment,
        num_scale_up=redundancy_elimination_workers,
        #profile="dev",
        src=".",
        example="candidate_and_reduce_launch",
        #example="counter",
        on=localhost,
        display_id="candidate_reduce",
        #args=args,
        # '--output-candidates-file-name', f'candidates_{i}.jsonl',
        kwargs_instances={i: {"args":(args + ['--executor-name', f"candidate_executor_{i}", '-o', f'{experiment_name}_optimal_{i}.jsonl'])} for i in range(redundancy_elimination_workers)},
        )]

    ## Connect named ports of services
    # Sender service's "output" port to receiver service's "input" port
    send_to_demux(write_choices_service, candidates_service)

    # Deploy and start, blocking until deployment is complete
    await deployment.deploy()

    await deployment.start()

    # Wait for user input to terminate
    input("Press enter to terminate...")


    #print(f"Sender service exit code: {await sender_service.exit_code()}")
    #print(f"Receiver service exit code: {await receiver_service.exit_code()}")

if __name__ == "__main__":
    import sys
    import hydro.async_wrapper
    hydro.async_wrapper.run(main, sys.argv[1:])