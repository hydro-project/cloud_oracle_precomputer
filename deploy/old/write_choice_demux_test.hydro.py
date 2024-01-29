import hydro

async def main(args):
    deployment = hydro.Deployment()

    localhost = deployment.Localhost()

    redundancy_elimination_workers = 2

    args = {
        "region-selector": "aws",
        "replication-factor": "3",
        "output-file-name": "/dev/null",
        "batch-size": "200",
        "network-file": "/Users/tbang/git/sky-pie-precomputer/network_cost_v2.csv",
        "object-store-file": "/Users/tbang/git/sky-pie-precomputer/storage_pricing.csv",
        "redundancy-elimination-workers": redundancy_elimination_workers,
        #"output_candidates": ""
    }
    # Convert args to a list of strings with --key=value format
    args = [f"--{key}={value}" for key, value in args.items()]

    generator_service = deployment.HydroflowCrate(
        src="./skypie_lib",
        example="write_choices_simple_demux_launch",
        on=localhost,
        display_id="generator",
        args=args
    )

    def create_scale_out_service(*args, num_scale_out, display_id, **kwargs):
        for i in range(num_scale_out):
            yield deployment.HydroflowCrate(
                *args,
                display_id=f"{display_id}:{i}",
                **kwargs
            )

    receiver_service = [s for s in create_scale_out_service(src="./skypie_lib",
        num_scale_out=redundancy_elimination_workers,
        example="counter",
        on=localhost,
        display_id="counter",
        args=args
        )]

    """ deployment.HydroflowCrate(
        src="./skypie_lib",
        example="counter",
        on=localhost,
        display_id="counter",
        args=args
    ) """

    def send_to_demux(src_service, dest_services):
        src_service.ports.output.send_to(hydro.demux({
        #0: receiver_service.ports.input.merge(),
        i: s.ports.input.merge() for i, s in enumerate(dest_services)
    }))

    ## Connect named ports of services
    # Sender service's "output" port to receiver service's "input" port
    #generator_service.ports.output.send_to(receiver_service.ports.input)
    """ generator_service.ports.output.send_to(hydro.demux({
        #0: receiver_service.ports.input.merge(),
        i: s.ports.input.merge() for i, s in enumerate(receiver_service)
    })) """

    send_to_demux(generator_service, receiver_service)

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