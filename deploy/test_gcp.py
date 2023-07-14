import hydro

async def main(args):
    deployment = hydro.Deployment()

    gcp_vpc = hydro.GCPNetwork(
        project="hydro-chrisdouglas",
    )

    doGCP = True
    localhost_machine = deployment.Localhost()
    def create_machine():
        if doGCP:
            return deployment.GCPComputeEngineHost(
                project="hydro-chrisdouglas",
                machine_type="e2-micro",
                image="debian-cloud/debian-11",
                region="us-west1-a",
                network=gcp_vpc
            )
        else:
            return localhost_machine

    

    args = {
        "region-selector": "aws",
        "replication-factor": "3",
        "output-file-name": "/dev/null",
        "batch-size": "200",
        "network-file": "/Users/tbang/git/sky-pie-precomputer/network_cost_v2.csv",
        "object-store-file": "/Users/tbang/git/sky-pie-precomputer/storage_pricing.csv",
        "experiment-name": "test",
        "redundancy-elimination-workers": "0",
        #"output_candidates": ""
    }
    # Convert args to a list of strings with --key=value format
    args = [f"--{key}={value}" for key, value in args.items()]

    generator_service = deployment.HydroflowCrate(
        src=".",
        example="decisions_generator_launch",
        on=create_machine(),
        display_id="generator",
        args=args
    )

    receiver_service = deployment.HydroflowCrate(
        src=".",
        example="decisions_counter_launch",
        #example="counter",
        on=create_machine(),
        display_id="counter",
        args=args
    )

    ## Connect named ports of services
    # Sender service's "output" port to receiver service's "input" port
    generator_service.ports.output.send_to(receiver_service.ports.input)

    # Deploy and start, blocking until deployment is complete
    await deployment.deploy()

    await deployment.start()

    """ async for log in receiver_service_stdout:
        print(f"Sender stdout: {log}") """
    """ async for log in receiver_service_stderr:
        print(f"Sender stdout: {log}") """

    # Wait for user input to terminate
    input("Press enter to terminate...")


    #print(f"Sender service exit code: {await sender_service.exit_code()}")
    #print(f"Receiver service exit code: {await receiver_service.exit_code()}")

if __name__ == "__main__":
    import sys
    import hydro.async_wrapper
    hydro.async_wrapper.run(main, sys.argv[1:])