import hydro

async def main(args):
    deployment = hydro.Deployment()

    localhost = deployment.Localhost()

    args = {
        "region-selector": "aws",
        "replication-factor": "3",
        "output-file-name": "/dev/null",
        "batch-size": "200",
        "network-file": "/Users/tbang/git/sky-pie-precomputer/network_cost_v2.csv",
        "object-store-file": "/Users/tbang/git/sky-pie-precomputer/storage_pricing.csv",
        #"output_candidates": ""
    }
    # Convert args to a list of strings with --key=value format
    args = [f"--{key}={value}" for key, value in args.items()]

    generator_service = deployment.HydroflowCrate(
        src="./skypie_lib",
        example="decisions_generator_launch",
        on=localhost,
        display_id="generator",
        args=args
    )

    receiver_service = deployment.HydroflowCrate(
        src="./skypie_lib",
        example="redundancy_elim_test_launch",
        on=localhost,
        display_id="redundancy_elim",
        args=args
    )

    ## Connect named ports of services
    # Sender service's "output" port to receiver service's "input" port
    generator_service.ports.output.send_to(receiver_service.ports.input)

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