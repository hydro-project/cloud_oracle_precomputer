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

    write_choices_service = deployment.HydroflowCrate(
        src=".",
        example="write_choices_simple_launch",
        on=localhost,
        display_id="write_choices",
        args=args
    )
    
    candidates_service = deployment.HydroflowCrate(
        src=".",
        #example="simple_candidate_policies_launch",
        example="candidate_policies_launch",
        on=localhost,
        display_id="candidate_policies",
        args=args
    )

    #candidates_service_stderr = await candidates_service.stderr()
    #async for log in candidates_service_stderr:
    #    print(f"Candidates stderr: {log}")

    receiver_service = deployment.HydroflowCrate(
        src=".",
        #example="decisions_counter_launch",
        example="counter",
        on=localhost,
        display_id="counter",
        args=args
    )

    ## Connect named ports of services
    # Sender service's "output" port to receiver service's "input" port
    write_choices_service.ports.output.send_to(candidates_service.ports.input)
    candidates_service.ports.output.send_to(receiver_service.ports.input)

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