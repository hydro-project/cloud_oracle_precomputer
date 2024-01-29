import os
import hydro
import json

from deploy import Experiment
from deploy.util import Tee, create_scale_up_service, send_to_demux

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
        "network-file": os.path.join(e.data_dir, "network_cost_v2.csv"),
        "object-store-file": os.path.join(e.data_dir,"storage_pricing.csv"),
        "redundancy-elimination-workers": e.redundancy_elimination_workers,
        #"output_candidates": ""
        "experiment-name": e.experiment_dir_full,
        "influx-host": "",
        "num-workers": num_workers,
        "optimizer": e.optimizer,
    }

    if e.latency_slo is not None and e.latency_file is not None:
        args["latency-slo"] = e.latency_slo
        args["latency-file"] = e.latency_file

    if e.replication_factor_max is not None:
        args["replication-factor-max"] = e.replication_factor_max


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