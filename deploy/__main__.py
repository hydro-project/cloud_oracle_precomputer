import argparse
import sys
from deploy import Experiment, run
from deploy.experiments import named_experiments

def get_args(args):
    parser = argparse.ArgumentParser(description="Run the SkyPie precomputer.")
    parser.add_argument_group("Specify named pre-registered experiments.")
    parser.add_argument("--experiment", type=str, help="Run a named pre-registered experiment.")
    parser.add_argument("--list-experiments", action="store_true", help="List all pre-registered experiments.")
    precomp_args = parser.add_argument_group("Overwrite paramters of pre-registered experiments or specify a custom precomputation.")
    precomp_args.add_argument("--experiment-name", type=str, help="The name of the experiment specified by the custom precomputation.")
    precomp_args.add_argument("--replication-factor", type=int, help="The replication factor to use for the precomputation.")
    precomp_args.add_argument("--redundancy-elimination-workers", type=int, help="The number of workers to use for the redundancy elimination.")
    precomp_args.add_argument("--region-selector", type=str, default="", help="The region selector to use for the precomputation.")
    precomp_args.add_argument("--object-store-selector", type=str, help="The region selector to use for the precomputation.")
    precomp_args.add_argument("--batch-size", type=int, help="The batch size to use for the precomputation.")
    precomp_args.add_argument("--hydro-dir", type=str, help="The directory of the SkyPie precomputer hydroflow project.")
    precomp_args.add_argument("--data-dir", type=str, help="The data directory of the supplemental files.")
    precomp_args.add_argument("--experiment-dir", type=str, help="The base directory to store the experiment results.")
    precomp_args.add_argument("--profile", type=str, help="The compiler profile to use, e.g., dev or release.")
    precomp_args.add_argument("--latency-slo", type=float, help="The latency SLO to use for the precomputation.")

    return parser.parse_args(args=args)

def main(argv):

    args = get_args(argv)

    if args.list_experiments:
        print("Available experiments:")
        for name in named_experiments.keys():
            print(f"\t{name}")
        return

    if args.experiment is not None:
        experiments = named_experiments[args.experiment]()

        # Overwrite parameters
        params = {k:v for k,v in args.__dict__.items() if v and k not in ["experiment", "experiment_name"]}
        if params:
            print("Overwriting parameters:", params)
            experiments = [e.copy(**params) for e in experiments]

    else:
        exp_args = {k:v for k,v in args.__dict__.items() if v}
        experiments = [Experiment(**exp_args)]
    
    run(experiments)

if __name__ == "__main__":
    main(sys.argv[1:])
