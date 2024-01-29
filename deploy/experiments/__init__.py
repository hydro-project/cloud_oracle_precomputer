import deploy.experiments.skystore as skystore
import deploy.experiments.sigmod as sigmod

# All named experiments with prefix according to use case
named_experiments = {}
named_experiments.update({ f"skystore_{k}": v for k,v in skystore.named_experiments.items()})
named_experiments.update({ f"sigmod_{k}":v for k,v in sigmod.named_experiments.items()})