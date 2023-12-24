from sky_pie_baselines import MigrationOptimizer

network_file = "/home/vscode/sky-pie-precomputer/data/network_cost_v2.csv"
object_store_file = "/home/vscode/sky-pie-precomputer/data/storage_pricing.csv"
region_selector = "aws-eu"
object_store_selector = ""

object_stores = [
"aws-af-south-1-s3-General Purpose",
"aws-ap-east-1-s3-General Purpose",
"aws-ap-northeast-1-s3-General Purpose",
"aws-ap-northeast-2-s3-General Purpose",
"aws-ap-northeast-3-s3-General Purpose",
"aws-ap-south-1-s3-General Purpose",
"aws-ap-southeast-1-s3-General Purpose",
"aws-ap-southeast-2-s3-General Purpose",
"aws-ap-southeast-3-s3-General Purpose",
"aws-ca-central-1-s3-General Purpose",
"aws-eu-central-1-s3-General Purpose",
"aws-eu-central-2-s3-General Purpose",
"aws-eu-north-1-s3-General Purpose",
"aws-eu-south-1-s3-General Purpose",
"aws-eu-south-2-s3-General Purpose",
"aws-eu-west-1-s3-General Purpose",
"aws-eu-west-2-s3-General Purpose",
"aws-eu-west-3-s3-General Purpose",
"aws-me-central-1-s3-General Purpose",
"aws-me-south-1-s3-General Purpose",
"aws-sa-east-1-s3-General Purpose",
"aws-us-east-1-s3-General Purpose",
"aws-us-east-2-s3-General Purpose",
"aws-us-west-1-s3-General Purpose",
"aws-us-west-2-s3-General Purpose",
"aws-af-south-1-s3-Infrequent Access",
"aws-ap-east-1-s3-Infrequent Access",
"aws-ap-northeast-1-s3-Infrequent Access",
"aws-ap-northeast-2-s3-Infrequent Access",
"aws-ap-northeast-3-s3-Infrequent Access",
"aws-ap-south-1-s3-Infrequent Access",
"aws-ap-southeast-1-s3-Infrequent Access",
"aws-ap-southeast-2-s3-Infrequent Access",
"aws-ap-southeast-3-s3-Infrequent Access",
"aws-ca-central-1-s3-Infrequent Access",
"aws-eu-central-1-s3-Infrequent Access",
"aws-eu-central-2-s3-Infrequent Access",
"aws-eu-north-1-s3-Infrequent Access",
"aws-eu-south-1-s3-Infrequent Access",
"aws-eu-south-2-s3-Infrequent Access",
"aws-eu-west-1-s3-Infrequent Access",
"aws-eu-west-2-s3-Infrequent Access",
"aws-eu-west-3-s3-Infrequent Access",
"aws-me-central-1-s3-Infrequent Access",
"aws-me-south-1-s3-Infrequent Access",
"aws-sa-east-1-s3-Infrequent Access",
"aws-us-east-1-s3-Infrequent Access",
"aws-us-east-2-s3-Infrequent Access",
"aws-us-west-1-s3-Infrequent Access",
"aws-us-west-2-s3-Infrequent Access",
"aws-af-south-1-s3-Non-Critical Data",
"aws-ap-east-1-s3-Non-Critical Data",
"aws-ap-northeast-1-s3-Non-Critical Data",
"aws-ap-northeast-2-s3-Non-Critical Data",
"aws-ap-northeast-3-s3-Non-Critical Data",
"aws-ap-south-1-s3-Non-Critical Data",
"aws-ap-southeast-1-s3-Non-Critical Data",
"aws-ap-southeast-2-s3-Non-Critical Data",
"aws-ap-southeast-3-s3-Non-Critical Data",
"aws-ca-central-1-s3-Non-Critical Data",
"aws-eu-central-1-s3-Non-Critical Data",
"aws-eu-central-2-s3-Non-Critical Data",
"aws-eu-north-1-s3-Non-Critical Data",
"aws-eu-south-1-s3-Non-Critical Data",
"aws-eu-south-2-s3-Non-Critical Data",
"aws-eu-west-1-s3-Non-Critical Data",
"aws-eu-west-2-s3-Non-Critical Data",
"aws-eu-west-3-s3-Non-Critical Data",
"aws-me-central-1-s3-Non-Critical Data",
"aws-me-south-1-s3-Non-Critical Data",
"aws-sa-east-1-s3-Non-Critical Data",
"aws-us-east-1-s3-Non-Critical Data",
"aws-us-east-2-s3-Non-Critical Data",
"aws-us-west-1-s3-Non-Critical Data",
"aws-us-west-2-s3-Non-Critical Data"
]

object_stores = [o for o in object_stores if o.startswith(region_selector) and object_store_selector in o]

application_regions = [
"aws-af-south-1",
"aws-ap-east-1",
"aws-ap-northeast-1",
"aws-ap-northeast-2",
"aws-ap-northeast-3",
"aws-ap-south-1",
"aws-ap-southeast-1",
"aws-ap-southeast-2",
"aws-ap-southeast-3",
"aws-ca-central-1",
"aws-eu-central-1",
"aws-eu-central-2",
"aws-eu-north-1",
"aws-eu-south-1",
"aws-eu-south-2",
"aws-eu-west-1",
"aws-eu-west-2",
"aws-eu-west-3",
"aws-me-central-1",
"aws-me-south-1",
"aws-sa-east-1",
"aws-us-east-1",
"aws-us-east-2",
"aws-us-west-1",
"aws-us-west-2"
]

application_regions = [region for region in application_regions if region.startswith(region_selector)]
application_regions = {region: i for i, region in enumerate(application_regions)}

optimizer = MigrationOptimizer(network_file, object_store_file, object_stores, application_regions, verbose=2)


cur = list(object_stores[:5])
opt = list(object_stores[5:7])

workload_id = 1
object_num = 1
object_size = 1000.0
cur_cost = 1000.0
opt_cost = 50.0
opt_cost_2 = 40.0
opt_cost_3 = 2.0
opt_2 = list(object_stores[7:9])

# optimize_online_recurring_by_name(&mut self, workload_id: u64, opt: Vec<&str>, cur_cost: f64, opt_cost: f64, object_num: u64, object_size: f64) -> bool
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, cur, cur_cost, cur_cost, object_num, object_size)
assert do_migration == True, "do_migration should be True"
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, opt, cur_cost, opt_cost, object_num, object_size)
assert do_migration == False, "do_migration should be False"
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, opt, cur_cost, opt_cost, object_num, object_size)
assert do_migration == True, "do_migration should be True"
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, opt_2, opt_cost, opt_cost_2, object_num, object_size)

# Accumulate loss but optimal cost + migration is still larger than current cost
assert do_migration == False, "do_migration should be False"
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, opt_2, opt_cost, opt_cost_2, object_num, object_size)
assert do_migration == False, "do_migration should be False"
# Loss exceeds migration cost but optimal cost + migration is still larger than current cost
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, opt_2, opt_cost, opt_cost_2, object_num, object_size)
assert do_migration == False, "do_migration should be False"
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, opt_2, opt_cost, opt_cost_2, object_num, object_size)
assert do_migration == False, "do_migration should be False"
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, opt_2, opt_cost, opt_cost_2, object_num, object_size)
assert do_migration == False, "do_migration should be False"

# Now trigger migration with lower optimal cost
do_migration = optimizer.optimize_online_recurring_by_name(workload_id, opt_2, opt_cost, opt_cost_3, object_num, object_size)
assert do_migration == True, "do_migration should be True"