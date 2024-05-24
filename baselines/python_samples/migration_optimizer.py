import numpy as np

from sky_pie_baselines import MigrationOptimizer, Workload

network_file = "data/network_cost_v2.csv"
object_store_file = "data/storage_pricing.csv"
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

workload_id = "1"
object_num = 1
object_size = 1000.0
cur_cost = 1000.0
opt_cost = 50.0
opt_cost_2 = 40.0
opt_cost_3 = 2.0
opt_2 = list(object_stores[7:9])

reading_regions = ['aws-eu-west-1', 'aws-eu-west-2']
#reading_regions = ['aws-eu-west-1']
writing_regions = reading_regions
puts = 0.0
gets = [0.0 for _ in range(len(application_regions))]
egress = [0.0 for _ in range(len(application_regions))]
ingress = [0.0 for _ in range(len(application_regions))]

for application_region, idx in application_regions.items():
    gets[idx] = object_num if application_region in reading_regions else 0.0
    egress[idx] = gets[idx] * object_num * object_size

    if application_region in writing_regions:
        puts += object_num
        ingress[idx] = object_num * object_size

workload = Workload(
    size=object_size * object_num,
    puts=puts,
    gets=gets,
    ingress=ingress,
    egress=egress,
)

write_choice = ["aws-eu-west-2-s3-General Purpose"]
read_choice = {"aws-eu-west-1": "aws-eu-west-2-s3-General Purpose", "aws-eu-west-2": "aws-eu-west-2-s3-General Purpose"}

cost_expected = 0.0
zero_list = [0.0 for _ in application_regions]

storage_price = optimizer.cost(Workload(size=1, puts=0, gets=zero_list, ingress=zero_list, egress=zero_list), write_choice, read_choice)
storage_cost_expected = storage_price * object_size * object_num
storage_cost = optimizer.cost(Workload(size=object_size * object_num, puts=0, gets=zero_list, ingress=zero_list, egress=zero_list), write_choice, read_choice)
assert storage_cost == storage_cost_expected, "Storage cost should be equal to storage_cost_expected"
cost_expected += storage_cost_expected

put_price = optimizer.cost(Workload(size=0, puts=1, gets=zero_list, ingress=zero_list, egress=zero_list), write_choice, read_choice)
put_cost_expected = put_price * puts
put_cost = optimizer.cost(Workload(size=0, puts=puts, gets=zero_list, ingress=zero_list, egress=zero_list), write_choice, read_choice)
assert put_cost == put_cost_expected, "Put cost should be equal to put_cost_expected"
cost_expected += put_cost_expected

get_price = {}
ingress_price = {}
egress_price = {}
for region in application_regions:
    region_list_unit = [1.0 if r == region else 0.0 for r in application_regions]

    get_price[region] = optimizer.cost(Workload(size=0, puts=0, gets=region_list_unit, ingress=zero_list, egress=zero_list), write_choice, read_choice)
    get_cost_expected = get_price[region] * gets[application_regions[region]]
    region_list_actual = [gets[application_regions[region]] if r == region else 0.0 for r in application_regions]
    get_cost = optimizer.cost(Workload(size=0, puts=0, gets=region_list_actual, ingress=zero_list, egress=zero_list), write_choice, read_choice)
    assert get_cost == get_cost_expected, "Get cost should be equal to get_cost_expected"
    cost_expected += get_cost_expected

    ingress_price[region] = optimizer.cost(Workload(size=0, puts=0, gets=zero_list, ingress=region_list_unit, egress=zero_list), write_choice, read_choice)
    ingress_cost_expected = ingress_price[region] * ingress[application_regions[region]]
    region_list_actual = [ingress[application_regions[region]] if r == region else 0.0 for r in application_regions]
    ingress_cost = optimizer.cost(Workload(size=0, puts=0, gets=zero_list, ingress=region_list_actual, egress=zero_list), write_choice, read_choice)
    assert ingress_cost == ingress_cost_expected, "Ingress cost should be equal to ingress_cost_expected"
    cost_expected += ingress_cost_expected

    egress_price[region] = optimizer.cost(Workload(size=0, puts=0, gets=zero_list, ingress=zero_list, egress=region_list_unit), write_choice, read_choice)
    egress_cost_expected = egress_price[region] * egress[application_regions[region]]
    region_list_actual = [egress[application_regions[region]] if r == region else 0.0 for r in application_regions]
    egress_cost = optimizer.cost(Workload(size=0, puts=0, gets=zero_list, ingress=zero_list, egress=region_list_actual), write_choice, read_choice)
    assert egress_cost == egress_cost_expected, "Egress cost should be equal to egress_cost_expected"
    cost_expected += egress_cost_expected

cost = optimizer.cost(workload, write_choice, read_choice)
print(f"Cost: {cost}, Expected: {cost_expected}")
assert np.isclose(cost, cost_expected), "Cost should be equal to cost_expected"

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