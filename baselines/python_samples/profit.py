from sky_pie_baselines import ProfitBasedOptimizer, Workload

network_file = "/Users/tbang/git/sky-pie-precomputer/network_cost_v2.csv"
object_store_file = "/Users/tbang/git/sky-pie-precomputer/storage_pricing.csv"
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

workload = Workload(
    size=1.0,
    puts=0.0,
    gets=[1000.0] * len(application_regions),
    ingress=[0.0] * len(application_regions),
    egress=[1000.0] * len(application_regions)
)
optimizer = ProfitBasedOptimizer(network_file, object_store_file, object_stores, application_regions)

cost, decision = optimizer.optimize(workload)
print(f"Cost: {cost}")