from sky_pie_baselines import spanstore_aggregate
from pydantic import BaseModel
from datetime import datetime
from typing import Literal
from collections import defaultdict
import time

class Request(BaseModel):
    timestamp: datetime
    op: Literal["read", "write", "evict"]
    issue_region: str
    obj_key: str
    size: float


def spanstore_aggregate_py(requests, objects_in_access_set):
    put_counts = defaultdict(int)
    get_counts = defaultdict(int)
    ingress_counts = defaultdict(float)
    egress_counts = defaultdict(float)
    
    for i in range(len(requests)):
            request = requests[i]
            if request.obj_key in objects_in_access_set:
                if request.op == "write":
                    put_counts[request.issue_region] += 1
                    ingress_counts[request.issue_region] += request.size
                elif request.op == "read":
                    get_counts[request.issue_region] += 1
                    egress_counts[request.issue_region] += request.size
    return put_counts, get_counts, ingress_counts, egress_counts

if __name__ == "__main__":
    no_objects = 10000
    objects_in_access_set = [ f"key{i}" for i in range(no_objects)]
    region = ['gcp:europe-west1-b','azure:eastus','azure:westus','gcp:us-east1-b', 'gcp:us-west1-a', 'azure:westeurope', 'aws:us-east-1', 'aws:eu-west-1', 'azure:westus']
    timestamp = datetime.now()
    requests = [
        Request(timestamp=timestamp, op="read" if (i % 2) == 0 else "write", issue_region=region[i % len(region)], obj_key=objects_in_access_set[i], size=float(i))
        for i in range(no_objects)
    ]
    #print(requests)
    start1 = time.time_ns()
    res1 = spanstore_aggregate(requests, objects_in_access_set)
    end1 = time.time_ns()
    duration1 = (end1 - start1) / 1e6
    print(f"Time taken by Rust function: {duration1} ms")

    start2 = time.time_ns()
    res2 = spanstore_aggregate_py(requests, objects_in_access_set)
    end2 = time.time_ns()
    duration2 = (end2 - start2) / 1e6
    assert(res1 == res2)
    print(f"Time taken by Python function: {duration2} ms")
    print(f"Speedup: {duration2/duration1}")
    #put_counts, get_counts, ingress_counts, egress_counts = res1
    #put_counts2, get_counts2, ingress_counts2, egress_counts2 = res2
    #print(put_counts, get_counts, ingress_counts, egress_counts)
    #print(put_counts2, get_counts2, ingress_counts2, egress_counts2)