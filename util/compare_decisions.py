import json
#import vectordb
from docarray import BaseDoc, DocList
from  docarray.typing import NdArray, ID
import numpy as np
#from docarray.index import HnswDocumentIndex
from docarray.index import InMemoryExactNNIndex
import os
#from vectordb import InMemoryExactNNVectorDB, HNSWVectorDB

embedding_length = 128
class MyDoc(BaseDoc):
    embedding: NdArray[embedding_length]
    doc: ID = 0

def load_original(original_json_file):
    with open(original_json_file) as f:
        original_json = json.load(f)

    # Traverse path to the policies of the first optimizer
    original_policies = original_json["tier_advise"]
    while "candidate_partitions" not in  original_policies:
        original_policies = original_policies[next(iter(original_policies))]

    original_candidates = original_policies["candidate_partitions"]

    # Get optimal ids, decode delta ids, and get optimal policies from candidates
    original_optimal_ids = original_policies["optimal_partitions_by_optimizer"]
    original_optimal_ids = original_optimal_ids[next(iter(original_optimal_ids))]
    original_optimal_ids = original_optimal_ids["optimal_partition_ids_delta"]

    offset = 0
    original_optimal_policies = []
    for i in original_optimal_ids:
        offset += i
        original_optimal_policies.append(original_candidates[offset])

    return original_optimal_policies

def load_new(new_jsonl_file):
    if not isinstance(new_jsonl_file, list):
        new_jsonl_file = [new_jsonl_file]

    new_optimal_policies = []
    for f in new_jsonl_file:
        with open(f) as f:
            new_optimal_policies.extend([json.loads(line) for line in f])
    
    return new_optimal_policies

def compare(original_optimal_policies, new_optimal_policies):
    # Build dictionary of original optimal policies
    original_optimal_policies_dict = {}
    for policy in original_optimal_policies:
        policy = policy["replicationScheme"]
        # Key is the name of the object stores and the assignments
        assignments = set([f'{a["app"]}->{a["objectStore"]}' for a in policy["appAssignments"]])
        assignments.update([o for o in policy["objectStores"].keys()])

        key = tuple(sorted(assignments))
        original_optimal_policies_dict[key] = policy

    new_optimal_policies_dict = {}
    for policy in new_optimal_policies:
        policy = policy["replicationScheme"]
        # Key is the name of the object stores and the assignments
        assignments = set([f'{a["app"]}->{a["objectStore"]}' for a in policy["appAssignments"]])
        assignments.update([o for o in policy["objectStores"]])

        key = tuple(sorted(assignments))
        new_optimal_policies_dict[key] = policy

    # Compare the two dictionaries
    matches = 0
    missing = []
    for key in original_optimal_policies_dict.keys():
        if key not in new_optimal_policies_dict:
            #print(f'Original optimal policy not found in new optimal policies: {key}')

            missing.append(original_optimal_policies_dict[key])
            continue

        original_policy = original_optimal_policies_dict[key]
        new_policy = new_optimal_policies_dict[key]

        #if original_policy != new_policy:
        #    print(f'Original policy: {original_policy}')
        #    print(f'New policy: {new_policy}')
        #    print()
        #    continue

        matches += 1

    print(f'Number of matches: {matches}/{len(original_optimal_policies_dict)}')
    print(f'Number of missing: {len(missing)}')
    json.dump(missing, open('missing_original.json', 'w'), indent=4, sort_keys=False)

def compare_costs(original_optimal_policies, new_optimal_policies, missing_file=None):
    # Specify your workspace path
    #db = InMemoryExactNNVectorDB[MyDoc](workspace='./workspace_path')
    # create a Document Index
    #index = HnswDocumentIndex[MyDoc](work_dir='./test_index')
    index = InMemoryExactNNIndex[MyDoc]()

    original_optimal_policies_dict = {}
    for policy in original_optimal_policies:
        #policy = policy["replicationScheme"]

        # Order the assignments by app name
        # XXX: Assuming new_optimal_policies are already sorted!
        assignments = sorted(policy["replicationScheme"]["appAssignments"], key=lambda a: a["app"])
        # Restructure the costs when the sorted assignments are different
        sorted_index = {a["app"]: i for i, a in enumerate(assignments)}
        old = policy["costWLHalfplane"][:]
        for i, a in enumerate(policy["replicationScheme"]["appAssignments"]):
            s = sorted_index[a["app"]]
            if i != s:
                # Copy old costs
                offset = 3
                no_apps = len(policy["replicationScheme"]["appAssignments"])
                # Swap get costs
                get_offset = 3
                offset = get_offset
                #policy["costWLHalfplane"][offset+i] = old[offset+s]
                policy["costWLHalfplane"][offset+s] = old[offset+i]
                # Swap ingress costs
                ingress_offset = get_offset + no_apps
                offset = ingress_offset
                #policy["costWLHalfplane"][offset+i] = old[offset+s]
                policy["costWLHalfplane"][offset+s] = old[offset+i]
                # Swap egress costs
                egress_offset = ingress_offset + no_apps
                offset = egress_offset
                #policy["costWLHalfplane"][offset+i] = old[offset+s]
                policy["costWLHalfplane"][offset+s] = old[offset+i]

        # Key is the costs
        key = tuple(float(c) for c in policy["costWLHalfplane"])
        #key = tuple(sorted(float(c) for c in policy["costWLHalfplane"]))
        original_optimal_policies_dict[key] = policy

    # Index a list of documents with random embeddings
    doc_list= []
    for policy in new_optimal_policies:
        #[MyDoc(embedding=np.concatenate([np.array(key, dtype=float), np.zeros(embedding_length - len(key), dtype=float)])) for policy in new_optimal_policies]
        key = policy["costWLHalfplane"]
        key = np.concatenate([np.array(key, dtype=float), np.zeros(embedding_length - len(key), dtype=float)])
        doc_list.append(MyDoc(embedding=key, doc=len(doc_list)))
    index.index(DocList[MyDoc](doc_list))

    # Compare the two dictionaries
    matches = 0
    missing = []

    original_doc_list = [ np.concatenate([np.array(key, dtype=float), np.zeros(embedding_length - len(key), dtype=float)]) for key in original_optimal_policies_dict.keys()]
    results, scores = index.find_batched(np.array(original_doc_list), limit=3, search_field='embedding')

    for (results_new, s, key) in zip(results, scores, original_optimal_policies_dict.keys()):

        #print(s[0])
        if s[0] < 0.99999999999999:
            missing.append({"Original": original_optimal_policies_dict[key], "Similar new": [(score, new_optimal_policies[int(result.doc)]) for score, result in zip(s, results_new)]})
        else:
            matches += 1

    print(f'Number of matches: {matches}/{len(original_optimal_policies_dict)}')
    print(f'Number of missing: {len(missing)}')
    
    if missing_file:
        json.dump(missing, open(missing_file, 'w'), indent=4, sort_keys=False)

# Take original json file and compare it to the new jsonl file

#if len(sys.argv) < 2:
#    print("Usage: python compare_decisions.py <original_json_file> <new_jsonl_file>")
#    exit(1)

#original_json_file = sys.argv[1]
#new_jsonl_file = sys.argv[2]

# Replication factor 2 - aws-southeast
original_json_file = "experiment-2023-08-09-14-40-59/original.json"
new_jsonl_file = "experiment-2023-08-09-14-40-59/candidates.jsonl"

# Replication factor 3 - aws-southeast
original_json_file = "experiment-2023-08-11-15-30-47/original.json"
new_jsonl_file = "experiment-2023-08-11-15-30-47/candidates.jsonl"

# Replication factor 1 - aws
original_json_file = "experiment-2023-08-14-12-16-18/original.json"
new_jsonl_file = "experiment-2023-08-14-12-16-18/candidates.jsonl"

# Replication factor 2 - aws
original_json_file = "experiment-2023-08-14-12-17-29/original.json"
#new_jsonl_file = "experiment-2023-08-14-12-17-29/candidates.jsonl"
#new_jsonl_file = "experiment-2023-08-14-12-17-29/optimal_0.jsonl"
new_jsonl_file = "experiment-2023-08-14-13-03-56/optimal_0.jsonl"
new_jsonl_file = "experiment-2023-08-14-13-03-56/candidates.jsonl"

new_jsonl_file = [f"experiment-2023-08-14-15-06-00/candidates_{i}.jsonl" for i in range(8)]

if isinstance(new_jsonl_file, list):
    missing_file = new_jsonl_file[0]
else:
    missing_file = new_jsonl_file
missing_file = f"{missing_file}.missing.json"

original_optimal_policies = load_original(original_json_file)
new_optimal_policies = load_new(new_jsonl_file)

compare(original_optimal_policies, new_optimal_policies)
compare_costs(original_optimal_policies, new_optimal_policies, missing_file=missing_file)
