import asyncio
from typing import List
from deploy import Experiment
from deploy.precomputation import precomputation

async def __wrapper(experiments: List[Experiment]):
    print("Running precomputations:", len(experiments))
    for experiment in experiments:
        await precomputation(e=experiment)
        
        # Cool down
        await asyncio.sleep(5)

def run(experiments: List[Experiment]):
    import hydro.async_wrapper
    hydro.async_wrapper.run(__wrapper, experiments)