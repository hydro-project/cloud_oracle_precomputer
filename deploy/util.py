import hydro
import sys
from dataclasses import dataclass, field
from typing import Any

@dataclass
class Tee:
    file: Any
    stdout: Any = field(init=False)

    def __post_init__(self):
        #self.file = open(name, mode)
        self.stdout = sys.stdout
        sys.stdout = self
    def close(self):
        self.flush()
        sys.stdout = self.stdout
        self.file.close()
    def write(self, data):
        self.file.write(data)
        self.stdout.write(data)
    def flush(self):
        self.file.flush()

def create_scale_up_service(deployment, *args, num_scale_up, display_id, kwargs_instances=dict(), **kwargs):
    """
    Creates a scale-up service by generating 'n' identical instances of a sercice, i.e.,HydroflowCrate.

    Parameters:
    - deployment: The deployment object used to create the scale-out service.
    - *args: Variable length argument list to be passed to the HydroflowCrate constructor.
    - num_scale_up: The number of instances to be created for the scale-up service.
    - display_id: The display ID prefix for each instance. The display ID of each instance will be in the format "{display_id}:{i}",
                  where i is the index of the instance.
    - **kwargs: Keyword arguments to be passed to the HydroflowCrate constructor.

    Returns:
    - A generator that yields the created instances of HydroflowCrate.

    Example usage:
    ```
    deployment = Deployment()
    scale_out_service = create_scale_up_service(deployment, arg1, arg2, num_scale_out=3, display_id="my service", kwarg1=val1, kwarg2=val2)
    for instance in scale_out_service:
        # Do something with each instance
    ```
    """

    for i in range(num_scale_up):
        yield deployment.HydroflowCrate(
            *args,
            display_id=f"{display_id}:{i}",
            **kwargs,
            **(kwargs_instances.get(i, {}))
        )

def send_to_demux(src_service, dest_services):
    """
    Sends data from the source service to a demultiplexer, which distributes the data to multiple destination services.
    It defines consecutive indexes for the destination services starting from 0, in the order they are passed in the list.

    Parameters:
    - src_service: The source service that provides the data to be sent.
    - dest_services: A list of destination services that will receive the data.

    Returns:
    None

    Example:
    send_to_demux(source_service, [destination_service1, destination_service2, destination_service3])
    """

    src_service.ports.output.send_to(hydro.demux({
        i: s.ports.input.merge() for i, s in enumerate(dest_services)
    }))