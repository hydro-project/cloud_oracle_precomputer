import time
from dataclasses import dataclass, field

@dataclass
class Timing:
    name: str = ""
    __startTime: int = field(default_factory=lambda: time.time_ns())
    __duration: int = 0
    verbose: int = 0

    def __iadd__(self, other: "Timing") -> "Timing":
        self.__duration += other.__duration
        return self

    def start(self):
        self.__duration = 0
        self.__startTime = time.time_ns()

    def cont(self):
        self.__startTime = time.time_ns()

    def stop(self):
        now = time.time_ns()
        self.__duration += now - self.__startTime
        if self.verbose > 0:
            print(f"{self.name} = {self.__duration} ns")
        return self.__duration
    
    def getDuration(self):
        return self.__duration
    
@dataclass
class Timer:
    verbose: int = 0
    __computationTime: Timing = field(default_factory=lambda: Timing(name="Computation"))
    __overheadTime: Timing = field(default_factory=lambda: Timing(name="With overhead"))

    def __post_init__(self):
        self.__computationTime.verbose = self.verbose
        self.__overheadTime.verbose = self.verbose

    def __iadd__(self, other: "Timer"):
        self.__computationTime += other.__computationTime
        self.__overheadTime += other.__overheadTime
        return self

    def startComputation(self):
        self.__computationTime.start()

    def continueComputation(self):
        self.__computationTime.cont()

    def stopComputation(self):
        return self.__computationTime.stop()

    def startOverhead(self):
        self.__overheadTime.start()

    def continueOverhead(self):
        self.__overheadTime.cont()

    def stopOverhead(self):
        return self.__overheadTime.stop()

    def cont(self):
        self.continueComputation()
        self.continueOverhead()

    def stop(self):
        self.stopComputation()
        self.stopOverhead()
        return self.getTotalTime()

    def getComputationTime(self):
        return self.__computationTime.getDuration()
    
    def getOverheadTime(self):
        return self.__overheadTime.getDuration()
    
    def getTotalTime(self):
        return self.getOverheadTime()