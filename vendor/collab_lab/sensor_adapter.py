"""Active sensor port. The default backend reads the digital simulation."""
import copy
import uuid
import time
from typing import Protocol
from .common import write_json
from .environment import ContractError
from .runtime import RuntimeFault


class SensorPort(Protocol):
    """Implement read() to return a normalized server sensor snapshot.

    Required keys: version, step, observed_at, nodes, links, flows, metrics, task.
    A production backend must also provide compatible execution and verification.
    """
    source: str

    def read(self) -> dict: ...


class SimulationSensorPort:
    source = "simulated_edge_sensor_adapter"

    def __init__(self, world):
        self.world = world

    def read(self):
        return self.world.observation()


class SensorAdapter:
    def __init__(self, session, port=None):
        self.session = session
        self.appliance=getattr(session,'appliance',False)
        self.port = port if port is not None else session.world if self.appliance else SimulationSensorPort(session.world)
        self.latest = None
        self.samples = {}
        self.sample_sequence = 0

    def read(self, arguments):
        if arguments:
            raise ContractError("read_sensor_adapter不需要参数")
        observation = self.port.read()
        required=('version','step','observed_at','readings','task') if self.appliance else ('version','step','observed_at','nodes','links','flows','metrics','task')
        if not isinstance(observation, dict) or not all(k in observation for k in required):
            raise ContractError("传感器返回缺少标准观测字段")
        result = {**copy.deepcopy(observation), "reading_id": uuid.uuid4().hex[:12],
                  "source": self.port.source, "sampling": "on_explicit_model_call"}
        self.sample_sequence += 1
        result.update(observation_id=result['reading_id'], sample_sequence=self.sample_sequence,
                      task_activation=self.session.tasks.active().get('activation_sequence',1),
                      environment_revision=observation.get('environment_revision',1),
                      **self.session.tasks.identity())
        self.latest = result
        self.samples[result['observation_id']] = (copy.deepcopy(result),time.monotonic())
        while len(self.samples)>self.session.config.get('sample_cache_limit',512):self.samples.pop(next(iter(self.samples)))
        write_json(self.session.directory / "sensor_reads" / (result["reading_id"] + ".json"), result)
        self.session.evidence.add("sensor_read", "edge", {
            "reading_id": result["reading_id"], "version": result["version"], "step": result["step"],
            "observed_at": result["observed_at"], "implicit_injection": False})
        return copy.deepcopy(result)

    def current(self):
        if self.latest is None:
            raise ContractError("尚无传感器观测，请先主动调用read_sensor_adapter")
        return copy.deepcopy(self.latest)

    def resolve(self, observation_id):
        if not isinstance(observation_id,str) or observation_id not in self.samples:
            raise RuntimeFault('OBSERVATION_REQUIRED','请主动读取传感器并引用真实observation_id')
        result, captured = self.samples[observation_id]
        if any(result.get(k)!=v for k,v in self.session.tasks.identity().items()):
            raise RuntimeFault('WRONG_TASK_OBSERVATION','这份观测不属于当前任务，请重新采样')
        current = self.session.world.snapshot()
        if result['version']!=current['version'] or result['environment_revision']!=current.get('environment_revision',1):
            raise RuntimeFault('STALE_OBSERVATION','观测对应现场已经变化，请重新采样后自主决策')
        if time.monotonic()-captured > self.session.config.get('observation_ttl_seconds',1800):
            raise RuntimeFault('STALE_OBSERVATION','观测已超过配置的时效，请重新采样')
        return copy.deepcopy(result)
