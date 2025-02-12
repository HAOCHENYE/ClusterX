import asyncio
import fcntl
import getpass
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Generic, get_args

from cyclopts import Parameter
from pydantic import BaseModel, model_validator
from typing_extensions import Annotated, TypeVar

from clusterx.utils import Pager, StrEnum

DEFAULT_JOB_NAME = f"clusterx-{getpass.getuser()}"


def set_non_blocking(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


class JobStatus(StrEnum):
    QUEUING = "Queuing"
    RUNNING = "Running"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    UNRECORGNIZED = "unrecognized"
    STOPPED = "Stopped"


class NodeStatus(StrEnum):
    RUNNING = "running"
    IDLE = "idle"
    MIXED = "mixed"
    DRAIN = "drain"
    UNRECORGNIZED = "unrecognized"


T_partition = TypeVar("T_partition", bound=StrEnum)


class BaseRunParams(BaseModel, Generic[T_partition]):
    job_name: Annotated[str, Parameter(help="任务名", name=["-J", "--job-name"])] = "clustex"
    partition: Annotated[T_partition | None, Parameter(help="分区名", name=["-p", "--partition"])] = None
    cmd: Annotated[str, Parameter(show=False)] = ""
    num_nodes: Annotated[int, Parameter(help="节点数", name=["--num-nodes", "-N"])] = 1
    tasks_per_node: Annotated[int, Parameter(help="每个任务使用的节点数")] = 1
    gpus_per_task: Annotated[int, Parameter(help="每个任务使用的GPU数")] = 0
    cpus_per_task: Annotated[int, Parameter(help="每个任务使用的CPU数")] = 4
    memory_per_task: Annotated[int, Parameter(help="每个任务使用的内存, 例如 `1800`，单位为 GB")] = 10
    include: Annotated[str | None, Parameter(help="以 `,` 为分隔符，输入节点列表")] = None
    exclude: Annotated[str | None, Parameter(help="以 `,` 为分隔符，排除的节点列表")] = None
    priority: Annotated[int, Parameter(help="优先级")] = 1
    group: Annotated[str | None, Parameter(show=False)] = None
    retry: Annotated[bool, Parameter(help="任务失败时自动重启")] = False
    no_env: Annotated[bool, Parameter(help="是否让发起的任务继承当前的环境变量", negative=False)] = False
    preemptible: Annotated[bool | None, Parameter(help="是否开启闲时资源任务", negative=False)] = None

    @model_validator(mode="before")
    def convert_input(cls, data):
        partition_cls: T_partition = get_args(cls.model_fields["partition"].annotation)[0]
        if "partition" in data:
            partition = data["partition"].replace("-", "_")
            if partition in partition_cls.__members__.keys():
                partition = partition_cls[partition]
                data["partition"] = partition

        return data


T_run = TypeVar("T_run", bound=BaseRunParams)


class JobSchema(BaseModel):
    job_id: str
    user: str
    gpus_per_node: int
    cpus_per_node: int
    memory: str
    status: JobStatus | str
    num_nodes: int
    partition: StrEnum | str
    job_name: str | None = None
    nodes: list[str] | None = None
    nodes_ip: list[str] | None = None
    submit_time: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    cmd: str = ""


class NodeSchema(BaseModel):
    name: str
    total_gpu: int
    total_cpu: int
    total_memory: str
    status: NodeStatus
    jobs_schema: list[JobSchema] | None = None


class BaseCluster(ABC, Generic[T_run, T_partition]):
    _groups: dict
    _stats: dict

    @abstractmethod
    def run(self, run_params: T_run) -> JobSchema:
        raise NotImplementedError()

    @abstractmethod
    def stop(
        self,
        *,
        job_id: str | None = None,
        regex: str | None = None,
        group: str | None = None,
        user: str | None = None,
        partition: T_partition | None = None,
        status: JobStatus | None = None,
        confirm: bool = False,
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    def stats_node_cli(
        self,
        partition: Annotated[T_partition, Parameter(help="分区名")],
        /,
        *,
        out: Annotated[Path | None, Parameter(name=["--out", "-o"])] = None,
        verbose: Annotated[bool, Parameter(name=["--verbose", "-v"])] = True,
    ):
        """统计分区的节点信息，在底层设置 verbose 和  out 接口是为了方便记录集群相关的完整信息."""
        raise NotImplementedError()

    @abstractmethod
    def get_node_info(self, /, node: str, *, out: Path | None = None, verbose=False) -> NodeSchema:
        """获取节点详细信息, 在底层设置 verbose 和 out 接口是为了方便集群相关的完整信息."""
        raise NotImplementedError()

    @abstractmethod
    def list_jobs(
        self,
        *,
        group: str | None = None,
        user: str | None = None,
        partition: T_partition | None = None,
        status: JobStatus | None = None,
        regex: str | None = None,
        num: int = 20,
        out: Path | None = None,
        verbose: bool = False,
    ) -> list[JobSchema]:
        raise NotImplementedError()

    @abstractmethod
    def get_job_info(self, /, job_id: str, verbose=False) -> JobSchema:
        raise NotImplementedError()

    @abstractmethod
    def get_log(self, job_id: str) -> str:
        raise NotImplementedError()

    @abstractmethod
    async def async_get_log(self, job_id: str) -> str:
        """For getting log in pager mode asynchronously."""
        raise NotImplementedError()

    @abstractmethod
    async def async_get_job_info(self, /, job_id: str, verbose=False) -> JobSchema:
        raise NotImplementedError()

    def __init_subclass__(cls):
        cls._groups = {}
        cls._stats = {}
        super().__init_subclass__()

    @abstractmethod
    def list_cli(
        self,
        *,
        group: str | None = None,
        user: Annotated[str | None, Parameter(name=["--user", "-u"])] = None,
        partition: Annotated[T_partition | None, Parameter(name=["-p", "--partition"])] = None,
        status: Annotated[JobStatus | None, Parameter(name=["--status", "-s"])] = None,
        regex: Annotated[str | None, Parameter(help="匹配任务名的正则表达式")] = None,
        num: Annotated[int, Parameter(help="匹配任务名的正则表达式")] = 20,
        out: Annotated[Path | None, Parameter(name=["--out", "-o"])] = None,
        verbose: Annotated[bool, Parameter(name=["--verbose", "-v"])] = True,
    ):
        raise NotImplementedError()

    @abstractmethod
    def run_cli(
        self, *cmd: Annotated[str, Parameter(allow_leading_hyphen=True)], run_params: T_run | None = None
    ) -> JobSchema:
        raise NotImplementedError()

    def get_node_info_cli(self, node: str, /, *, out: Path | None = None, verbose=True):
        """获取节点详细信息, 在底层设置 verbose 和 out 接口是为了方便集群相关的完整信息."""
        self.get_node_info(node, out=out, verbose=verbose)

    def get_job_info_cli(self, /, job_id: str, verbose=True) -> JobSchema:
        return self.get_job_info(job_id, verbose=verbose)

    def get_log_cli(
        self,
        job_id: Annotated[str, Parameter(help="任务 ID")],
        streaming: Annotated[bool, Parameter(help="是否持续捕获运行中日志的输出")] = True,
    ):
        if streaming:
            pager = Pager(self._log_threading(job_id))
            asyncio.run(pager.run())
        else:
            print(self.get_log(job_id))

    @abstractmethod
    def stop_cli(
        self,
        *,
        job_id: Annotated[str | None, Parameter(name=["--job-id", "-j"])] = None,
        regex: Annotated[str | None, Parameter(help="匹配任务名的正则表达式")] = None,
        group: Annotated[str | None, Parameter(name=["--group", "-g"])] = None,
        user: Annotated[str | None, Parameter(name=["--user", "-u"])] = None,
        partition: Annotated[T_partition | None, Parameter(name=["-p", "--partition"])] = None,
        status: Annotated[JobStatus | None, Parameter(name=["-status", "-s"])] = None,
        confirm: Annotated[bool, Parameter(name=["--confirm", "-c"])] = False,
    ) -> None:
        self.stop(job_id=job_id, regex=regex, group=group, user=user, partition=partition, confirm=confirm)

    async def _log_threading(self, job_id):
        prev_logs = ""

        while True:
            log_status = (await self.async_get_job_info(job_id)).status
            cur_logs = await self.async_get_log(job_id)
            increment_logs = cur_logs[len(prev_logs) :]

            if not increment_logs:
                continue

            prev_logs = cur_logs
            for line in increment_logs.splitlines():
                yield line
            if log_status != JobStatus.RUNNING:
                break
