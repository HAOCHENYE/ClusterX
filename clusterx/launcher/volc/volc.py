import json
import os
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Annotated, cast

from cyclopts import Parameter
from more_itertools import consume
from pydantic import model_validator
from rich.console import Console
from rich.prompt import Confirm
from rich.table import Column, Table

from clusterx.logger import getLogger

from ..base import (
    BaseCluster,
    BaseRunParams,
    JobSchema,
    JobStatus,
    NodeSchema,
    NodeStatus,
)
from .api import (
    VolcConfig,
    async_get_job,
    async_get_log,
    create_job,
    get_job,
    get_jobs_list,
    get_log,
    stop_job,
)
from .volc_info import VolcPartition

logger = getLogger("clusterx.launcher.volc")


@Parameter(name="*")
class VolcRunParams(BaseRunParams[VolcPartition]):
    image: Annotated[str | None, Parameter(help="镜像 url，默认使用 `~/.config/cluster.yaml` 设置的镜像")] = None
    tasks_per_node: Annotated[int, Parameter(show=False)] = 1
    retry: Annotated[bool, Parameter(help="任务失败后是否自动重启任务")] = False
    priority: Annotated[int, Parameter(help="优先级，火山云优先级目前只支持 2，4，6")] = 2
    gpus_per_task: Annotated[int, Parameter(help="每个任务使用的GPU数")] = 0
    cpus_per_task: Annotated[int, Parameter(help="每个任务使用的CPU数")] = 0
    memory_per_task: Annotated[int, Parameter(help="每个任务使用的内存, 例如 `1800`，单位为 GB")] = 0
    tmpdir: Annotated[str | None, Parameter(help="任务节点可访问公共目录，默认使用 clusterx.yaml 中的路径")] = None

    @model_validator(mode="before")
    def convert_input(cls, data):
        if "include" in data:
            logger.warning("暂不支持 include 参数，如有强需求可以提需")

        if "exclude" in data:
            logger.warning("暂不支持 exclude 参数，如有强需求可以提需")

        if "group" in data:
            logger.warning("暂不支持 group 参数，如有强需求可以提需")
        return super().convert_input(data)


STATUS_MAPPING = {
    "Queue": JobStatus.QUEUING,
    "Running": JobStatus.RUNNING,
    "Abnormal": JobStatus.FAILED,
    "Stopping": JobStatus.RUNNING,
    "Stopped": JobStatus.STOPPED,
    "Deleting": JobStatus.RUNNING,
    "Deploying": JobStatus.QUEUING,
    "None": JobStatus.QUEUING,
    "Killed": JobStatus.STOPPED,
    "Success": JobStatus.SUCCEEDED,
    "Failed": JobStatus.FAILED,
    "Waiting": JobStatus.QUEUING,
    "FailedHolding": JobStatus.FAILED,
}


class VolcCluster(BaseCluster[VolcRunParams, VolcPartition]):
    def __init__(self, cfg: Path | None = None):
        from clusterx import CLUSTERX_CONFIG, CLUSTER_MAPPING
        # 火山云的 setup 以来 volcengine 的安装，此处检查 volc 相关的以来能否正确 import
        if CLUSTER_MAPPING["volc"]["params"] is None:
            tb = CLUSTER_MAPPING["volc"]["traceback"]
            raise RuntimeError(f"VolcCluster is not available: {tb}")

        super().__init__()

        if CLUSTERX_CONFIG.volc is None:
            raise ValueError("未找到火山云配置，请重新配置")

        if (default_cfg := CLUSTERX_CONFIG.volc.get("config")) is not None:
            self.volc_cfg = VolcConfig(cfg or Path(default_cfg))
        else:
            self.volc_cfg = VolcConfig(cfg)

        self.volc_clusterx_cfg = CLUSTERX_CONFIG.volc

    def run(self, run_params: VolcRunParams) -> JobSchema:
        cmd = self._build_command(run_params.cmd, run_params.no_env)
        if run_params.tmpdir is not None:
            tmpdir = Path(run_params.tmpdir)
        else:
            assert self.volc_clusterx_cfg["tmpdir"], "未在配置文件中发现 tmpdir, 请手工设置"
            tmpdir = Path(self.volc_clusterx_cfg["tmpdir"])

        cmd_path = tmpdir / f"{uuid.uuid4()}.sh"
        cmd_path.parent.mkdir(exist_ok=True, parents=True)
        with open(cmd_path, "w") as f:
            f.write(cmd)
        cmd = f"bash {cmd_path}"

        default_image = self.volc_cfg.cfg.get("ImageUrl")
        if default_image is None and run_params.image is None:
            raise ValueError("未指定镜像")

        image = cast(str, run_params.image or default_image)

        # default_data_sources: str = CLUSTERX_CONFIG["aliyun"].get("data_sources")
        cfg = self.volc_cfg

        if run_params.partition is None:
            partition = cfg.cfg["ResourceQueueName"]
        else:
            partition = run_params.partition.name

        job_id = create_job(
            cmd=cmd,
            workspace_id=VolcPartition[partition],
            job_name=run_params.job_name,
            image=image,
            podcount=run_params.num_nodes,
            gpus_per_task=run_params.gpus_per_task,
            cpus_per_task=run_params.cpus_per_task,
            memory_per_task=run_params.memory_per_task,
            retry=run_params.retry,
            envs=None,
            cfg=cfg,
            priority=run_params.priority,
            preemptible=run_params.preemptible,
        )["Id"]

        job_info = get_job(job_id, cfg=cfg)["job_info"]
        job_schema = self._create_job_schema(job_info)

        logger.info(f"任务 {job_id} 创建成功")
        logger.info(f"任务命令 {job_schema.cmd}")
        logger.info(job_schema.model_dump_json(indent=4))
        return job_schema

    def stats_node(self, /, partition: VolcPartition, *, out: Path | None = None, verbose=False) -> list[NodeSchema]:
        raise NotImplementedError("火山云暂不支持统计节点信息")

    def get_node_info(self, /, node: str, *, out: Path | None = None, verbose=False) -> NodeSchema:
        raise NotImplementedError("火山云暂不支持获取节点信息")

    def stop(
        self,
        *,
        job_id: str | None = None,
        regex: str | None = None,
        group: str | None = None,
        user: str | None = None,
        partition: VolcPartition | None = None,
        status: JobStatus | None = None,
        confirm: bool = False,
    ) -> None:
        if group is not None:
            raise NotImplementedError("暂不支持按照分组删除任务")

        if job_id is not None:
            stop_job(job_id, cfg=self.volc_cfg)
            logger.info(f"任务 {job_id} 已停止")
        else:
            jobs_schema = self.list_jobs(partition=partition, status=status, regex=regex, user=user, num=1000)
            filtered_jobs: list[JobSchema] = []
            for job_schema in jobs_schema:
                if job_schema.status not in [JobStatus.QUEUING, JobStatus.RUNNING]:
                    continue

                # if job_schema["UserId"] != user:
                #     continue
                #
                if partition is not None and job_schema.partition != partition.name:
                    continue

                job_name = job_schema.job_name
                if job_name is None:
                    raise RuntimeError("发现任务名为 `None`，这可能是 `clusterx` 的 bug，请反馈")

                if status is not None and job_schema.status != status:
                    continue
                filtered_jobs.append(job_schema)

            if not filtered_jobs:
                logger.info("未找到匹配的任务")
                return

            if confirm:
                console = Console()
                confirm_check = Confirm("是否删除以上任务？", console=console)

                table = Table(
                    Column("任务名", min_width=40, max_width=80, overflow="ellipsis"),
                    Column("任务 ID", max_width=30),
                    Column("任务状态", max_width=20),
                    Column("用户名", max_width=20),
                    Column("所属分区", max_width=20),
                )
                for job_schema in filtered_jobs:
                    table.add_row(
                        job_schema.job_name,
                        job_schema.job_id,
                        job_schema.status,
                        job_schema.user,
                        str(job_schema.partition),
                    )
                console.print(table, highlight=True)

                if not confirm_check.ask():
                    return

            job_id_list = [i.job_id for i in filtered_jobs]

            with ThreadPoolExecutor(max_workers=16) as pool:
                consume(pool.map(stop_job, job_id_list))

    def list_jobs(
        self,
        *,
        group: str | None = None,
        user: Annotated[str | None, Parameter(help="火山云目前不支持传入用户 ID")] = None,
        partition: VolcPartition | None = None,
        status: JobStatus | None = None,
        regex: str | None = None,
        num: int = 100,
        out: Path | None = None,
        verbose: bool = False,
    ) -> list[JobSchema]:
        if user is not None:
            raise NotImplementedError("火山云暂不支持传入用户 ID")

        job_list = get_jobs_list(
            workspace_name=partition.name if partition is not None else None, num=num, cfg=self.volc_cfg
        )

        _jobs_schema: list[JobSchema] = []
        for job_info in job_list:
            _jobs_schema.append(self._create_job_schema(job_info))

        jobs_schema: list[JobSchema] = []
        for job_schema in _jobs_schema:
            # if user is not None and job_schema.user != str(user):
            #     continue

            if status is not None and status != job_schema.status:
                continue

            job_name = job_schema.job_name
            if job_name is None:
                raise RuntimeError("发现任务名为 `None`，这可能是 `clusterx` 的 bug，请反馈")

            if regex is not None and not re.search(regex, job_name):
                continue

            jobs_schema.append(job_schema)

        if verbose:
            console = Console()
            table = Table(
                Column("任务名", min_width=40, max_width=80, overflow="ellipsis"),
                Column("任务 ID", max_width=30, overflow="fold"),
                Column("任务状态", max_width=20),
                Column("使用 GPU", max_width=20),
                Column("使用 CPU", max_width=20),
                Column("使用内存", max_width=20),
                Column("占用节点数", max_width=20),
                Column("用户名", max_width=20),
                Column("所属分区", max_width=20),
                Column("创建时间", max_width=20),
                Column("结束时间", max_width=20),
            )
            for job_schema in jobs_schema:
                table.add_row(
                    job_schema.job_name,
                    job_schema.job_id,
                    job_schema.status,
                    str(job_schema.gpus_per_node),
                    str(job_schema.cpus_per_node),
                    f"{job_schema.memory}GB",
                    str(job_schema.num_nodes),
                    job_schema.user,
                    job_schema.partition.name
                    if isinstance(job_schema.partition, VolcPartition)
                    else job_schema.partition,
                    job_schema.start_time,
                    job_schema.end_time,
                )
            console.print(table, highlight=True)
            console.print()

        if out is not None:
            out.parent.mkdir(exist_ok=True, parents=True)
            with out.open("w") as f:
                json.dump(job_list, f)

        return jobs_schema

    def get_log(self, job_id: str) -> str:
        logs = get_log(job_id, cfg=self.volc_cfg)
        if isinstance(logs, list):
            logs = "\n".join(logs)
        return logs

    def get_job_info(self, job_id: str, verbose=False) -> JobSchema:
        job_info = get_job(job_id, cfg=self.volc_cfg)
        return self._get_job_info(job_info, verbose=verbose)

    def _get_job_info(self, job_info: dict, verbose=False) -> JobSchema:
        job_schema = self._create_job_schema(job_info["job_info"], job_info["container_info"])

        if verbose:
            console = Console()
            console.print(job_schema.model_dump())

        return job_schema

    def _build_command(self, cmd: str, no_env: bool) -> str:
        _cmd = self._build_pytorch_ddm_cmd()
        if not no_env:
            env_cmd = ""
            inherited_env = {k: v for k, v in os.environ.items() if v != ""}
            for k, v in inherited_env.items():
                env_cmd += f"export {k}='{v}'\n"
            _cmd += env_cmd
            _cmd += f"cd {os.getcwd()}\n"
        cmd = _cmd + cmd
        return cmd

    def _build_pytorch_ddm_cmd(self):
        cmd = (
            "export MASTER_ADDR=$MLP_WORKER_0_HOST\n"
            "export MASTER_PORT=$MLP_WORKER_0_PORT\n"
            "export WORLD_SIZE=$MLP_WORKER_NUM\n"
            "export RANK=$MLP_ROLE_INDEX\n"
        )
        return cmd

    def _get_node_status(self, node_info: dict) -> NodeStatus:
        status = node_info["status"]
        if status in ["SchedulingDisabled", "NotReady"]:
            return NodeStatus.DRAIN
        elif status == "Ready":
            if node_info["Jobs"] is None:
                return NodeStatus.IDLE
            else:
                return NodeStatus.MIXED
        else:
            return NodeStatus.UNRECORGNIZED

    def _get_job_status(self, job_info: dict) -> JobStatus:
        status = job_info["State"]
        return STATUS_MAPPING.get(status, status)

    def _get_job_partition(self, job_info: dict) -> VolcPartition | str:
        if (partition := job_info["ResourceQueueId"]) in VolcPartition.__members__.values():
            partition = VolcPartition(partition)
        return partition

    def stats_node_cli(
        self,
        partition: Annotated[VolcPartition, Parameter(help="分区名")],
        /,
        *,
        out: Annotated[Path | None, Parameter(name=["--out", "-o"])] = None,
        verbose: Annotated[bool, Parameter(name=["--verbose", "-v"])] = True,
    ):
        """统计分区的节点信息，在底层设置 verbose 和  out 接口是为了方便记录集群相关的完整信息."""
        self.stats_node(partition=partition, out=out, verbose=verbose)

    def list_cli(
        self,
        *,
        group: str | None = None,
        user: Annotated[str | None, Parameter(name=["--user", "-u"])] = None,
        partition: Annotated[VolcPartition | None, Parameter(name=["-p", "--partition"])] = None,
        status: Annotated[JobStatus | None, Parameter(name=["--status", "-s"])] = None,
        regex: Annotated[str | None, Parameter(help="匹配任务名的正则表达式")] = None,
        num: Annotated[int, Parameter(help="匹配任务名的正则表达式")] = 20,
        out: Annotated[Path | None, Parameter(name=["--out", "-o"])] = None,
        verbose: Annotated[bool, Parameter(name=["--verbose", "-v"])] = True,
    ):
        self.list_jobs(
            group=group, user=user, partition=partition, regex=regex, status=status, num=num, out=out, verbose=verbose
        )

    def run_cli(
        self, *cmd: Annotated[str, Parameter(allow_leading_hyphen=True)], run_params: VolcRunParams | None = None
    ) -> JobSchema:
        if run_params is None:
            run_params = VolcRunParams()
        run_params.cmd = " ".join(cmd)
        return self.run(run_params)

    def stop_cli(
        self,
        *,
        job_id: Annotated[str | None, Parameter(name=["--job-id", "-j"])] = None,
        regex: Annotated[str | None, Parameter(help="匹配任务名的正则表达式")] = None,
        group: Annotated[str | None, Parameter(name=["--group", "-g"])] = None,
        user: Annotated[str | None, Parameter(name=["--user", "-u"])] = None,
        partition: Annotated[VolcPartition | None, Parameter(name=["-p", "--partition"])] = None,
        status: Annotated[JobStatus | None, Parameter(name=["-status", "-s"])] = None,
        confirm: Annotated[bool, Parameter(name=["--confirm", "-c"])] = True,
    ) -> None:
        self.stop(
            job_id=job_id, regex=regex, group=group, user=user, partition=partition, status=status, confirm=confirm
        )

    def _create_job_schema(self, job_info: dict, container_info: dict | None = None) -> JobSchema:
        # TODO: 多个 jobspec 的情况？
        task_role_spec = job_info["TaskRoleSpecs"][0]
        gpu_num = task_role_spec["ResourceSpec"]["CustomResource"]["GPUNum"]
        cpu_num = task_role_spec["ResourceSpec"]["CustomResource"]["vCPU"]
        memory = task_role_spec["ResourceSpec"]["CustomResource"]["Memory"]
        num_nodes = task_role_spec["RoleReplicas"]

        # 火山云没有节点名，只有 pod 名
        nodes_name = None
        nodes_ip = None

        if container_info is not None and container_info:
            if "PodName" in container_info[0]:
                nodes_name = [i.get("PodName") for i in container_info]
            if "PrimaryIp" in container_info[0]:
                nodes_ip = [i.get("PrimaryIp") for i in container_info]

        return JobSchema(
            job_id=job_info["Id"],
            job_name=job_info["Name"],
            user=str(job_info["CreatorUserId"]),
            gpus_per_node=gpu_num,
            cpus_per_node=cpu_num,
            memory=str(memory),
            status=self._get_job_status(job_info),
            partition=self._get_job_partition(job_info),
            submit_time=job_info["CreateTime"],
            start_time=job_info["LaunchTime"],
            end_time=job_info["FinishTime"],
            cmd=job_info["EntrypointPath"],
            num_nodes=num_nodes,
            nodes=nodes_name,
            nodes_ip=nodes_ip,
        )

    async def async_get_log(self, job_id: str) -> str:
        return await async_get_log(job_id, cfg=self.volc_cfg)

    async def async_get_job_info(self, /, job_id: str, verbose=False) -> JobSchema:
        job_info = await async_get_job(job_id, cfg=self.volc_cfg)
        return self._get_job_info(job_info, verbose)
