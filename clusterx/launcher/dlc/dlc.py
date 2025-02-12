import json
import os
import re
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from more_itertools import consume
from pydantic import model_validator
from requests import RequestException
from rich import print
from rich.console import Console
from rich.prompt import Confirm
from rich.style import Style
from rich.table import Column, Table
from rich.text import Text

from clusterx.logger import getLogger

from ..base import (
    BaseCluster,
    BaseRunParams,
    JobSchema,
    JobStatus,
    NodeSchema,
    NodeStatus,
)
from .aliyun_info import AliyunPartitionEnum, QuotaEnum
from .api import (
    _AliyunConfig,
    async_get_job,
    async_get_log,
    create_job,
    get_all_node_in_cluster,
    get_all_pods_logs,
    get_job,
    get_jobs_list,
    get_log,
    get_quota,
    stop_job,
)

logger = getLogger("clusterx.launcher.dlc")


class NodeSchemaVerbose(NodeSchema):
    free_gpu: int
    free_cpu: int
    free_memory: str
    user_count: int
    abnormal_pod_count: int
    spot_task: int


@Parameter(name="*")
class DLCRunParams(BaseRunParams[AliyunPartitionEnum]):
    retry: Annotated[bool, Parameter(help="是否使用 AIMaster 检查任务状态，自动重启")] = False
    aimaster: Annotated[bool, Parameter(help="是否使用 AIMaster 检查任务状态，自动重启", show=False)] = False
    image: Annotated[str | None, Parameter(help="镜像 url，默认使用 `~/.config/cluster.yaml` 设置的镜像")] = None
    data_sources: Annotated[
        str | None,
        Parameter(help="挂载数据源，默认使用 `~/.config/cluster.yaml` 设置的数据源，以 `,` 为分隔符输入，例如 `aaa,bbb,ccc`"),
    ] = None  # noqa: E501
    exclude: Annotated[str | None, Parameter(show=False)] = None
    tasks_per_node: Annotated[int, Parameter(show=False)] = 1
    shared_memory: Annotated[int | None, Parameter(help="每个任务使用的shared_memory内存, 例如 `1800`，单位为 GB")] = None
    tmpdir: Annotated[str | None, Parameter(help="任务节点可访问公共目录，默认使用 clusterx.yaml 中的路径")] = None
    priority_preemptible: Annotated[bool | None, Parameter(help="是否开启优先级抢占式任务", negative=False)] = None

    @model_validator(mode="before")
    def convert_input(cls, data):
        data = super().convert_input(data)
        if "aimaster" in data:
            # TODO: Deprecate `aimster`
            logger.warning("`aimaster` will be deprecated soon.")
            data["retry"] = data["aimaster"]

        if "exclude" in data:
            logger.warning("暂不支持 exclude 参数，如有强需求可以提需")

        if "group" in data:
            logger.warning("暂不支持 group 参数，如有强需求可以提需")

        if (gpus_per_task := data.get("gpus_per_task")) is not None:
            if isinstance(gpus_per_task, str):
                gpus_per_task = int(gpus_per_task)

            if "memory_per_task" not in data:
                data["memory_per_task"] = 150 * gpus_per_task

            if "cpus_per_task" not in data:
                data["cpus_per_task"] = 12 * gpus_per_task

        return data


tmp_script_path = Path("/tmp")
STATUS_MAPPING = {
    "Initialized": JobStatus.RUNNING,
    "Starting": JobStatus.RUNNING,
    "WorkflowServiceStarting": JobStatus.RUNNING,
    "Running": JobStatus.RUNNING,
    "Terminating": JobStatus.RUNNING,
    "Terminated": JobStatus.FAILED,
    "Succeeded": JobStatus.SUCCEEDED,
    "Failed": JobStatus.FAILED,
    "Creating": JobStatus.QUEUING,
    "Queuing": JobStatus.QUEUING,
    "Stopped": JobStatus.STOPPED,
    "Dequeued": JobStatus.QUEUING,
}


class AliyunCluster(BaseCluster[DLCRunParams, AliyunPartitionEnum]):
    def __init__(self, dlc_cfg: Path | None = None):
        from clusterx import CLUSTERX_CONFIG

        super().__init__()

        if CLUSTERX_CONFIG.aliyun is None:
            raise ValueError(f"未配置阿里云集群，请重新配置 {CLUSTERX_CONFIG}")

        if (default_cfg := CLUSTERX_CONFIG.aliyun.get("config")) is not None:
            self.dlc_cfg = _AliyunConfig(dlc_cfg or Path(default_cfg))
        else:
            self.dlc_cfg = _AliyunConfig(dlc_cfg)

        self.aliyun_clusterx_cfg = CLUSTERX_CONFIG.aliyun

    def run(self, run_params: DLCRunParams) -> JobSchema:
        cmd = self._build_command(run_params.cmd, run_params.no_env)

        if run_params.tmpdir is not None:
            tmpdir = Path(run_params.tmpdir)
        else:
            assert self.aliyun_clusterx_cfg["tmpdir"], "未在配置文件中发现 tmpdir, 请手工设置"
            tmpdir = Path(self.aliyun_clusterx_cfg["tmpdir"])

        cmd_name = uuid.uuid4()
        cmd_path = tmpdir / f"{cmd_name}.sh"
        cmd_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cmd_path, "w") as f:
            f.write(cmd)
        cmd = f"bash {cmd_path}"

        default_image: str = self.aliyun_clusterx_cfg["image"]
        image = run_params.image or default_image

        if not image:
            raise RuntimeError("请设置镜像地址")
        # default_data_sources: str = CLUSTERX_CONFIG["aliyun"].get("data_sources")
        cfg = self.dlc_cfg

        if run_params.partition is None:
            partition = self.aliyun_clusterx_cfg["partition"]
        else:
            partition = run_params.partition.name

        if not partition:
            raise RuntimeError("请设置分区名")
        workspace_id = AliyunPartitionEnum[partition].value

        # Suck aliyun dlc api... DLC Jobs cannot be created with some environment variables
        # Just temporarily comment this here... Maybe someday aliyun could support `env` argument better

        # exclude_substring = [
        #     "KUBERNETES",
        #     "ALI",
        #     "dsw",
        #     "DSW",
        #     "PAI",
        #     "DLC",
        # ]
        #
        # exclude_envs = {
        #     "REGION",
        #     "TZ",
        #     "NVIDIA_VISIBLE_DEVICES",
        #     "INSTANCE_EXTRA_INFO",
        #     "NVIDIA_REQUIRE_CUDA",
        #     "JUPYTER_NAME",
        # }
        # inherited_env = {
        #     k: v
        #     for k, v in os.environ.items()
        #     if v != "" and k not in exclude_envs and all(i not in k for i in exclude_substring)
        # }

        # inherited_env = {k: v for k, v in list(inherited_env.items())[int(len(inherited_env) * 3/8):int(len(inherited_env) * 1/2)]}  # noqa: E501
        # print(inherited_env)
        # choose half of the envs

        resp = create_job(
            cmd=cmd,
            workspace_id=workspace_id,
            job_name=run_params.job_name or "clusterx",
            image=image,
            podcount=run_params.num_nodes,
            cpus_per_task=run_params.cpus_per_task,
            gpus_per_task=run_params.gpus_per_task,
            memory_per_task=run_params.memory_per_task,
            shared_memory=run_params.shared_memory,
            aimaster=run_params.retry,
            cfg=cfg,
            envs=None,  # `envs` variable maybe leads to the failed of creating job
            priority=run_params.priority,
            data_sources=list(
                set(
                    self.aliyun_clusterx_cfg["data_sources"] + (run_params.data_sources.split(",") if run_params.data_sources else [])
                )
            ),  # noqa: E501
            include=run_params.include,
            preemptible=run_params.preemptible,
            priority_preemptible=run_params.priority_preemptible,
            # data_sources=run_params.data_sources or default_data_sources,  TODO: 挂载数据源
        )
        job_id = resp["JobId"]
        job_schema = self.get_job_info(job_id)

        logger.info(f"任务 {job_id} 创建成功")
        logger.info(f"任务命令 {job_schema.cmd}")
        logger.info(job_schema.model_dump_json(indent=4))
        cmd_json = tmpdir / f"{cmd_name}.json"
        with open(cmd_json, "w") as f:
            json.dump(job_schema.model_dump(), f, indent=4, ensure_ascii=False)
        logger.info(f"任务信息已保存到 {cmd_json}")
        return job_schema

    def stats_node(
        self, /, partition: AliyunPartitionEnum, *, out: Path | None = None, verbose=False
    ) -> list[NodeSchema]:
        # TODO: 目前只适配了 H 卡，A 卡看情况支持
        # 获取集群内的所有GPU节点名称
        all_nodes = get_all_node_in_cluster(cfg=self.dlc_cfg)["ClusterNodeInfos"]["items"]
        node_infos = []

        for node in all_nodes:
            if node["gpuType"] == "NVIDIA-L20Y":
                node_infos.append(node)

        if partition == QuotaEnum.all:
            current_quotas = [
                (AliyunPartitionEnum[name], QuotaEnum[name]) for name in AliyunPartitionEnum.__members__.keys()
            ]
        else:
            current_quotas = [(partition, QuotaEnum[partition.name])]

        # 定义当前使用的配额
        # current_quotas = {'quota1eidzbbfuz2': 'alillm_h3'}
        # 筛选指定节点的配额与对应的节点名称
        quota_nodes = []
        for partition_enum, quota_enum in current_quotas:
            quota_desc = get_quota(quota_enum.value, partition_enum.value, cfg=self.dlc_cfg)
            if "TotalQuota" in quota_desc and len(quota_desc["TotalQuota"]["NodeNames"]) > 0:
                quota_nodes.extend(quota_desc["TotalQuota"]["NodeNames"])

        if not quota_nodes:
            messages = "该分区没有绑定任何节点，因此和其他分区共享以上所有节点"
            partition = AliyunPartitionEnum.unbinded
        else:
            messages = f"{partition.name} 分区节点情况如上"

        # node_table: list[list[str]] = [""]

        table_node_data: list[list] = []
        node_info_status: dict[str, list] = defaultdict(list)

        for node_info in node_infos:
            node_info_status[node_info["status"]].append(node_info)

        node_info_status = {k: sorted(v, key=lambda x: x["FreeGPU"], reverse=True) for k, v in node_info_status.items()}
        node_infos = list(chain.from_iterable(node_info_status.values()))

        # 当分区未绑定节点时，排除已绑定节点。
        if partition == QuotaEnum.unbinded:
            binded_node: list[str] = []
            for name, quota_id in QuotaEnum.__members__.items():
                quota_desc = get_quota(quota_id, AliyunPartitionEnum[name].value, cfg=self.dlc_cfg)
                if "TotalQuota" in quota_desc:
                    binded_node.extend(quota_desc["TotalQuota"]["NodeNames"])
            node_infos = [node_info for node_info in node_infos if node_info["nodeName"] not in binded_node]
        elif partition != QuotaEnum.all:
            node_infos = [node_info for node_info in node_infos if node_info["nodeName"] in quota_nodes]

        if verbose:
            table = Table(
                Column("节点名", max_width=20),
                Column("状态", max_width=20),
                Column("空闲CPU/所有CPU", max_width=20),
                Column("空闲内存/所有内存", max_width=20),
                Column("空闲GPU/所有GPU", max_width=20),
                Column("用户数量", max_width=20),
                Column("异常 Pod 数量", max_width=20),
                Column("抢占任务数", max_width=20),
            )

            for node_info in node_infos:
                node_info["SpotTask"] = 0
                for job in node_info["Jobs"] or []:
                    job_id = job["JobId"]
                    if job["WorkspaceId"] != partition.value:
                        logger.warning(f"获取任务 {job_id} 信息失败，用户：{job['UserId']}")
                        node_info["SpotTask"] += 1
                        continue

                    # TODO: Aliyun auth cannot get preemitible job from other partition now ...

                    # if job_id.startswith("dsw"):
                    #     continue
                    # try:
                    #     job_info = get_job(job_id, cfg=self.dlc_cfg)
                    # except RequestException:
                    #     node_info["SpotTask"] += 1
                    #     continue

                    # settings = job_info["Settings"]
                    # if settings["EnablePreemptibleJob"] is False:
                    #     continue

                    # if job_info["WorkspaceID"] != partition.value:
                    #     resource = job_info["JobSpecs"][0]["ResourceConfig"]
                    # node_info["SpotGPU"] += int(resource["GPU"])

            table_node_data = []
            nodes_schema: list[NodeSchemaVerbose] = []
            for node_info in node_infos:
                node_name = node_info["nodeName"]
                status = self._get_node_status(node_info)
                free_cpu = node_info["FreeCPU"]
                total_cpu = node_info["totalCPU"]
                free_memory = node_info["FreeMemory"] // 1000000000
                total_memory = node_info["totalMemory"] // 1000000000
                free_gpu = node_info["FreeGPU"]
                total_gpu = node_info["totalGPU"]
                user_count = node_info["userCount"]
                abnormal_pod_count = node_info["AbnormalPodCount"]
                spot_task = node_info["SpotTask"]

                table_node_data.append(
                    [
                        node_name,
                        status,
                        f"{free_cpu}/{total_cpu}",
                        f"{free_memory}/{total_memory}",
                        f"{free_gpu}/{total_gpu}",
                        user_count,
                        abnormal_pod_count,
                        spot_task,
                    ]
                )
                node_schema = NodeSchemaVerbose(
                    name=node_name,
                    status=status,
                    free_gpu=free_gpu,
                    total_gpu=total_gpu,
                    free_cpu=free_cpu,
                    total_cpu=total_cpu,
                    free_memory=f"{total_memory}",
                    total_memory=f"{total_memory}",
                    user_count=user_count,
                    abnormal_pod_count=abnormal_pod_count,
                    spot_task=spot_task,
                )
                nodes_schema.append(node_schema)
            for row in table_node_data:
                table.add_row(*map(str, row))

            summarize = Table("总节点数", "正常节点数", "坏节点数", "空闲 GPU 数", "GPU 完全空闲节点数")
            summarize.add_row(
                str(len(node_infos)),
                str(len(list(i for i in node_infos if i["status"] == "Ready"))),
                str(len(list(i for i in node_infos if i["status"] != "Ready"))),
                str(sum(i["FreeGPU"] for i in node_infos)),
                str(len(list(i for i in node_infos if i["FreeGPU"] == 8))),
            )

            print(table)
            print(summarize)
            print(Text(messages, Style(color="green")))

        if out is not None:
            out.parent.mkdir(exist_ok=True, parents=True)
            with out.open("w") as f:
                json.dump(table_node_data, f, indent=4, ensure_ascii=True)

        if verbose:
            return nodes_schema

        nodes_schema: list[NodeSchema] = []

        for node_info in node_infos:
            name = node_info["nodeName"]
            status = self._get_node_status(node_info)
            node_schema = NodeSchema(
                name=name,
                total_gpu=node_info["totalGPU"],
                total_cpu=node_info["totalCPU"],
                total_memory=f"{node_info['totalMemory'] // 1000000000}",
                status=status,
            )
            nodes_schema.append(node_schema)
        return nodes_schema

    def get_node_info(self, /, node: str, *, out: Path | None = None, verbose=False) -> NodeSchema:
        all_nodes = get_all_node_in_cluster(cfg=self.dlc_cfg)["ClusterNodeInfos"]["items"]
        for node_info in all_nodes:
            if node_info["nodeName"] == node:
                break
        else:
            raise ValueError(f"未找到节点 {node}")

        jobs_schema: list[JobSchema] = []
        for job in node_info["Jobs"] or []:
            job_id = job["JobId"]
            try:
                job_info = get_job(job_id, cfg=self.dlc_cfg)
            except RequestException:
                print(f"获取任务 {job_id} 信息失败")
                jobs_schema.append(
                    JobSchema(
                        job_id=job_id,
                        user="无法获取",
                        gpus_per_node=0,
                        cpus_per_node=0,
                        memory="无法获取",
                        status=JobStatus.UNRECORGNIZED,
                        num_nodes=0,
                        partition="",
                    )
                )
                continue
            job_spec_array = job_info["JobSpecs"]
            assert len(job_spec_array) == 1, "未处理的异常，请联系 clusterx 开发人员"
            job_schema = self._get_job_info(job_id, job_info)
            jobs_schema.append(job_schema)

        node_schema = NodeSchema(
            name=node_info["nodeName"],
            total_gpu=node_info["totalGPU"],
            total_cpu=node_info["totalCPU"],
            total_memory=f"{node_info['totalMemory'] // 1000000000}GB",
            status=self._get_node_status(node_info),
            jobs_schema=jobs_schema,
        )

        if verbose:
            print(f"{node} 任务信息：")
            table = Table(
                Column("任务 ID", max_width=20),
                Column("用户", max_width=20),
                Column("GPU 数", max_width=20),
                Column("CPU 数", max_width=20),
                Column("内存", max_width=20),
            )
            for job in jobs_schema:
                table.add_row(
                    job.job_id,
                    job.user,
                    str(job.gpus_per_node),
                    str(job.cpus_per_node),
                    job.memory,
                )
            print(table)
            print(f"节点状态: {node_schema.status}")
            print(node_schema)
        return node_schema

    def stop(
        self,
        *,
        job_id: str | None = None,
        regex: str | None = None,
        group: str | None = None,
        user: str | None = None,
        partition: AliyunPartitionEnum | None = None,
        status: JobStatus | None = None,
        confirm: bool = False,
    ) -> None:
        if group is not None:
            raise NotImplementedError("暂不支持按照分组删除任务")

        if job_id is not None:
            stop_job(job_id, cfg=self.dlc_cfg)
            logger.info(f"任务 {job_id} 已停止")
        else:
            assert user is not None, "不传入 job_ids 时，必须传入 user 参数"
            jobs = get_jobs_list(cfg=self.dlc_cfg, status="Running,Queuing,Creating", num=1000)
            filtered_jobs: list[dict] = []
            for job in jobs:
                if job["UserId"] != user:
                    continue
                if partition is not None and job["WorkspaceName"] != partition.name:
                    continue
                if regex is not None and not re.search(regex, job["DisplayName"]):
                    continue
                job_status = self._get_job_status(job)
                if status is not None and job_status != status:
                    continue
                filtered_jobs.append(job)

            if not filtered_jobs:
                logger.info("未找到匹配的任务")
                return

            if confirm:
                console = Console()
                confirm_check = Confirm("是否删除以上任务？", console=console)

                table = Table(
                    Column("任务名", min_width=40, max_width=80, overflow="ellipsis"),
                    Column("任务 ID", max_width=20),
                    Column("任务状态", max_width=20),
                    Column("用户名", max_width=20),
                    Column("所属分区", max_width=20),
                )
                for job in filtered_jobs:
                    table.add_row(
                        job["DisplayName"],
                        job["JobId"],
                        job["Status"],
                        job["UserId"],
                        job["WorkspaceName"],
                    )
                console.print(table, highlight=True)

                if not confirm_check.ask():
                    return

            job_id_list = [i["JobId"] for i in filtered_jobs]

            with ThreadPoolExecutor(max_workers=16) as pool:
                consume(pool.map(stop_job, job_id_list))

    def list_jobs(
        self,
        *,
        group: str | None = None,
        user: str | None = None,
        partition: AliyunPartitionEnum | None = None,
        status: JobStatus | None = None,
        regex: str | None = None,
        num: int = 20,
        out: Path | None = None,
        verbose: bool = False,
    ) -> list[JobSchema]:
        jobs = get_jobs_list(status=status.value if status is not None else status, cfg=self.dlc_cfg, num=num)
        job_list: list[dict] = []
        for job_info in jobs:
            if user is not None and job_info["UserId"] != user:
                continue
            if partition is not None and job_info["WorkspaceName"] != partition.name:
                continue
            if regex is not None and not re.search(regex, job_info["DisplayName"]):
                continue
            job_list.append(job_info)

        if verbose:
            console = Console()

            table = Table(
                Column("任务名", min_width=40, max_width=80, overflow="ellipsis"),
                Column("任务 ID", max_width=20),
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
            for job_info in job_list:
                table.add_row(
                    job_info["DisplayName"],
                    job_info["JobId"],
                    job_info["Status"],
                    str(job_info["RequestGPU"]),
                    str(job_info["RequestCPU"]),
                    f"{job_info['RequestMemory']}GB",
                    str(len(set(job_info["NodeNames"]))),
                    job_info["UserId"],
                    job_info["WorkspaceName"],
                    job_info["GmtRunningTime"],
                    job_info["GmtFinishTime"],
                )
            console.print(table, highlight=True)
            console.print()

        if out is not None:
            out.parent.mkdir(exist_ok=True, parents=True)
            with out.open("w") as f:
                json.dump(job_list, f)

        jobs_schema: list[JobSchema] = []
        for job_info in job_list:
            job_schema = self._get_job_info(job_info["JobId"], job_info)
            jobs_schema.append(job_schema)
        return jobs_schema

    def get_log(self, job_id: str) -> str:
        logs = get_log(job_id, cfg=self.dlc_cfg)
        return "\n".join(logs)

    def get_all_pods_logs(self, job_id: str) -> list[str]:
        return get_all_pods_logs(job_id, cfg=self.dlc_cfg)

    def get_job_info(self, job_id: str, verbose=False) -> JobSchema:
        job_info = get_job(job_id, cfg=self.dlc_cfg)
        return self._get_job_info(job_id, job_info, verbose)

    def _get_job_info(self, job_id, job_info: dict, verbose=False) -> JobSchema:
        job_spec_array = job_info["JobSpecs"]
        if job_spec_array is not None:
            # NOTE: This maybe a bug of aliyun api. Sometimes the acquired job_info does not contain JobSpecs!
            # Howevery we can see the job spec in the PAI webset.
            assert len(job_spec_array) == 1
            job_spec = job_spec_array[0]
            resource = job_spec["ResourceConfig"]
            nnodes = job_info["JobSpecs"][0]["PodCount"]
        else:
            resource = {
                "GPU": 0,
                "CPU": 0,
                "Memory": 0,
            }
            nnodes = 0

        job_schema = JobSchema(
            job_id=job_id,
            job_name=job_info["DisplayName"],
            user=job_info["UserId"],
            gpus_per_node=resource["GPU"] or 0,
            cpus_per_node=resource["CPU"] or 0,
            memory=resource["Memory"] or "0",
            status=STATUS_MAPPING.get(job_info["Status"], "unrecognized"),
            nodes=[f"{pod['PodId']}:{pod['NodeName']}" for pod in (job_info.get("Pods") or [])],
            num_nodes=nnodes,
            nodes_ip=[pod["Ip"] for pod in (job_info.get("Pods") or [])],
            partition=self._get_job_partition(job_info),
            submit_time=job_info["GmtSubmittedTime"],
            start_time=job_info["GmtRunningTime"],
            end_time=job_info["GmtFinishTime"],
            cmd=job_info["UserCommand"],
        )

        if verbose:
            console = Console()
            console.print(job_schema.model_dump())

        return job_schema

    def _build_command(self, cmd: str, no_env: bool) -> str:
        _cmd = ""
        if not no_env:
            env_cmd = ""
            inherited_env = {k: v for k, v in os.environ.items() if v != ""}
            for k, v in inherited_env.items():
                env_cmd += f"export {k}='{v}'\n"
            _cmd += env_cmd
            _cmd += f"cd {os.getcwd()}\n"
        cmd = _cmd + cmd
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
        status = job_info["Status"]
        return STATUS_MAPPING.get(status, JobStatus.UNRECORGNIZED)

    def _get_job_partition(self, job_info: dict) -> AliyunPartitionEnum | str:
        if (partition := job_info["WorkspaceName"]) in AliyunPartitionEnum.__members__.keys():
            partition = AliyunPartitionEnum[partition]
        return partition

    def stats_node_cli(
        self,
        partition: Annotated[AliyunPartitionEnum, Parameter(help="分区名")],
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
        partition: Annotated[AliyunPartitionEnum | None, Parameter(name=["-p", "--partition"])] = None,
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
        self, *cmd: Annotated[str, Parameter(allow_leading_hyphen=True)], run_params: DLCRunParams | None = None
    ) -> JobSchema:
        if run_params is None:
            run_params = DLCRunParams()
        run_params.cmd = " ".join(cmd)
        return self.run(run_params)

    def stop_cli(
        self,
        *,
        job_id: Annotated[str | None, Parameter(name=["--job-id", "-j"])] = None,
        regex: Annotated[str | None, Parameter(help="匹配任务名的正则表达式")] = None,
        group: Annotated[str | None, Parameter(name=["--group", "-g"])] = None,
        user: Annotated[str | None, Parameter(name=["--user", "-u"])] = None,
        partition: Annotated[AliyunPartitionEnum | None, Parameter(name=["-p", "--partition"])] = None,
        status: Annotated[JobStatus | None, Parameter(name=["-status", "-s"])] = None,
        confirm: Annotated[bool, Parameter(name=["--confirm", "-c"])] = False,
    ) -> None:
        self.stop(
            job_id=job_id, regex=regex, group=group, user=user, partition=partition, status=status, confirm=confirm
        )

    async def async_get_log(self, job_id: str) -> str:
        logs = await async_get_log(job_id, cfg=self.dlc_cfg)
        return "\n".join(logs)

    async def async_get_job_info(self, /, job_id: str, verbose=False) -> JobSchema:
        job_info = await async_get_job(job_id, cfg=self.dlc_cfg)
        return self._get_job_info(job_id, job_info, verbose)
