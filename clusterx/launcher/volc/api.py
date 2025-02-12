import configparser
import getpass
import json
import logging
import sys
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, TypedDict, cast

import aiohttp
import yaml
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from typing_extensions import NotRequired
from volcengine import Credentials
from volcengine.ApiInfo import ApiInfo
from volcengine.auth.SignerV4 import SignerV4
from volcengine.base.Service import Service
from volcengine.ServiceInfo import ServiceInfo
from volcengine.tls.TLSService import SearchLogsResponse, TLSService, HEADER_API_VERSION, API_VERSION_V_0_2_0, SEARCH_LOGS
from volcengine.tls.tls_requests import *

from .volc_info import VolcPartition

API_INFOS = {
    "CreateCustomTask": ApiInfo(
        "POST",
        "/",
        {
            "Action": "CreateCustomTask",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
    "ListResourceQueues": ApiInfo(
        "POST",
        "/",
        {
            "Action": "ListResourceQueues",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
    "GetCustomTask": ApiInfo(
        "POST",
        "/",
        {
            "Action": "GetCustomTask",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
    "ListCustomTasks": ApiInfo(
        "POST",
        "/",
        {
            "Action": "ListCustomTasks",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
    "ListResourceGroups": ApiInfo(
        "POST",
        "/",
        {
            "Action": "ListResourceGroups",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
    "GetContainerLogs": ApiInfo(
        "POST",
        "/",
        {
            "Action": "GetContainerLogs",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
    "GetCustomTaskInstances": ApiInfo(
        "POST",
        "/",
        {
            "Action": "GetCustomTaskInstances",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
    "StopCustomTask": ApiInfo(
        "POST",
        "/",
        {
            "Action": "StopCustomTask",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
    "GetFlavor": ApiInfo(
        "POST",
        "/",
        {
            "Action": "GetFlavor",
            "Version": "2021-10-01",
        },
        {},
        {},
    ),
}


username = getpass.getuser()
default_volc_cfg = {
    "DelayExitTimeSeconds": 60,
    "Description": "volc 任务",
    "Framework": "PyTorchDDP",
    "ImageUrl": "vemlp-cn-shanghai.cr.volces.com/preset-images/pytorch:1.13.1",
    "ResourceQueueName": None,
    "Preemptible": False,
    "Storages": [
        {"MountPath": "/fs-computility/llm/shared/", "ReadOnly": False, "SubPath": "./llm/shared/", "Type": "Vepfs"},
        {
            "MountPath": f"/fs-computility/llm/{username}/",
            "ReadOnly": False,
            "SubPath": f"./llm/{username}",
            "Type": "Vepfs",
        },
    ],
}


class Client(Service):
    def __init__(self, ak, sk):
        credentials = Credentials.Credentials(
            ak=ak,
            sk=sk,
            service="ml_platform",
            region="cn-beijing",
        )
        self.service_info = ServiceInfo(
            "open.volcengineapi.com",
            {"Accept": "application/json"},
            credentials,
            60,
            60,
            "http",
        )
        self.api_info = API_INFOS
        super().__init__(self.service_info, self.api_info)

    async def async_json(self, action: str, params: dict, body: str) -> str:
        api_info = self.api_info[action]
        r = self.prepare_request(api_info, params)
        r.headers["Content-Type"] = "application/json"
        r.body = body
        SignerV4.sign(r, self.service_info.credentials)

        url = r.build()
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method=r.method,
                url=url,
                headers=r.headers,
                data=r.body,
            ) as resp:
                if resp.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message=f"请求失败: {await resp.text()}",
                    )
                return await resp.text()



class TLSClient(TLSService):
    def __init__(self, *args, **kwargs):
        from volcengine.tls.util import get_logger
        self.__logger = get_logger("tls-python-sdk-logger")
        self.__logger.setLevel(logging.ERROR)
        super().__init__(*args, **kwargs)

    async def async_search_logs_v2(self, search_logs_request: SearchLogsRequest) -> dict:
        if search_logs_request.check_validation() is False:
            raise TLSException(error_code="InvalidArgument", error_message="Invalid request, please check it")
        headers = {HEADER_API_VERSION: API_VERSION_V_0_2_0}
        api = SEARCH_LOGS
        body = search_logs_request.get_api_input()
        request_headers=headers

        if request_headers is None:
            request_headers = {HEADER_API_VERSION: self.__api_version}
        elif HEADER_API_VERSION not in request_headers:
            request_headers[HEADER_API_VERSION] = self.__api_version
        if CONTENT_TYPE not in request_headers:
            request_headers[CONTENT_TYPE] = APPLICATION_JSON
        request = self._TLSService__prepare_request(api, None, body, request_headers)

        method = self.api_info[api].method
        url = request.build()

        async with aiohttp.ClientSession() as session:
            async with session.request(
                method=method,
                url=url,
                headers=request.headers,
                data=request.body,
            ) as resp:
                if resp.status != 200:
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message=f"请求失败: {await resp.text()}",
                    )
                return await resp.json()


@lru_cache(maxsize=128)
def get_client(ak, sk):
    return Client(ak, sk)


class StorageConfigDict(TypedDict):
    MountPath: str
    ReadOnly: bool
    SubPath: str
    Type: str


# volc config could contain more fields for volc cli, but `clusterx` will only use a subset of them
class VolcConfigDict(TypedDict):
    DelayExitTimeSeconds: NotRequired[str]  # int str
    Description: NotRequired[str]
    ImageUrl: NotRequired[str]
    ResourceQueueName: str
    Preemptible: NotRequired[bool]
    Storages: list[StorageConfigDict]
    RetryOptions: NotRequired[dict[str, str]]
    access_key_id: str
    secret_access_key: str


class VolcConfig:
    def __init__(self, volc_cfg: Path | None = None) -> None:
        if volc_cfg is None:
            volc_cfg_dir = Path.home() / ".volc"
            volc_cfg = volc_cfg_dir / "config.yaml"
        else:
            volc_cfg_dir = Path(volc_cfg).parent

        if not volc_cfg.exists():
            volc_cfg.parent.mkdir(parents=True, exist_ok=True)
            print(f"没有发现可用的 volc cfg {volc_cfg}, 自动 dump 默认的 volc config")
            image_url = "https://console.volcengine.com/ml-platform/region:ml-platform+cn-beijing/resourceQueue?Category=General&current=1&fields=%7B%22Role%22%3A%22General%22%7D&pageSize=10"  # noqa:E501
            link = "\033]8;;{link}\033\\{link}\033]8;;\033\\".format(link=image_url)
            print(f"请输入队列 ID, 可以在 {link}")
            queue_name = prompt(">>>")
            default_volc_cfg["ResourceQueueName"] = queue_name

            with volc_cfg.open("w") as f:
                yaml.dump(default_volc_cfg, f)

            confirm = None
            while confirm not in ["y", "n"]:
                print(
                    f"配置文件保存至\033[31m {volc_cfg} \033[0m，请确认配置文件是否符合预期，尤其关注 \033[31mStorage\033[0m 字段，请确保挂载路径是真实存在且可访问的！"  # noqa: E501
                )
                confirm = prompt("输入 y 继续，n 退出配置 (y/n): ", completer=WordCompleter({"y", "n"}))  # noqa:E501
                if confirm == "n":
                    volc_cfg.unlink()
                    sys.exit(-1)
                elif confirm == "y":
                    break

        with open(volc_cfg) as f:
            self.cfg = yaml.safe_load(f)

        if not (volc_cfg_dir / "credentials").exists():
            credentials_cfg = configparser.ConfigParser()
            credentials_cfg["default"] = {}
            print("Credentials not found, you can find your aksk in https://console.volcengine.com/iam/keymanage/")
            credentials_cfg["default"]["access_key_id"] = prompt("Please input your access key id: ")
            credentials_cfg["default"]["secret_access_key"] = prompt("Please input your secret_access_key id: ")
            with open(volc_cfg_dir / "credentials", "w") as f:
                credentials_cfg.write(f)

        credentials_cfg = configparser.ConfigParser()
        credentials_cfg.read(volc_cfg_dir / "credentials")

        storages = self.cfg["Storages"]
        self.cfg["access_key_id"] = credentials_cfg["default"]["access_key_id"]
        self.cfg["secret_access_key"] = credentials_cfg["default"]["secret_access_key"]

        if not storages:
            print("Please set storages in vold config first!")
            sys.exit(-1)

        self.cfg = cast(VolcConfigDict, self.cfg)


def _get_cfg(cfg: VolcConfig | None = None) -> VolcConfig:
    global default_cfg
    if default_cfg is None:
        default_cfg = cast(VolcConfig, VolcConfig())

    return cfg or default_cfg  # type: ignore


default_cfg: None | VolcConfig = None


flavor_mapping = {
    "ml.g3i.24xlarge": 0,
    "ml.pni2l.3xlarge": 1,
    "ml.pni2l.7xlarge": 2,
    "ml.pni2l.14xlarge": 4,
    "ml.hpcpni2l.28xlarge": 8,
}


# 靠后的 flavor 是靠前 flavor 的能力子集，例如如果一个队列具备 hpcpni3l.48xlarge flavor，那么 hpcpni3l.48xlarge
# 之后的所有 flavor 都可以被使用
gpu_flavor_subsets = [
    ["ml.hpcpni3l.48xlarge", "ml.pni3l.48xlarge", "ml.pni3l.24xlarge", "ml.pni3l.12xlarge", "ml.pni3l.6xlarge"],
    ["ml.hpcpni2l.28xlarge", "ml.pni2l.14xlarge", "ml.pni2l.7xlarge", "ml.pni2l.3xlarge"],
]

cpu_flavor_subsets = [
    "ml.g3i.48xlarge",
    "ml.g3i.24xlarge",
    "ml.g2a.28xlarge",
    "ml.g2a.16xlarge",
    "ml.g2a.8xlarge",
]


def _get_volc_flavor(
    available_flavors: list[str], gpu_num: int, cpu_num: int = 0, memory: int = 0, cfg: VolcConfig | None = None
) -> str:
    if gpu_num > 0:
        # 从 gpu_flavor_subsets 里获取与当前队列匹配的 flavor 子集列表

        # available_flavors: ['hpcpni3l.48xlarge', 'ml.hpcpni2l.28xlarge']
        # 此时可用的规格实际上是 [
        #      ["hpcpni3l.48xlarge", "pni3l.48xlarge", "pni3l.24xlarge", "pni3l.12xlarge", "pni3l.6xlarge"],
        #      ["ml.hpcpni2l.28xlarge", "ml.pni2l.14xlarge", "ml.pni2l.7xlarge", "ml.pni2l.3xlarge"],
        # ]
        # 低于 `hpcpni3l.48xlarge` 和 `ml.hpcpni2l.28xlarge` 的规格都可以被使用
        matched_subsets = []
        for subset in gpu_flavor_subsets:
            for _flavor in subset:
                if _flavor in available_flavors:
                    matched_subsets.append(subset)
                    break

        if not matched_subsets:
            raise RuntimeError(f"无法识别的规格 {available_flavors}，请连续 clusterx 管理员支持")

        max_matched_index = -1
        new_available_flavors = []

        # 找到子规格
        for matched_subset in matched_subsets:
            for idx, _flavor in enumerate(matched_subset):
                for ava_flavor in available_flavors:
                    if _flavor == ava_flavor:
                        max_matched_index = max(max_matched_index, idx)
            new_available_flavors.extend(matched_subset[max_matched_index:])
        available_flavors = new_available_flavors
    else:
        available_flavors = cpu_flavor_subsets

    flavor_info_list = [get_flavor_info(cfg, flavor) for flavor in available_flavors]
    valid_gpu_num = set(flavor_info["GPUNum"] for flavor_info in flavor_info_list)

    if gpu_num not in valid_gpu_num:
        raise ValueError(f"Cannot find flavor with {gpu_num} GPUs, please set `gpu_num` to one of {valid_gpu_num}.")

    valid_gpu_flavors = [flavor for flavor in flavor_info_list if flavor["GPUNum"] == gpu_num]

    def find_nearest_dist(flavor_list: List[Dict], target_num: int, fields: str) -> List[Dict]:
        min_dist = float("inf")
        for flavor in flavor_list:
            dist = abs(flavor[fields] - target_num)
            if dist <= min_dist:
                min_dist = dist
        ret = [flavor for flavor in flavor_list if abs(flavor[fields] - target_num) == min_dist]
        return ret

    if cpu_num == 0:
        nearest_flavors = valid_gpu_flavors
    else:
        nearest_flavors = find_nearest_dist(valid_gpu_flavors, cpu_num, "vCPU")

    if memory != 0:
        nearest_flavors = find_nearest_dist(valid_gpu_flavors, cpu_num, "Memory")

    flavor: Dict
    if len(nearest_flavors) > 1:
        # 有多个匹配规格的情况下，用最贵的
        flavor = max(nearest_flavors, key=lambda x: x["Price"])
    else:
        flavor = nearest_flavors[0]

    if cpu_num != 0 and flavor["vCPU"] != cpu_num:
        print(f"\033[31mFailed to find flavor with {cpu_num} CPUs, using `{flavor['Id']}` {flavor['vCPU']} CPUs instead.\033[0m")  # noqa: E501

    if memory != 0 and flavor["Memory"] != memory:
        print(f"\033[31mFailed to find flavor with {memory} Memory, using `{flavor['Id']}` {flavor['Memory']} GB Memory instead.\033[0m")  # noqa: E501
    return flavor["Id"]


def get_flavors_by_queue(cfg: VolcConfig, workspace_id: str) -> List[str]:
    queue_infos = get_queue_infos(cfg)[workspace_id]
    return [i["Id"] for i in queue_infos["FlavorResources"]]


def create_job(
    cmd: str,
    workspace_id: str,
    job_name,
    image: str | None = None,
    podcount: int = 1,
    gpus_per_task: int = 0,
    cpus_per_task: int = 0,
    memory_per_task: int = 0,
    retry=False,
    envs: list[dict] | None = None,
    cfg: VolcConfig | None = None,
    priority: int = 2,
    preemptible: bool | None = None,
):
    cfg = _get_cfg(cfg)

    client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])

    if image is None and "ImageUrl" not in cfg.cfg:
        raise ValueError("Please set `ImageUrl` in volc config or pass image explicitly!")

    default_retry_option = {"EnableRetry": True, "MaxRetryTimes": 5, "IntervalSeconds": 120, "PolicySets": ["Failed"]}
    data = {
        "Name": job_name,
        "ImageSpec": {
            "Id": image or cfg.cfg.get("ImageUrl"),
        },
        "EntrypointPath": cmd,
        "Framework": "PyTorchDDP",
        "DelayExitTimeSeconds": int(cfg.cfg.get("DelayExitTimeSeconds", 10)),
        "TaskRoleSpecs": [
            {
                "RoleName": "worker",
                "RoleReplicas": podcount,
                "ResourceSpecId": "",
                "ResourceSpec": {
                    # "FlavorID": "ml.pni2l",
                    "FlavorID": _get_volc_flavor(
                        get_flavors_by_queue(cfg, workspace_id), gpus_per_task, cpus_per_task, memory_per_task, cfg=cfg
                    ),
                },
            }
        ],
        "Envs": envs or [],
        "ResourceQueueId": workspace_id,
        "Priority": priority,
        "Storages": cfg.cfg["Storages"],
    }

    if preemptible is not None:
        data["Preemptible"] = preemptible

    if retry:
        data["RetryOptions"] = cfg.cfg.get("RetryOptions", default_retry_option)

    resp = client.json("CreateCustomTask", params={}, body=json.dumps(data))
    return json.loads(resp)["Result"]


def get_job(job_id: str, cfg: VolcConfig | None = None) -> dict:
    cfg = _get_cfg(cfg)

    client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])

    body = {"Id": job_id}
    job_info = client.json("GetCustomTask", params={}, body=json.dumps(body))

    body = {"CustomTaskId": job_id}
    container_info = client.json("GetCustomTaskInstances", params={}, body=json.dumps(body))

    return {
        "job_info": json.loads(job_info)["Result"],
        "container_info": json.loads(container_info)["Result"]["List"],
    }


def get_jobs_list(
    workspace_name: str | None = None,
    user: int | None = None,
    num: int = 100,
    cfg: VolcConfig | None = None,
) -> list[dict]:
    cfg = _get_cfg(cfg)

    # if user is None:
    #     user_ids = []
    # else:
    #     user_ids = [user]

    client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])
    if workspace_name is not None:
        workspace_id = VolcPartition[workspace_name].value
    else:
        workspace_id = VolcPartition[cfg.cfg["ResourceQueueName"]].value
    body = {
        "ResourceQueueIds": [
            workspace_id,
        ],
        "Limit": num,
        # TODO: 火山云传 user id 没法返回具体用户的任务列表
        # "CreatorUserIds": user_ids,
    }
    resp = client.json("ListCustomTasks", params={}, body=json.dumps(body))
    return json.loads(resp)["Result"]["List"]


def _get_resources_list(cfg: VolcConfig | None = None):
    # TODO: 火山云暂时不知道如何获取全量的节点信息
    # raise NotImplementedError("火山云暂时不知道如何获取全量的节点信息")
    cfg = _get_cfg(cfg)
    #
    client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])
    body: Dict = {}
    resp = client.json("ListResourceGroups", params={}, body=json.dumps(body))
    data = json.loads(resp)["Result"]
    return data


def get_flavor_info(cfg: VolcConfig | None, flavor: str):
    cfg = _get_cfg(cfg)
    client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])
    resp = client.json("GetFlavor", params={}, body=json.dumps({"Id": flavor}))
    queue_list = json.loads(resp)["Result"]
    return queue_list


def get_queue_infos(cfg: VolcConfig | None = None) -> Dict[str, Dict]:
    # TODO: 火山云暂时不知道如何获取全量的节点信息
    # raise NotImplementedError("火山云暂时不知道如何获取全量的节点信息")
    cfg = _get_cfg(cfg)

    client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])
    resp = client.json("ListResourceQueues", params={}, body="{}")
    queue_list = json.loads(resp)["Result"]["List"]
    ret = {q["Id"]: q for q in queue_list}
    return ret


async def async_get_job(job_id: str, cfg: VolcConfig | None = None) -> dict:
    cfg = _get_cfg(cfg)
    client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])

    container_info = await client.async_json(
        "GetCustomTaskInstances", params={}, body=json.dumps({"CustomTaskId": job_id})
    )
    job_info = await client.async_json("GetCustomTask", params={}, body=json.dumps({"Id": job_id}))
    resp = {
        "job_info": json.loads(job_info)["Result"],
        "container_info": json.loads(container_info)["Result"]["List"],
    }
    return resp


async def async_get_log(job_id: str, worker_idx: int | None = None, cfg: VolcConfig | None = None) -> str:
    cfg = _get_cfg(cfg)
    ak = cfg.cfg["access_key_id"]
    sk = cfg.cfg["secret_access_key"]
    tls_client = TLSClient("tls-cn-beijing.ivolces.com", ak, sk, "cn-beijing")
    num = 100
    if worker_idx is None:
        query = f"__tag__mlp_customtask__:{job_id}"
    else:
        query = f"__tag__mlp_customtask__:{job_id} AND __tag__role_index__:{worker_idx}"
    resp = await tls_client.async_search_logs_v2(
        SearchLogsRequest(
            "e80185a8-d693-46f2-a33b-b627a62650dc",
            # 筛选 job-id + pod id
            query=query,
            start_time=1346457600000,
            end_time=int(time.time() * 1000),
            limit=num
        )
    )
    return "\n".join([i["__content__"] for i in resp["Logs"][::-1]])



def stop_job(job_id: str, cfg: VolcConfig | None = None):
    cfg = _get_cfg(cfg)
    client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])
    body = {"Id": job_id}
    resp = client.json("StopCustomTask", params={}, body=json.dumps(body))
    return json.loads(resp)["Result"]


def get_log(job_id: str, worker_idx: int | None = None, cfg: VolcConfig | None = None) -> list[str]:
    cfg = _get_cfg(cfg)
    ak = cfg.cfg["access_key_id"]
    sk = cfg.cfg["secret_access_key"]
    tls_client = TLSClient("tls-cn-beijing.ivolces.com", ak, sk, "cn-beijing")
    num = 100
    if worker_idx is None:
        query = f"__tag__mlp_customtask__:{job_id}"
    else:
        query = f"__tag__mlp_customtask__:{job_id} AND __tag__role_index__:{worker_idx}"
    resp = tls_client.search_logs_v2(
        SearchLogsRequest(
            "e80185a8-d693-46f2-a33b-b627a62650dc",
            # 筛选 job-id + pod id
            query=query,
            start_time=1346457600000,
            end_time=int(time.time() * 1000),
            limit=num
        )
    )
    logs_info = resp.search_result.get_logs()
    return [i["__content__"] for i in logs_info[::-1]]


if __name__ == "__main__":
    # resp = create_job("ls /fs-computility/llm", "q-20241107090149-fqbbn", "my-task-name")
    # time.sleep(10)
    # resp = get_job(resp["Id"])
    # __import__('ipdb').set_trace()
    # print(resp)
    # print(get_jobs_list("q-20241107090149-fqbbn"))
    # resources_list = get_resources_list()
    # print(resources_list)
    # import asyncio
    # resp = asyncio.run(async_get_job("t-20250219155430-lqndt"))
    # resp = get_log("t-20250219155430-lqndt")
    # print(resp)
    # get_resources_list()
    # get_queue_infos()
    # get_flavor_info(None, "ml.hpcg1ve.21xlarge")
    # cfg = _get_cfg()
    # client = get_client(cfg.cfg["access_key_id"], cfg.cfg["secret_access_key"])
    # body = {}
    # resp = json.loads(client.json("ListQue", params={}, body=json.dumps(body)))
    # body = {
    #     "AggregationType": "QueueView",
    #     "ModuleName": "resourcequeue",
    #     "Name": "q-20241107090149-fqbbn",
    #     "MetricsKey": [
    #         "GpuRateMetrics"
    #     ],
    #     "Gpus": [
    #         {
    #             "Name": "NVIDIA-A800-SXM4"
    #         }
    #     ],
    #     "WorkloadType": [
    #         "DevInstance",
    #         "CustomTask",
    #         "Inference"
    #     ]
    # }
    # resp = json.loads(client.json("GetMetrics", params={}, body=json.dumps(body)))
    # result = get_queue_infos()
    # print(result['q-20241107090149-fqbbn']['QuotaCapability']['GPUResources']['NVIDIA-A800-SXM4-80GB'])
    # print(result['q-20241107090149-fqbbn']['QuotaCapability']['VCPU'])
    # print(result['q-20241107090149-fqbbn']['QuotaCapability']['Memory'])
    #
    # print(result['q-20241107090149-fqbbn']['QuotaAllocated']['GPUResources']['NVIDIA-A800-SXM4-80GB'])
    # print(result['q-20241107090149-fqbbn']['QuotaAllocated']["Memory"])
    # print(result['q-20241107090149-fqbbn']['QuotaAllocated']["VCPU"])
    # __import__('ipdb').set_trace()
    get_log("t-20250524161803-2bnxz")
