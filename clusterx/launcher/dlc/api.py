import base64
import datetime
import hashlib
import hmac
import json
import os
import uuid
from pathlib import Path
from typing import cast

import aiohttp
import requests
import toml
from pydantic import TypeAdapter, ValidationError
from rich.console import Console
from typing_extensions import TypedDict

_AliyunUser = TypedDict("_AliyunUser", {"access_id": str, "access_key": str})


class _AliyunDLCConfig(TypedDict):
    user: _AliyunUser


class _AliyunConfig:
    def __init__(self, dlc_cfg: Path | None = None):
        if dlc_cfg is None:
            dlc_cfg = Path().home() / ".dlc/config"
        self.dlc_cfg = dlc_cfg
        adapter = TypeAdapter(_AliyunDLCConfig)

        config: _AliyunDLCConfig | dict
        if not dlc_cfg.exists():
            self._create_dlc_config(dlc_cfg)
            config = toml.load(dlc_cfg)
        else:
            config = toml.load(dlc_cfg)
            try:
                adapter.validate_python(config)
            except ValidationError:
                self._create_dlc_config(dlc_cfg)
                config = toml.load(dlc_cfg)
        config = cast(_AliyunDLCConfig, config)
        self.access_key_id = config["user"]["access_id"]
        self.access_key_secret = config["user"]["access_key"]

        endpoint = os.getenv(
            "ALI_ENDPOINT", "pai-proxy.cda8b74cb9cf64340ab0a0b889e4beec5.cn-shanghai.alicontainer.com:80"
        )
        self.endpoint = endpoint
        self.protocol = "http"

    def _create_dlc_config(self, dlc_cfg: Path):
        console = Console()
        console.print("Cannot found dlc config, create a new one:")
        access_id = console.input("access_id:")
        access_key = console.input("access_key:")
        self.dlc_cfg.parent.mkdir(exist_ok=True, parents=True)

        with open(dlc_cfg, "w") as f:
            toml.dump({"user": {"access_id": access_id, "access_key": access_key}}, f)


default_cfg = None


def do_request(
    api_product, api_method, api_path, cfg: _AliyunConfig | None = None, api_body=None, api_query=None
) -> str:
    """根据请求信息，生成认证信息，发送请求给到后端服务."""
    global default_cfg

    if api_query is None:
        api_query = {}
    if default_cfg is None:
        default_cfg = _AliyunConfig()

    if cfg is None:
        cfg = default_cfg

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    signature_nonce = str(uuid.uuid4())
    headers = {
        "x-acs-signature-method": "HMAC-SHA1",
        "date": ts,
        "x-acs-signature-nonce": signature_nonce,  # 必须
        "x-pai-product": api_product,  # 必须
        "accept": "application/json",
    }
    if api_body is not None:
        headers["content-type"] = "application/json"

    api_url = f"{cfg.protocol}://{cfg.endpoint}{api_path}"
    # 使用请求信息，生成请求使用的签名(signature)，然后生成对应认证信息，在请求头里传递给到服务(authorization)
    string_to_sign = get_string_to_sign(
        method=api_method,
        url_path=api_path,
        headers=headers,
        query=api_query,
    )
    signature = get_roasignature(string_to_sign=string_to_sign, secret=cfg.access_key_secret)
    headers["Authorization"] = f"acs {cfg.access_key_id}:{signature}"

    # TODO: Hardcode retry 10 times for ConnectionError
    resp = None
    for _ in range(10):
        try:
            resp = requests.request(
                method=api_method,
                url=api_url,
                params=api_query,
                headers=headers,
                json=api_body,
            )
        except requests.ConnectionError as e:
            print(f"Connection Error: {e}, retrying...")
        else:
            break

    if resp is None:
        raise requests.RequestException("请求失败: 重试次数过多")

    if resp.status_code != 200:
        raise requests.RequestException(f"请求失败: {resp.content}")

    # verify=False,)
    # print(resp.status_code)
    # print(resp.content)
    return resp.content.decode("utf-8")


async def async_do_request(
    api_product, api_method, api_path, cfg: _AliyunConfig | None = None, api_body=None, api_query=None
) -> str:
    """根据请求信息，生成认证信息，发送请求给到后端服务."""
    global default_cfg

    if api_query is None:
        api_query = {}
    if default_cfg is None:
        default_cfg = _AliyunConfig()

    if cfg is None:
        cfg = default_cfg

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    signature_nonce = str(uuid.uuid4())
    headers = {
        "x-acs-signature-method": "HMAC-SHA1",
        "date": ts,
        "x-acs-signature-nonce": signature_nonce,  # 必须
        "x-pai-product": api_product,  # 必须
        "accept": "application/json",
    }
    if api_body is not None:
        headers["content-type"] = "application/json"

    api_url = f"{cfg.protocol}://{cfg.endpoint}{api_path}"
    # 使用请求信息，生成请求使用的签名(signature)，然后生成对应认证信息，在请求头里传递给到服务(authorization)
    string_to_sign = get_string_to_sign(
        method=api_method,
        url_path=api_path,
        headers=headers,
        query=api_query,
    )
    signature = get_roasignature(string_to_sign=string_to_sign, secret=cfg.access_key_secret)
    headers["Authorization"] = f"acs {cfg.access_key_id}:{signature}"

    async with aiohttp.ClientSession() as session:
        async with session.request(
            method=api_method,
            url=api_url,
            params=api_query,
            headers=headers,
            json=api_body,
        ) as resp:
            if resp.status != 200:
                raise aiohttp.ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"请求失败: {await resp.text()}",
                )
            return await resp.text()


def get_jobs_list(status: str | None = None, cfg: _AliyunConfig | None = None, num: int = 20) -> list[dict]:
    if cfg is None:
        cfg = default_cfg
    # API请求的URL，以及参数
    api_path = "/api/v1/jobs"
    # 填写请求query上的参数 api_method = "GET"
    # 请求API归属产品: # PAI-DLC: dlc
    # PAIFlow: paiflow # PAI-DSW: dsw
    api_method = "GET"
    api_product = "dlc"
    query = []

    torch_query = {"JobType": "PyTorchJob", "PageSize": str(num)}

    if status is not None:
        torch_query["Status"] = status

    resp = do_request(
        api_product=api_product,
        api_query=torch_query,
        api_method=api_method,
        api_path=api_path,
        cfg=cfg,
    )
    query += json.loads(resp)["Jobs"]
    batch_query = {
        "JobType": "BatchJob",
        "PageSize": str(num),
    }

    if status is not None:
        batch_query["Status"] = status

    resp = do_request(
        api_product=api_product,
        api_query=batch_query,
        api_method=api_method,
        api_path=api_path,
        cfg=cfg,
    )
    query += json.loads(resp)["Jobs"]
    return query


# GET /api/v1/quotas/[QuotaId]
def get_quota(quota_id, workspace_id, cfg: _AliyunConfig | None = None) -> dict:
    if cfg is None:
        cfg = default_cfg
    # API请求的URL，以及参数
    api_path = f"/api/v1/quotas/{quota_id}"
    api_method = "GET"
    api_product = "dlc"
    query = {"WorkspaceId": workspace_id}
    resp = do_request(
        api_product=api_product,
        api_query=query,
        api_method=api_method,
        api_path=api_path,
        cfg=cfg,
    )
    try:
        return json.loads(resp)
    except json.JSONDecodeError:
        raise ValueError(f"获取配额 {quota_id} 失败")


# GET /api/v1/data/nodeInfos
def get_all_node_in_cluster(cfg: _AliyunConfig | None = None) -> dict:
    if cfg is None:
        cfg = default_cfg
    api_path = "/api/v1/data/nodeInfos"
    api_method = "GET"
    api_product = "dlc"
    resp = do_request(
        api_product=api_product,
        api_query="",
        api_method=api_method,
        api_path=api_path,
        cfg=cfg,
    )
    try:
        return json.loads(resp)
    except json.JSONDecodeError:
        raise ValueError("获取集群信息失败")


def get_job(job_id, cfg: _AliyunConfig | None = None) -> dict:
    if cfg is None:
        cfg = default_cfg
    api_path = "/api/v1/jobs/{0}".format(job_id)
    api_method = "GET"
    api_product = "dlc"
    resp = do_request(
        api_product=api_product,
        api_query="",
        api_method=api_method,
        api_path=api_path,
        cfg=cfg,
    )
    try:
        return json.loads(resp)
    except json.JSONDecodeError:
        raise ValueError(f"获取任务 {job_id} 失败")


async def async_get_job(job_id, cfg: _AliyunConfig | None = None) -> dict:
    if cfg is None:
        cfg = default_cfg
    api_path = "/api/v1/jobs/{0}".format(job_id)
    api_method = "GET"
    api_product = "dlc"
    resp = await async_do_request(
        api_product=api_product,
        api_query="",
        api_method=api_method,
        api_path=api_path,
        cfg=cfg,
    )
    try:
        return json.loads(resp)
    except json.JSONDecodeError:
        raise ValueError(f"获取任务 {job_id} 失败")


def stop_job(job_id, cfg: _AliyunConfig | None = None):
    if cfg is None:
        cfg = default_cfg
    api_path = "/api/v1/jobs/{0}/stop".format(job_id)
    api_method = "POST"
    api_product = "dlc"
    return do_request(
        api_product=api_product,
        api_query="",
        api_method=api_method,
        api_path=api_path,
        cfg=cfg,
    )


def create_job(
    cmd: str,
    workspace_id: str,
    job_name,
    image: str,
    podcount: int = 1,
    cpus_per_task: int = 1,
    gpus_per_task: int = 0,
    memory_per_task: int = 1,
    shared_memory: int | None = None,
    aimaster=False,
    include: str | None = None,  # TODO: Support include specific nodes
    data_sources: list[str] | None = None,
    envs: dict[str, str] | None = None,
    priority: int = 1,
    preemptible: bool | None = None,
    priority_preemptible: bool | None = None,
    cfg: _AliyunConfig | None = None,
):
    api_path = "/api/v1/jobs"
    api_method = "POST"
    if shared_memory is None:
        shared_memory = f"{memory_per_task // 8}Gi"  # HardCode here
    else:
        shared_memory = f"{shared_memory}Gi"
    api_body = {
        "DisplayName": job_name,
        "JobType": "PyTorchJob",
        "JobSpecs": [
            {
                "Type": "Worker",
                "Image": image,
                "PodCount": podcount,
                "ResourceConfig": {
                    "CPU": str(cpus_per_task),
                    "Memory": f"{memory_per_task}Gi",
                    "SharedMemory": shared_memory,
                },
            }
        ],
        "WorkspaceId": workspace_id,
        "UserCommand": cmd,
        "Priority": priority,
    }

    if gpus_per_task > 0:
        api_body["JobSpecs"][0]["ResourceConfig"]["GPU"] = str(gpus_per_task)
        api_body["JobSpecs"][0]["ResourceConfig"]["GPUType"] = "NVIDIA-L20Y"

    if envs is not None:
        api_body["Envs"] = envs

    if include:
        api_body["JobSpecs"][0]["AssignNodeSpec"] = {
            "EnableAssignNode": True,
            "NodeNames": include,
        }

    if data_sources:
        api_body["DataSources"] = [
            {
                "DataSourceId": k,
            }
            for k in data_sources
        ]

    settings = api_body.get("Settings", {})

    if aimaster:
        settings.update(
            {
                "EnableErrorMonitoringInAIMaster": True,
                "ErrorMonitoringArgs": "--job-execution-mode=Sync "
                "--enable-job-restart=True "
                "--max-num-of-job-restart=10 "
                "--job-restart-timeout=1800 "
                "--fault-tolerant-policy=OnFailure "
                "--enable-local-detection=True "
                "--enable-job-hang-detection=True "
                "--job-hang-interval=1800 ",
            }
        )

    if preemptible:  # None or False means disabling preemptible
        settings.update({"EnablePreemptibleJob": True})
    if priority_preemptible:  # None or False means disabling priority preemptible
        settings.update({"EnablePriorityPreemption": True})

    if settings:
        api_body["Settings"] = settings

    resp = do_request(
        api_product="dlc",
        api_method=api_method,
        api_path=api_path,
        api_body=api_body,
        cfg=cfg,
    )
    try:
        return json.loads(resp)
    except json.JSONDecodeError:
        raise ValueError(f"创建任务 {job_name} 失败")


def get_all_pods_logs(job_id: str, cfg: _AliyunConfig | None = None) -> list[str]:
    job_info = get_job(job_id)
    pod_idx = 0
    pods = [pod_spec for pod_spec in job_info["Pods"] if pod_spec["Type"] in ["worker", "master"]]
    logs = []
    for pod_idx, pod_spec in enumerate(pods):
        pod_id = pod_spec["PodId"]
        api_path = f"/api/v1/jobs/{job_id}/pods/{pod_id}/logs"
        api_method = "GET"
        api_product = "dlc"
        api_query = {
            "MaxLines": 1000000,
        }
        resp = do_request(
            api_product=api_product,
            api_method=api_method,
            api_path=api_path,
            api_query=api_query,
            cfg=cfg,
        )
        try:
            logs += [f"{pod_spec['PodId']} >> {log}" for log in json.loads(resp)["Logs"]]
        except (json.JSONDecodeError, KeyError):
            raise ValueError(f"获取任务 {job_id} 日志失败")
    return logs

def get_log(job_id: str, cfg: _AliyunConfig | None = None) -> list[str]:
    job_info = get_job(job_id)
    pod_idx = 0
    for pod_idx, pod_spec in enumerate(job_info["Pods"]):
        if pod_spec["Type"] == "worker":
            break
    pods = job_info["Pods"]
    if not pods:
        return []
    pod_id = pods[pod_idx]["PodId"]
    api_path = f"/api/v1/jobs/{job_id}/pods/{pod_id}/logs"
    api_method = "GET"
    api_product = "dlc"
    api_query = {
        "MaxLines": 1000000,
    }
    resp = do_request(
        api_product=api_product,
        api_method=api_method,
        api_path=api_path,
        api_query=api_query,
        cfg=cfg,
    )
    try:
        return json.loads(resp)["Logs"]
    except (json.JSONDecodeError, KeyError):
        raise ValueError(f"获取任务 {job_id} 日志失败")


async def async_get_log(job_id: str, cfg: _AliyunConfig | None = None) -> list[str]:
    job_info = await async_get_job(job_id)
    pod_idx = 0
    for pod_idx, pod_spec in enumerate(job_info["Pods"]):
        if pod_spec["Type"] in ["worker", "master"]:
            break
    pods = job_info["Pods"]
    if not pods:
        return []
    pod_id = pods[pod_idx]["PodId"]
    api_path = f"/api/v1/jobs/{job_id}/pods/{pod_id}/logs"
    api_method = "GET"
    api_product = "dlc"
    api_query = {
        "MaxLines": 1000000,
    }
    resp = await async_do_request(
        api_product=api_product,
        api_method=api_method,
        api_path=api_path,
        api_query=api_query,
        cfg=cfg,
    )
    try:
        return json.loads(resp)["Logs"]
    except (json.JSONDecodeError, KeyError):
        raise ValueError(f"获取任务 {job_id} 日志失败")


def to_string(s, encoding="utf-8"):
    if s is None:
        return s
    if isinstance(s, bytes):
        return s.decode(encoding)
    else:
        return str(s)


def get_string_to_sign(method: str, url_path: str, headers: dict, query: dict) -> str:
    """使用请求信息生成待签名的字符串."""
    accept = "" if headers.get("accept") is None else headers.get("accept")
    content_md5 = "" if headers.get("content-md5") is None else headers.get("content-md5")
    content_type = "" if headers.get("content-type") is None else headers.get("content-type")
    date = "" if headers.get("date") is None else headers.get("date")
    header = "%s\n%s\n%s\n%s\n%s\n" % (
        method,
        accept,
        content_md5,
        content_type,
        date,
    )
    canon_headers = _get_canonicalized_headers(headers)
    canon_resource = _get_canonicalized_resource(url_path, query)
    sign_str = header + canon_headers + canon_resource
    return sign_str


def _get_canonicalized_resource(pathname: str, query: dict) -> str:
    if len(query) <= 0:
        return pathname
    resource = "%s?" % pathname
    query_list = sorted(list(query))
    for key in query_list:
        if query[key] is not None:
            if query[key] == "":
                s = "%s&" % key
            else:
                s = "%s=%s&" % (key, to_string(query[key]))
            resource += s
    return resource[:-1]


def _get_canonicalized_headers(headers: dict) -> str:
    """将请求头排序后，获取“x-acs-”作为前缀按字母序排序后拼接。 注意，按RFC2616, HTTP 请求头的名称是大小写不敏感的。"""
    canon_keys = []
    for k in headers:
        if k.startswith("x-acs-"):
            canon_keys.append(k)
    canon_keys = sorted(canon_keys)
    canon_header = ""
    for k in canon_keys:
        canon_header += "%s:%s\n" % (k, headers[k])
    return canon_header


def get_roasignature(string_to_sign: str, secret: str) -> str:
    """生成签名:使用HMAC-256生成签名，然后通过base64输出签名字符串。"""
    hash_val = hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha1).digest()
    signature = base64.b64encode(hash_val).decode("utf-8")
    return signature
