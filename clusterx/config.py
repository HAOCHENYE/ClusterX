import os
from pathlib import Path

import yaml
from cyclopts import Parameter
from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from pydantic import BaseModel, ValidationError, model_validator
from typing_extensions import Annotated, NotRequired, TypedDict

from clusterx.constant import aliyun_partition_ids

VALID_CUSTER = ("aliyun", "volc")
from clusterx.constant import aliyun_partition_ids

# TODO: Need to implement ClusterX PydanticModel Config to implement the validation


class AliyunConfig(TypedDict):
    image: str
    data_sources: list[str]
    partition: str
    config: NotRequired[str]
    tmpdir: str
    custom_partition: NotRequired[list[tuple[str, str]]]
    custom_quota: NotRequired[list[tuple[str, str]]]


class VolcConfig(TypedDict):
    config: NotRequired[str]
    tmpdir: str
    custom_partition: NotRequired[list[tuple[str, str]]]
    ...


class ClusterXConfig(BaseModel):
    default: str
    aliyun: AliyunConfig | None = None
    volc: VolcConfig | None = None

    @model_validator(mode="before")
    def validate_input(cls, data):
        assert data["default"] in VALID_CUSTER, f"Invalid cluster name: {data['default']}"
        if (aliyun_cfg := (data.get("aliyun") or {})) and "custom_quota" in aliyun_cfg:
            aliyun_quota = aliyun_cfg["quota"]
            aliyun_partition = aliyun_cfg["partition"]
            assert len(aliyun_quota) == len(aliyun_partition), "分区和配额数量不匹配"

        return data


DEFAULT_CFG_PATH = Path().home() / ".config/clusterx.yaml"
CLUSTER: str
CLUSTERX_CONFIG: ClusterXConfig

def _set_aliyun_config():
    # prompt inside cyclopts need to add `\n` manually sometimes. ohtherwise it will be a mess
    partition_mappings = dict(aliyun_partition_ids)
    partition_names = list(partition_mappings.keys())

    partition = prompt(f"配置默认阿里云分区 {partition_names} >>> ", completer=WordCompleter(partition_names))
    workspace_id = "无默认值"

    custom_partition = None
    if partition:
        if partition not in partition_names:
            print(f"\033[32m自定义的分区名，请输入分区的 workspace id. 如果需要配置多个自定义分区，请手动编辑 {DEFAULT_CFG_PATH}\033[0m")
            workspace_id = prompt(">>>")
            custom_partition = [(partition, workspace_id)]

    print(f"配置默认阿里云镜像")
    image = prompt(">>>")

    print(f"以逗号为分隔符，输入 datasources id")
    data_sources_str = prompt(">>> ")
    data_sources = [item.strip() for item in data_sources_str.split(",") if item.strip()]

    config = os.environ.get("CLUSTERX_ALIYUN_CONFIG", str(Path().home() / ".dlc/config"))
    print(f"请输入阿里云配置文件，默认使用 {config}")
    config_path = prompt(">>> ")

    default_tmpdir = os.environ.get("CLUSTERX_ALIYUN_TMPDIR", str(Path().home() / ".tmp"))
    print(f"请输入任务节点可访问公共目录，回车默认使用 {default_tmpdir}\n")
    tmpdir = prompt(">>> ")

    # TODO: 设置挂载目录
    cfg = {
        "image": image,
        "partition": partition,
        "data_sources": data_sources,
        "config": config_path or config,
        "tmpdir": tmpdir or default_tmpdir,
    }
    if custom_partition is not None:
        cfg["custom_partition"] = custom_partition
    return cfg


def _setup_volc():
    config = os.environ.get("CLUSTERX_VOLC_CONFIG", str(Path().home() / ".volc/config.yaml"))
    print(f"请输入火山云配置文件，默认使用 {config}")
    config_path = prompt(">>> ")

    default_tmpdir = os.environ.get("CLUSTERX_VOLC_TMPDIR", str(Path().home() / ".tmp"))
    print(f"请输入任务节点可访问公共目录，回车默认使用 {default_tmpdir}")
    tmpdir = prompt(">>> ")

    # TODO: 设置挂载目录
    return {"config": config_path or config, "tmpdir": tmpdir or default_tmpdir}


def configure(
    *,
    reset: Annotated[bool, Parameter(help="是否重新配置 ", negative="")] = False,
    show: Annotated[bool, Parameter(help="查看配置文件路径", negative="")] = False,
):
    if reset:
        if DEFAULT_CFG_PATH.exists():
            print(f"发现配置文件路径: {DEFAULT_CFG_PATH}")
            while True:
                confirm = prompt("是否删除已有配置文件，重新配置？Y/N\n>>>")
                if confirm.lower() == "y":
                    os.remove(DEFAULT_CFG_PATH)
                    _configure()
                    return
                elif confirm.lower() == "n":
                    return
                else:
                    print("请输入 Y/N")
                    continue
        return _configure()

    if show:
        if DEFAULT_CFG_PATH.exists():
            with open(DEFAULT_CFG_PATH, "r") as f:
                print("=" * 100)
                print(f"配置文件路径: {DEFAULT_CFG_PATH}")
                print("=" * 100)
                print(f"配置文件内容: {f.read()}")
        else:
            print(f"配置文件路径: {DEFAULT_CFG_PATH} 不存在")
        return


def _configure():
    if not DEFAULT_CFG_PATH.exists():
        DEFAULT_CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
        default = prompt(f"输入集群名 ({VALID_CUSTER}) >>> ", completer=WordCompleter(VALID_CUSTER))
        assert default in VALID_CUSTER, f"Invalid cluster name: {default}"
        default_cfg = {
            "default": default,
        }

        if default == "aliyun":
            default_cfg["aliyun"] = _set_aliyun_config()
        elif default == "volc":
            default_cfg["volc"] = _setup_volc()

        clusterx_config = ClusterXConfig(**default_cfg)
        with open(DEFAULT_CFG_PATH, "w") as f:
            yaml.dump(clusterx_config.model_dump(), f, Dumper=yaml.Dumper)
            print(f"配置文件保存至 {DEFAULT_CFG_PATH}")
    else:
        with open(DEFAULT_CFG_PATH, "r") as f:
            try:
                _cfg = yaml.load(f, Loader=yaml.Loader) or {}
                clusterx_config = ClusterXConfig(**_cfg)
            except ValidationError as e:
                raise RuntimeError(f"Invalid config file: {DEFAULT_CFG_PATH} for {e}, please remove it and retry")

    return clusterx_config


if not DEFAULT_CFG_PATH.exists():
    CLUSTERX_CONFIG = _configure()
else:
    with open(DEFAULT_CFG_PATH, "r") as f:
        try:
            _cfg = yaml.load(f, Loader=yaml.Loader) or {}
        except ValidationError as e:
            raise RuntimeError(f"Invalid config file: {DEFAULT_CFG_PATH} for {e}, please remove it and retry")
        else:
            CLUSTERX_CONFIG = ClusterXConfig(**_cfg)


# TODO: Default parameter for each cluster
CLUSTER = CLUSTERX_CONFIG.default
