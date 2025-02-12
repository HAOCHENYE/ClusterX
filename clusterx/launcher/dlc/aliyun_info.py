from clusterx.utils.types_helper import StrEnum
from clusterx.config import CLUSTERX_CONFIG
from clusterx.constant import aliyun_partition_ids, aliyun_partition_quota


if CLUSTERX_CONFIG.aliyun is not None:
    custom_partition: list[tuple[str, str]] = CLUSTERX_CONFIG.aliyun.get("custom_partition") or []
    custom_quota: list[tuple[str, str]] = CLUSTERX_CONFIG.aliyun.get("custom_quota", [])

    if not custom_quota:
        custom_quota = [(i[0], "unbinded") for i in custom_partition]

    builtin_partition = tuple(i[0] for i in aliyun_partition_ids)
    for partition, quota in zip(custom_partition, custom_quota):
        if partition in builtin_partition:
            continue
        aliyun_partition_ids.append(partition)
        aliyun_partition_quota.append(quota)


QuotaEnum = StrEnum("QuotaEnum", aliyun_partition_quota)
AliyunPartitionEnum = StrEnum("AliyunPartitionEnum", aliyun_partition_ids)
