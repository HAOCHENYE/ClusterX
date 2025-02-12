from clusterx.utils import StrEnum
from clusterx.config import CLUSTERX_CONFIG
from clusterx.constant import volc_partition_ids


if (volc_cfg := CLUSTERX_CONFIG.volc) is not None:
    custom_partition: list[tuple[str, str]] = volc_cfg.get("custom_partition") or []

    builtin_partition = tuple(i[0] for i in volc_partition_ids)
    for partition in custom_partition:
        if partition in builtin_partition:
            continue
        volc_partition_ids.append(partition)


VolcPartition = StrEnum("VolcPartition", volc_partition_ids)
