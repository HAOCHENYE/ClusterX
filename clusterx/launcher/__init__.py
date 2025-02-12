# mypy: disable-error-code="misc,assignment"
import traceback

from typing_extensions import Type, TypedDict

from clusterx.utils import StrEnum

from .base import BaseCluster, BaseRunParams
from .dlc import AliyunCluster, AliyunPartitionEnum, DLCRunParams

try:
    from .volc import VolcCluster, VolcRunParams
except KeyboardInterrupt as e:
    raise e
except Exception:
    volc_tb = traceback.format_exc()
    VolcCluster = None
    VolcRunParams = None
else:
    volc_tb = None


ClusterSpec = TypedDict(
    "ClusterSpec",
    {
        "type": Type[BaseCluster] | None,
        "params": Type[BaseRunParams] | None,
        "partition": Type[StrEnum] | None,
        "traceback": str | None,
    },
)

CLUSTER_MAPPING = {
    "aliyun": ClusterSpec(type=AliyunCluster, params=DLCRunParams, partition=AliyunPartitionEnum, traceback=None),
    "volc": ClusterSpec(type=VolcCluster, params=VolcRunParams, partition=AliyunPartitionEnum, traceback=volc_tb),
}
