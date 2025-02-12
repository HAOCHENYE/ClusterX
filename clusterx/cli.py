from cyclopts import App

from clusterx import CLUSTER, CLUSTER_MAPPING

from .config import configure

ClusterSpec = CLUSTER_MAPPING[CLUSTER]
ClusterCls, ClusterParam = ClusterSpec["type"], ClusterSpec["params"]

if ClusterCls is None:
    tb = ClusterSpec["traceback"]
    raise RuntimeError(f"Failed to initialize {CLUSTER} for {tb}")
cluster = ClusterCls()


app = App(
    name="clusterx",
    help="为不同集群提供统一的任务启动、查看、停止接口",
)


app.command(name="get-job", obj=cluster.get_job_info_cli)
app.command(name="get-node", obj=cluster.get_node_info_cli)
app.command(name="stats", obj=cluster.stats_node_cli)
app.command(name="list", obj=cluster.list_cli)
app.command(name="run", obj=cluster.run_cli)
app.command(name="log", obj=cluster.get_log_cli)
app.command(name="stop", obj=cluster.stop_cli)
app.command(name="config", obj=configure)


if __name__ == "__main__":
    app()
