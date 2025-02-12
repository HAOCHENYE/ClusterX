from clusterx.cli import app
from clusterx.launcher.base import JobSchema


def test_aliyun():
    app("run --help")
    app("stats --help")
    app("log --help")
    app("list --help")
    app("get-job --help")
    app("get-node --help")

    # 通用测试
    app("list -u yehaochen")
    app("list -p alillm_h1")
    app("list -p alillm_h1 -u yehaochen")
    app("list -p alillm_h1 -u yehaochen -s running")
    app("stats alillm_h1")

    job_schema: JobSchema = app("run sleep 1000 --gpus-per-task 0")
    assert isinstance(job_schema, JobSchema)
    while job_schema.status != "Running":
        job_schema = app(f"get-job {job_schema.job_id}")

    nodes_name = job_schema.nodes
    assert isinstance(nodes_name, list)
    node_name = nodes_name[0]
    app(f"get-node {node_name}")
    app(f"stop --job-id {job_schema.job_id}")
