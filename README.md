<div align="center">
  <img src="https://github.com/user-attachments/assets/4124cc14-9ff3-4807-a097-285d21aeefca" width="360"/>
  <br /><br />
</div>


clusterx 为不同云服务厂商的集群调度提供了统一的 CLI 和 Python API，屏蔽集群带来的差异性，让用户可以专注于任务本身，在 clusterx 支持的集群上，可以无缝迁移任务。具体来说，clusterx 为不同集群提供了统一的任务启动、查看、停止接口，用户只需要学习 clusterx 的语法，就能在不同集群之间实现“启动任务”、“查询任务”、“停止任务”等基础功能。


## Getting started

### 安装 clusterx:

```console
pip install git+https://github.com/InternLM/CLusterX.git
```

### 配置 clusterx：

- 阿里云：[docs/aliyun.md](docs/aliyun.md)
- 火山云：[docs/volc.md](docs/volc.md)

### 使用 clusterx：

```console
clusterx --help
```
```console
Usage: clusterx COMMAND

为不同集群提供统一的任务启动、查看、停止接口

╭─ Commands ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ get-job                                                                                                                                                                                                                                                                           │
│ get-node   获取节点详细信息, 在底层设置 verbose 和 out 接口是为了方便集群相关的完整信息.                                                                                                                                                                                          │
│ list                                                                                                                                                                                                                                                                              │
│ log                                                                                                                                                                                                                                                                               │
│ run                                                                                                                                                                                                                                                                               │
│ stats      统计分区的节点信息，在底层设置 verbose 和  out 接口是为了方便记录集群相关的完整信息.                                                                                                                                                                                   │
│ stop                                                                                                                                                                                                                                                                              │
│ --help -h  Display this message and exit.                                                                                                                                                                                                                                         │
│ --version  Display application version.                                                                                                                                                                                                                                           │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

1. 启动 CPU 任务:

    ```console
    clusterx run echo "hello world"
    ```

    ```console
    [15:40:55] INFO     任务 dlc11igt1jdmclzk 创建成功
            INFO     任务命令 bash ~/.tmp/85565b56-6b66-413c-91a4-d20bf9582946.sh
            INFO     {
                            "job_id": "dlc11igt1jdmclzk"
                            "user": "yehaochen"
                            "gpus_per_node": 0
                            "cpus_per_node": 4
                            "memory": "10Gi"
                            "status": "Queuing"
                            "num_nodes": 1
                            "partition": "ws142uih39u48m91"
                            "job_name": "clustex"
                            "nodes": []
                            "nodes_ip": []
                            "submit_time": null
                            "start_time": null
                            "end_time": null
                            "cmd": "bash ~/.tmp/85565b56-6b66-413c-91a4-d20bf9582946.sh"
                        }
    ```

2. 启动 GPU 任务 （以 lmdeploy 为例）

    需要额外传入 `--gpus-per-task 1`
    ```console
    clusterx run --gpus-per-task 1 lmdeploy serve api_server  <model-path> --tp 4 --chat-template qwen --log-level INFO
    ```

    多机任务则传入 `-N <节点数>`，以 `torchrun` 启动训练为例：

    ```console
    clusterx run -N 8 --gpus-per-task 8 --job-name clusterx \
    torchrun \
    --nproc-per-node=8  \
    --master_addr='${MASTER_ADDR}' \
    --master_port='${MASTER_PORT}' \
    --nnodes='${WORLD_SIZE}' \
    --node_rank='${RANK}' \
    <entrypoint> \
    ```

    上述命令会在集群中启动 8 个节点，每个节点 8 个 GPU，总共 64 个 GPU 的任务，更多参数请执行 `clusterx run --help` 查看

3. 查看任务状态：

    ```console
    clusterx get-job <job-id>
    ```

    在执行 `clusterx run` 会在终端输出任务的 `job_id`，可以通过 `clusterx get-job` 查看任务状态

4. 查看任务日志：

    ```console
    clusterx log <job-id>
    ```

5. 停止任务：

    ```console
    clusterx stop --job-id <job-id>
    ```

    如果想停止个人用户下的所有任务，可以执行：

    ```console
    clusterx stop -u <username>
    ```

    如果想停止符合正则匹配任务名的任务，可以执行：

    ```console
    clusterx stop --regex <regex>
    ```

6. 查看任务列表：

    ```console
    clusterx list -u <username>
    ```

    ```console
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ 任务名                                   ┃ 任务 ID          ┃ 任务状态  ┃ 使用 GPU ┃ 使用 CPU ┃ 使用内存 ┃ 占用节点数 ┃ 用户名    ┃ 所属分区  ┃ 创建时间             ┃ 结束时间             ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
    │ clustex                                  │ dlc1izsmpzvn594d │ Failed    │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-03T07:46:56Z │ 2025-03-03T07:47:06Z │
    │ clustex                                  │ dlc11igt1jdmclzk │ Succeeded │ 0        │ 4        │ 10GB     │ 1          │ yehaochen │ alillm_h1 │ 2025-03-03T07:41:34Z │ 2025-03-03T07:41:44Z │
    │ clustex                                  │ dlczak1zb3aqy7mv │ Succeeded │ 0        │ 4        │ 10GB     │ 1          │ yehaochen │ alillm_h1 │ 2025-03-03T07:41:30Z │ 2025-03-03T07:41:41Z │
    │ clustex                                  │ dlc15xtg5jykkgip │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:23Z │                      │
    │ clustex                                  │ dlc15nturrr95c12 │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:21Z │                      │
    │ clustex                                  │ dlc15du9dzh4utg2 │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:42Z │                      │
    │ clustex                                  │ dlc14tv2mfv7mgpo │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:42Z │                      │
    │ clustex                                  │ dlc14jvh8ngzonmu │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:21Z │                      │
    │ clustex                                  │ dlc149vvuvj79r6m │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:44Z │                      │
    │ clustex                                  │ dlc13zwah36ijz67 │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:17Z │                      │
    │ clustex                                  │ dlc13fx3pjihnbmh │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:11Z │                      │
    │ clustex                                  │ dlc135xibr2uxvko │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:23Z │                      │
    │ clustex                                  │ dlc12vxwxzl0wty0 │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:29Z │                      │
    │ clustex                                  │ dlc12byq6fk2kdd2 │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:30Z │                      │
    │ clustex                                  │ dlc121z4snnh3ox8 │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:30Z │                      │
    │ clustex                                  │ dlc11rzjevk9kd4m │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:32Z │                      │
    │ clustex                                  │ dlc11hzy1328mvkl │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:05Z │                      │
    │ clustex                                  │ dlc10y0r9j33e00e │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:23Z │                      │
    │ clustex                                  │ dlc10o15vrz74bfw │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:30Z │                      │
    │ clustex                                  │ dlc10e1khzgs3edl │ Running   │ 1        │ 12       │ 150GB    │ 1          │ yehaochen │ alillm_h1 │ 2025-03-02T12:38:30Z │                      │
    │ yehaochen_test                           │ dlc1p561vrucgct6 │ Succeeded │ 0        │ 4        │ 16GB     │ 1          │ yehaochen │ llm_ddd   │ 2024-12-04T16:50:42Z │ 2024-12-04T16:50:43Z │
    └──────────────────────────────────────────┴──────────────────┴───────────┴──────────┴──────────┴──────────┴────────────┴───────────┴───────────┴──────────────────────┴──────────────────────┘
    ```


# FAQ

1. SSL 认证失败，hostname 无法解析,代理错误

    例如:
    ```console
    File "/path/to/site-packages/urllib3/connectionpool.py", line 464, in _make_request
        self._validate_conn(conn)
    File "/path/to/site-packages/urllib3/connectionpool.py", line 1093, in _validate_conn
        conn.connect()
    File "/path/to/site-packages/urllib3/connection.py", line 741, in connect
        sock_and_verified = _ssl_wrap_socket_and_match_hostname(
    File "/path/to/site-packages/urllib3/connection.py", line 882, in _ssl_wrap_socket_and_match_hostname
        context.verify_mode = resolve_cert_reqs(cert_reqs)
    File "/path/to/ssl.py", line 738, in verify_mode
        super(SSLContext, SSLContext).verify_mode.__set__(self, value)
    ValueError: Cannot set verify_mode to CERT_NONE when check_hostname is enabled.
    ```

    或者：

    ```console
    File "/path/to/miniconda3/envs/clusterx/lib/python3.10/site-packages/requests/sessions.py", line 703, in send
    r = adapter.send(request, **kwargs)
    File "/path/to/miniconda3/envs/clusterx/lib/python3.10/site-packages/requests/adapters.py", line 694, in send
        raise ProxyError(e, request=request)
    requests.exceptions.ProxyError: HTTPConnectionPool(host='volc-proxy.pjlab.org.cn', port=13128): Max retries exceeded with url: http://open.volcengineapi.com/?Action=ListCustomTasks&Version=2021-10-01 (Caused by ProxyError('Unable to connect to proxy', ConnectionResetError(104, 'Connection reset by peer')))
    ```

    尝试关闭代理: `unset http_proxy https_proxy`
