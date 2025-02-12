# 火山云配置 clusterx

首先打开 `clusterx/constant.py`，填写集群相关的信息：

```python
aliyun_partition_quota = [
    ("分区名", "分区 quota id"),
]


aliyun_partition_ids = [
  ("分区名", "分区 id")
]
```

### 配置 clusterx

## 方法一：交互式填写配置文件

1. 执行 clusterx 命令，触发自动配置，选择阿里云作为集群名:

   ```console
   clusterx --help

   输入集群名 (['aliyun', 'volc']) >>> [火山云填写 volc，阿里云填写 aliyun]
   ```

2. 配置容器内可访问的公共目录

   ```console
   配置任务节点可访问公共目录
   ```


3. 填写火山云默认配置

   默认会找 `~/.volc/config.yaml` 和 `~/.volc/credentials`，如果这两个文件都不存在，则会交互式的创建这 2 个文件

   ```console
   没有发现可用的 volc cfg ~/.volc/config.yaml, 自动 dump 默认的 volc config
   请输入队列 ID   ```

   考虑到火山云的配置项比较多，这边只介绍自动生成的默认配置：`~/.volc/config.yaml`:

   ```yaml
       DelayExitTimeSeconds: 60
       Description: "volc \u4EFB\u52A1"
       Framework: PyTorchDDP
       ImageUrl: vemlp-cn-shanghai.cr.volces.com/preset-images/pytorch:1.13.1
       Preemptible: false
       ResourceQueueName: llmit
       Storages:
       - MountPath: /fs-computility/llm/shared/
         ReadOnly: false
         SubPath: ./llm/shared/
         Type: Vepfs
       - MountPath: /fs-computility/llm/${USER}/
         ReadOnly: false
         SubPath: ./llm/yehaochen
         Type: Vepfs
   ```

   - DelayExitTimeSeconds: 自定义任务提交后，有多少秒的保留时间
   - Description: 默认的任务描述
   - ImageUrl: 创建任务时，默认的镜像源
   - Preemptible: 默认情况下，是否以可抢占的模式提交任务
   - **ResourceQueueName**: 使用的队列名
   - **Storages**: 提交任务时候，容器内挂载的路径，

   配置火山云 aksk

   ```console
   Cannot found dlc config, create a new one:
   access_id: <username>
   access_key: <access_key>
   ```

## 方法二：直接编辑配置文件

> 具体字段可以参考[交互式填写配置文件](#方法一：交互式填写配置文件)

创建 `~/.config/clusterx.yaml` 文件，填入如下内容：

```yaml
aliyun: null
default: volc
volc:
  config: /fs-computility/llm/<username>/.volc/config.yaml
  tmpdir: /fs-computility/llm/<username>/.tmp
```

创建 `~/.dlc/config`，填入用户信息

```ini
[user]
access_id = <access_id>
access_key = <access_key>
```
