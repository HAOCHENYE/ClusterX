# 阿里云配置 clusterx

### 配置 clusterx

首先打开 `clusterx/constant.py`，填写集群相关的信息：

```python
aliyun_partition_quota = [
    ("分区名", "分区 quota id"),
]


aliyun_partition_ids = [
  ("分区名", "分区 id")
]
```

将具备权限的分区名和对应的 quota id 和分区 id 填入上面的列表中


## 方法一：交互式填写配置文件

1. 执行 clusterx 命令，触发自动配置，选择阿里云作为集群名:

    ```console
    clusterx --help

    输入集群名 (['aliyun', 'volc']) >>> [火山云填写 volc，阿里云填写 aliyun]
    ```

2. 选择默认分区，该分区会作为后续命令行启动任务的默认分区：

   ```console
   配置默认阿里云分区 [<分区名1>, <分区名2>] >>>
   ```

3. 选择镜像地址

   ```console
   配置默认阿里云镜像，查询地址：
   ...
   ```

   作为默认镜像

4. 选择数据挂载路径

   ```console
   以逗号为分隔符，输入 datasources
   >>>
   ```

   任务执行时需要将需要访问的文件挂载到容器中，目前阿里云集群默认只会把个人目录：`/cpfs01/user/${USER}` 和共享目录 `/cpfs01/shared/public` 挂载到容器里，如果需要访问其他目录，则需要指定 datasource id 进行手动挂载。

   例如如果想在容器内访问别的数据盘，就需要以**逗号**为分隔符，传入 datasources id：`<dataid1>,<dataid2>`

5. 配置容器内可访问的公共目录

   ```console
   配置任务节点可访问公共目录，回车默认使用 /cpfs01/user/yehaochen/.tmp  >>>
   ```

   clusterx 要求填入一个任务执行时可以访问到的公共目录，这个目录通常是和上一步填入的挂载目录相关。不过考虑到阿里云默认会挂载 `/cpfs01/user/${USER}`，所以这里可以直接填入 `/cpfs01/user/${USER}/.tmp` 作为默认值

6. 配置阿里云用户配置

   用户信息可以在阿里云的控制台右上角查看，如果不知道可以询问阿里云集群分区管理员

   ```console
   Cannot found dlc config, create a new one:
   access_id:<username>
   access_key:{access_key}
   ```

## 方法二：直接编辑配置文件

> 具体字段可以参考[交互式填写配置文件](#方法一：交互式填写配置文件)

创建 `~/.config/clusterx.yaml` 文件，填入如下内容：

```yaml
aliyun:
  data_sources:
  - <data-d1> # <数据挂载目录>
  - <data-id2>
  image: <image-url>  # 镜像地址
  partition: alillm_h1  # 分区名
  tmpdir: /cpfs01/user/yehaochen/.tmp  # 任务内可访问的公共目录
default: aliyun
volc: null
```

创建 `~/.dlc/config`，填入用户信息

```ini
[user]
access_id = ""
access_key = ""
```
