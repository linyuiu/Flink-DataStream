# DolphinScheduler 3.4.2 Docker Compose

这是一个单机、单 Master、单 Worker 的 DolphinScheduler 部署，包含：

- PostgreSQL
- ZooKeeper
- DolphinScheduler API
- DolphinScheduler Master
- DolphinScheduler Worker
- DolphinScheduler Alert Server
- Schema Initializer

## 部署

```bash
chmod +x deploy.sh
./deploy.sh deploy
```

访问：

```text
http://服务器IP:12345/dolphinscheduler/ui
```

默认账号：`admin`

默认密码：`dolphinscheduler123`

## 常用命令

```bash
./deploy.sh status
./deploy.sh logs
./deploy.sh logs dolphinscheduler-worker
./deploy.sh restart
./deploy.sh stop
./deploy.sh start
```

`stop` 会保留数据卷；`destroy` 会永久删除数据。

## 端口冲突

在 `.env` 中修改：

```dotenv
DS_API_PORT=12345
DS_PY_GATEWAY_PORT=25333
POSTGRES_PORT=5433
ZOOKEEPER_PORT=2181
```

修改后重新执行：

```bash
./deploy.sh stop
./deploy.sh start
```

## 注意

- 建议至少预留 4 GB 可用内存。
- 此配置适合学习、开发测试和轻量单机使用，不是高可用生产集群。
- Flink、Spark、DataX、MySQL JDBC 等任务可能需要额外安装对应插件、客户端或驱动。
