#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")"

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "错误：未检测到 Docker Compose。请先安装 docker compose 插件。" >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "错误：Docker 未启动，或者当前用户无权访问 Docker。" >&2
  exit 1
fi

usage() {
  cat <<'USAGE'
用法：
  ./deploy.sh deploy          首次部署：拉取镜像、初始化数据库并启动全部服务
  ./deploy.sh start           启动已有环境，不重复初始化数据库
  ./deploy.sh upgrade-schema  初始化或升级数据库结构
  ./deploy.sh stop            停止并删除容器，保留数据卷
  ./deploy.sh restart         重启 DolphinScheduler 服务
  ./deploy.sh status          查看服务状态
  ./deploy.sh logs [service]  查看日志；不传 service 时查看全部日志
  ./deploy.sh destroy         删除容器、网络和数据卷（会清空全部数据）
USAGE
}

wait_for_api() {
  local port="${DS_API_PORT:-12345}"
  if [[ -f .env ]]; then
    local configured_port
    configured_port="$(grep -E '^DS_API_PORT=' .env | tail -n1 | cut -d= -f2- || true)"
    [[ -n "$configured_port" ]] && port="$configured_port"
  fi

  echo "等待 API 服务就绪……"
  for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:${port}/dolphinscheduler/actuator/health" >/dev/null 2>&1; then
      echo "DolphinScheduler 已启动。"
      echo "访问地址：http://服务器IP:${port}/dolphinscheduler/ui"
      echo "默认账号：admin"
      echo "默认密码：dolphinscheduler123"
      return 0
    fi
    sleep 5
  done

  echo "API 暂未通过健康检查，请运行：./deploy.sh status 或 ./deploy.sh logs dolphinscheduler-api" >&2
  return 1
}

command_name="${1:-deploy}"
shift || true

case "$command_name" in
  deploy)
    echo "拉取 DolphinScheduler 及依赖镜像……"
    "${COMPOSE[@]}" --profile schema --profile all pull

    echo "启动 PostgreSQL……"
    "${COMPOSE[@]}" --profile schema up -d dolphinscheduler-postgresql

    echo "初始化/升级 DolphinScheduler 数据库结构……"
    "${COMPOSE[@]}" --profile schema run --rm dolphinscheduler-schema-initializer

    echo "启动全部 DolphinScheduler 服务……"
    "${COMPOSE[@]}" --profile all up -d
    "${COMPOSE[@]}" --profile all ps
    wait_for_api
    ;;

  start)
    "${COMPOSE[@]}" --profile all up -d
    "${COMPOSE[@]}" --profile all ps
    wait_for_api
    ;;

  upgrade-schema)
    "${COMPOSE[@]}" --profile schema up -d dolphinscheduler-postgresql
    "${COMPOSE[@]}" --profile schema run --rm dolphinscheduler-schema-initializer
    ;;

  stop)
    "${COMPOSE[@]}" --profile schema --profile all down
    ;;

  restart)
    "${COMPOSE[@]}" --profile all restart
    "${COMPOSE[@]}" --profile all ps
    ;;

  status)
    "${COMPOSE[@]}" --profile schema --profile all ps
    ;;

  logs)
    if [[ $# -gt 0 ]]; then
      "${COMPOSE[@]}" --profile schema --profile all logs -f --tail=200 "$1"
    else
      "${COMPOSE[@]}" --profile schema --profile all logs -f --tail=200
    fi
    ;;

  destroy)
    echo "警告：该操作会删除 PostgreSQL、ZooKeeper 和 DolphinScheduler 的全部数据。"
    read -r -p "输入 DELETE 确认：" answer
    if [[ "$answer" != "DELETE" ]]; then
      echo "已取消。"
      exit 0
    fi
    "${COMPOSE[@]}" --profile schema --profile all down -v --remove-orphans
    ;;

  -h|--help|help)
    usage
    ;;

  *)
    echo "未知命令：$command_name" >&2
    usage
    exit 1
    ;;
esac
