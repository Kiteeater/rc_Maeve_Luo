#!/bin/sh
# 用本地 compose 里的 mqadmin 创建即时 Topic 和延迟 Topic。
# 属性语法以 RocketMQ 5.3.1 的 updateTopic -a 为准。
set -eu
cd "$(dirname "$0")/.."

create_topic() {
  topic=$1
  type=$2
  docker compose exec -T rocketmq-broker sh mqadmin updateTopic \
    -n rocketmq-namesrv:9876 \
    -c DefaultCluster \
    -t "$topic" \
    -a "+message.type=${type}"
}

create_topic notifier-delivery NORMAL
create_topic notifier-delivery-delay DELAY
