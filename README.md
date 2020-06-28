# yotta-rebuilder
## 1. 部署与配置：
在项目的main目录下编译：
```
$ go build -o rebuilder
```
配置文件为`yotta-rebuilder.yaml`（项目的main目录下有示例），默认可以放在`home`目录或抽查程序同目录下，各配置项说明如下：
```
#grpc绑定端口，默认为0.0.0.0:8080
bind-addr: ":8080"
#分析库的URL地址，默认为mongodb://127.0.0.1:27017/?connect=direct
analysisdb-url: "mongodb://127.0.0.1:27017/?connect=direct"
#重建库的URL地址，默认为mongodb://127.0.0.1:27017/?connect=direct
rebuilderdb-url: "mongodb://127.0.0.1:27017/?connect=direct"
#消息队列相关配置
auramq:
  #本地订阅者接收队列长度，默认值1024
  subscriber-buffer-size: 1024
  #客户端ping服务端时间间隔，默认值30秒
  ping-wait: 30
  #客户端读超时时间，默认值60秒
  read-wait: 60
  #客户端写超时时间，默认值10秒
  write-wait: 10
  #矿机信息同步主题名称，默认值sync
  miner-sync-topic: "sync"
  #需要监听的全部SN消息队列端口地址列表
  all-sn-urls:
  - "ws://172.17.0.2:8787/ws"
  - "ws://172.17.0.3:8787/ws"
  - "ws://172.17.0.4:8787/ws"
  #鉴权用账号名，需要在BP事先建好，默认值为空
  account: "yottanalysis"
  #鉴权用私钥，为account在BP上的active私钥，默认值为空
  private-key: "5JU7Q3PBEV3ZBHKU5bbVibGxuPzYnwb5HXCGgTedtuhCsDc52j7"
#日志相关配置
logger:
  #日志输出类型：stdout为输出到标准输出流，file为输出到文件，默认为stdout，此时只有level属性起作用，其他属性会被忽略
  output: "file"
  #日志输出等级，默认为Info
  level: "Debug"
  #日志路径，默认值为./rebuilder.log，仅在output=file时有效
  file-path: "./rebuilder.log"
  #日志拆分间隔时间，默认为24（小时），仅在output=file时有效
  rotation-time: 24
  #日志最大保留时间，默认为240（10天），仅在output=file时有效
  max-age: 240
#其他重建相关配置
misc:
  #矿机状态变为2并经过该时间后可以开始重建，默认为14400（秒），即4小时
  rebuildable-miner-time-gap: 14400
  #重建程序筛选可重建矿机的时间间隔，默认值为10（秒）
  process-rebuildable-miner-interval: 10
  #重建程序刷新可重建矿机的待重建分片列表的时间间隔，默认值为10（秒）
  process-rebuildable-shard-interval: 10
  #重建程序清除已完成重建的分片的时间间隔，默认值为60（秒）
  process-reaper-interval: 60
  #超过该时间且未收到重建完成响应的分片会被重新包装成重建任务并发出，默认值是1200（秒）
  rebuild-shard-expired-time: 1200
  #每次获取可重建矿机的待重建分片列表的数量，默认值是10000个分片
  rebuild-shard-task-batch-size: 10000
  #每次发送给重建矿机的任务数量，默认值是1000个分片
  rebuild-shard-miner-task-batch-size: 1000
  #允许以该参数指定的值作为前缀的矿机地址为有效地址，默认为空，一般用于内网测试环境
  exclude-addr-prefix: "/ip4/172.17"
```
启动服务：
```
$ nohup ./rebuilder &
```
如果不想使用配置文件也可以通过命令行标志来设置参数，标志指定的值也可以覆盖掉配置文件中对应的属性：
```
$ ./rebuilder --bind-addr ":8080" --analysisdb-url "mongodb://127.0.0.1:27017/?connect=direct" --auramq.subscriber-buffer-size "1024" --auramq.ping-wait "30" --auramq.read-wait "60" --auramq.write-wait "10" --auramq.miner-sync-topic "sync" --auramq.all-sn-urls "ws://172.17.0.2:8787/ws,ws://172.17.0.3:8787/ws,ws://172.17.0.4:8787/ws" --auramq.account "yottanalysis" --auramq.private-key "5JU7Q3PBEV3ZBHKU5bbVibGxuPzYnwb5HXCGgTedtuhCsDc52j7" --logger.output "file" --logger.file-path "./spotcheck.log" --logger.rotation-time "24" --logger.max-age "240" --logger.level "Info" --misc.reckecking-pool-length "5000" --misc.reckecking-queue-length "10000" --misc.avaliable-node-time-gap "3" --misc.miner-version-threshold "0" --misc.punish-phase1 "4" --misc.punish-phase2 "24" --misc.punish-phase3 "168" --misc.punish-phase1-percent "1" --misc.punish-phase2-percent "10" --misc.punish-phase3-percent "50" --misc.spotcheck-skip-time "0" --misc.spotcheck-interval "60" --misc.spotcheck-connect-timeout "10" --misc.error-node-percent-threshold "95" --misc.pool-error-miner-time-threshold "14400" --misc.exclude-addr-prefix "/ip4/172.17"
```
SN端目前测试版本只需要重新编译`YDTNMgmtJavaBinding`项目的`dev`分支并替换原有jar包即可