# yotta-rebuilder
## 1. 部署与配置：
在项目的main目录下编译：
```
$ go build -o rebuilder
```
配置文件为`yotta-rebuilder.yaml`（项目的main目录下有示例），默认可以放在`home`目录或重建程序同目录下，各配置项说明如下：
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
  #MQ客户端ID，连接SN的MQ时使用，必须保证在MQ server端唯一，默认值为yottarebuilder
  client-id: "yottarebuilder"
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
  #修复失败分片的重试次数，默认为3
  retry-count: 3
  #最大缓存大小，缓存用于每个分片重建时相关分片的缓存，默认值为100000
  max-cache-size: 100000
```
启动服务：
```
$ nohup ./rebuilder &
```
如果不想使用配置文件也可以通过命令行标志来设置参数，标志指定的值也可以覆盖掉配置文件中对应的属性：
```
$ ./rebuilder --bind-addr ":8080" --analysisdb-url "mongodb://127.0.0.1:27017/?connect=direct" --rebuilderdb-url "mongodb://127.0.0.1:27017/?connect=direct" --auramq.subscriber-buffer-size "1024" --auramq.ping-wait "30" --auramq.read-wait "60" --auramq.write-wait "10" --auramq.miner-sync-topic "sync" --auramq.all-sn-urls "ws://172.17.0.2:8787/ws,ws://172.17.0.3:8787/ws,ws://172.17.0.4:8787/ws" --auramq.account "yottanalysis" --auramq.private-key "5JU7Q3PBEV3ZBHKU5bbVibGxuPzYnwb5HXCGgTedtuhCsDc52j7" --auramq.client-id "yottarebuilder" --logger.output "file" --logger.file-path "./rebuilder.log" --logger.rotation-time "24" --logger.max-age "240" --logger.level "Info" --misc.rebuildable-miner-time-gap "14400" --misc.process-rebuildable-miner-interval "10" --misc.process-rebuildable-shard-interval "10" --misc.process-reaper-interval "60" --misc.rebuild-shard-expired-time "1200" --misc.rebuild-shard-task-batch-size "10000" --misc.rebuild-shard-miner-task-batch-size "1000" --misc.exclude-addr-prefix "/ip4/172.17" --misc.retry-count "3" --misc.max-cache-size "100000"
```
SN端目前测试版本只需要重新编译`YDTNMgmtJavaBinding`项目的`dev`分支并替换原有jar包即可

## 2. 数据库配置：
analysisdb为analysis服务的数据库，该数据库主要用于获取矿机所属分片，其`metabase.shards`表的索引建立在`nodeId`和`_id`两个字段上，另外同analysis服务一样，rebuilder服务需要将各SN所属mongoDB数据库的分块分片数据同步至rebuilder服务所连接的mongoDB实例，需使用![yotta-sync-server](https://github.com/yottachain/yotta-sync-server)项目进行数据同步。该项目会将全部SN的metabase库中的blocks和shards集合同步至rebuilder服务所接入mongoDB实例的metabase库；除此之外还需要建立名称为`rebuilder`的分析库用于记录重建过程中的数据，该库包含三个集合，分别为`Node`、`RebuildMiner`和`RebuildShard`，`RebuildMiner`字段如下：
| 字段 | 类型 | 描述 |
| ---- | ---- | ---- |
| _id | int32 | 矿机ID，主键 |
| from | int64 | 被重建分片范围的起始ID |
| to |int64 |	被重建分片范围的结束ID |
| status | int32 | 矿机状态：2-待重建或重建中，3-重建完成 |
| timestamp	| int64	| 记录重建各阶段的时间戳 |

另外需要为`RebuildMiner`集合添加索引：
```
mongoshell> db.RebuildMiner.createIndex({status: 1, timestamp: 1})
```
`RebuildShard`字段如下
| 字段 | 类型 | 描述 |
| ---- | ---- | ---- |
| _id | int64 | 分片ID，主键 |
| VHF | int64 | 分片摘要 |
| minerID |int32 |	分片所属矿机ID |
| blockID | int64 | 分片所属块ID |
| type | int32 | 重建类型,0xc258为副本集，0x68b3为LRC编码 |
| VNF | int32 | 所属分块中的总分片数 |
| snID | int32 | 所属分块的所属SN ID|
| parityShardCount | int32 | 校验分片数量 |
| timestamp	| int64	| 记录分片在重建各阶段的时间戳 |
| errCount | int32 | 分片重建错误次数 |

另外需要为`RebuildShard`集合添加索引：
```
mongoshell> db.RebuildShard.createIndex({minerID: 1, timestamp: 1})
mongoshell> db.RebuildShard.createIndex({minerID: 1, _id: 1})
```
rebuilder服务启动后，也会从全部SN同步矿机信息至`rebuilder`库的`Node`集合，需要先将SN中全部矿机数据导入该集合：
在SN端：
```
$ mongoexport -h 127.0.0.1 --port 27017 -d yotta -c Node -o node.json
```
在重建服务器端：
```
$ mongoimport -h 127.0.0.1 --port 27017 -d rebuilder -c Node --file node.json
```

另外需要为`Node`集合建立索引：
```
mongoshell> db.Node.createIndex({status:1, timestamp:1})
```

## 3. 执行流程
* 重建程序启动后，会从各SN实时同步矿机信息到本地`rebuilder`库中的`Node`表，当发现有矿机的`status`属性变为2后，会在本地`Node`库为其增加一个`tasktimestamp`字段，其值为当前时间的时间戳
* 矿机筛选进程每隔`process-rebuildable-miner-interval`秒从`Node`表筛选出`status=2`并且`tasktimestamp`小于当前时间减去`rebuildable-miner-time-gap`的矿机（为了确保`status`变为2后没有新分片写入矿机），并将相关信息写入`RebuildMiner`表
* 分片刷新进程每隔`process-rebuildable-shard-interval`秒从`metabase`库的`shards`表中根据矿机ID按照`_id`顺序取出`rebuild-shard-task-batch-size`个分片，然后构造重建任务并写入`RebuildShard`表
* 没有重建任务的矿机在上报时SN会从重建程序获取重建任务，重建程序首先判断上报的矿机是否`status`为1且`weight`大于0，然后从`RebuildShard`表中获取`rebuild-shard-miner-task-batch-size`个重建任务，要求每一条重建任务的`timestamp`的值必须小于当前时间减去`rebuild-shard-expired-time`（即任务是第一次被发出或发出一段时间后没有收到响应），获取任务后同时将`timestamp`修改为当前时间的时间戳，每个任务会根据不同的重建类型获取相应信息后封装成protobuf格式消息，打包后发送给矿机
* 矿机收到消息后解析出重建任务，并依次执行重建，全部完成后会将所有重建结果打包发送给对应SN
* SN收到重建结果后转发给重建程序，重建程序根据结果更新`RebuildShard`表，成功完成重建的任务会将其`timestamp`字段更新为`INT64_MAX`，失败的任务会将其`errCount`字段加一，当达到`retry-count`指定次数后会将其该任务写入`UnrebuildShard`表，表示该任务重建失败
* 每隔`process-reaper-interval`指定的时间后，回收进程会将已完成的重建任务清除