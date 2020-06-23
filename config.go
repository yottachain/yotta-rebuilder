package ytrebuilder

const (
	//BindAddrField Field name of bind-addr config
	BindAddrField = "bind-addr"
	//AnalysisDBURLField Field name of analysisdb-url config
	AnalysisDBURLField = "analysisdb-url"
	//RebuilderDBURLField Field name of rebuilderdb-url config
	RebuilderDBURLField = "rebuilderdb-url"

	//AuramqSubscriberBufferSizeField Field name of auramq.subscriber-buffer-size
	AuramqSubscriberBufferSizeField = "auramq.subscriber-buffer-size"
	//AuramqPingWaitField Field name of auramq.ping-wait
	AuramqPingWaitField = "auramq.ping-wait"
	//AuramqReadWaitField Field name of auramq.read-wait
	AuramqReadWaitField = "auramq.read-wait"
	//AuramqWriteWaitField Field name of auramq.write-wait
	AuramqWriteWaitField = "auramq.write-wait"
	//AuramqMinerSyncTopicField Field name of auramq.miner-sync-topic
	AuramqMinerSyncTopicField = "auramq.miner-sync-topic"
	//AuramqAllSNURLsField Field name of auramq.all-sn-urls
	AuramqAllSNURLsField = "auramq.all-sn-urls"
	//AuramqAccountField Field name of auramq.account
	AuramqAccountField = "auramq.account"
	//AuramqPrivateKeyField Field name of auramq.private-key
	AuramqPrivateKeyField = "auramq.private-key"

	//LoggerOutputField Field name of logger.output config
	LoggerOutputField = "logger.output"
	//LoggerFilePathField Field name of logger.file-path config
	LoggerFilePathField = "logger.file-path"
	//LoggerRotationTimeField Field name of logger.rotation-time config
	LoggerRotationTimeField = "logger.rotation-time"
	//LoggerMaxAgeField Field name of logger.rotation-time config
	LoggerMaxAgeField = "logger.max-age"
	//LoggerLevelField Field name of logger.level config
	LoggerLevelField = "logger.level"

	//MiscRebuildableMinerTimeGapField Field name of misc.rebuildable-miner-time-gap config
	MiscRebuildableMinerTimeGapField = "misc.rebuildable-miner-time-gap"
	//MiscProcessRebuildableMinerIntervalField Field name of misc.process-rebuildable-miner-interval config
	MiscProcessRebuildableMinerIntervalField = "misc.process-rebuildable-miner-interval"
	//MiscProcessRebuildableShardIntervalField Field name of misc.process-rebuildable-shard-interval config
	MiscProcessRebuildableShardIntervalField = "misc.process-rebuildable-shard-interval"
	//MiscProcessReaperIntervalField Field name of misc.process-reaper-interval config
	MiscProcessReaperIntervalField = "misc.process-reaper-interval"
	//MiscRebuildShardExpiredTimeField Field name of misc.rebuild-shard-expired-time
	MiscRebuildShardExpiredTimeField = "misc.rebuild-shard-expired-time"
	//MiscRebuildShardTaskBatchSizeField Field name of misc.rebuild-shard-task-batch-size
	MiscRebuildShardTaskBatchSizeField = "misc.rebuild-shard-task-batch-size"
	//MiscRebuildShardMinerTaskBatchSizeField Field name of misc.rebuild-shard-miner-task-batch-size
	MiscRebuildShardMinerTaskBatchSizeField = "misc.rebuild-shard-miner-task-batch-size"
	//MiscExcludeAddrPrefixField Field name of misc.exclude-addr-prefix config
	MiscExcludeAddrPrefixField = "misc.exclude-addr-prefix"
)

//Config system configuration
type Config struct {
	BindAddr       string        `mapstructure:"bind-addr"`
	AnalysisDBURL  string        `mapstructure:"analysisdb-url"`
	RebuilderDBURL string        `mapstructure:"rebuilderdb-url"`
	AuraMQ         *AuraMQConfig `mapstructure:"auramq"`
	Logger         *LogConfig    `mapstructure:"logger"`
	MiscConfig     *MiscConfig   `mapstructure:"misc"`
}

//AuraMQConfig auramq configuration
type AuraMQConfig struct {
	SubscriberBufferSize int      `mapstructure:"subscriber-buffer-size"`
	PingWait             int      `mapstructure:"ping-wait"`
	ReadWait             int      `mapstructure:"read-wait"`
	WriteWait            int      `mapstructure:"write-wait"`
	MinerSyncTopic       string   `mapstructure:"miner-sync-topic"`
	AllSNURLs            []string `mapstructure:"all-sn-urls"`
	Account              string   `mapstructure:"account"`
	PrivateKey           string   `mapstructure:"private-key"`
}

//LogConfig system log configuration
type LogConfig struct {
	Output       string `mapstructure:"output"`
	FilePath     string `mapstructure:"file-path"`
	RotationTime int64  `mapstructure:"rotation-time"`
	MaxAge       int64  `mapstructure:"max-age"`
	Level        string `mapstructure:"level"`
}

//MiscConfig miscellaneous configuration
type MiscConfig struct {
	RebuildableMinerTimeGap         int    `mapstructure:"rebuildable-miner-time-gap"`
	ProcessRebuildableMinerInterval int    `mapstructure:"process-rebuildable-miner-interval"`
	ProcessRebuildableShardInterval int    `mapstructure:"process-rebuildable-shard-interval"`
	ProcessReaperInterval           int    `mapstructure:"process-reaper-interval"`
	RebuildShardExpiredTime         int    `mapstructure:"rebuild-shard-expired-time"`
	RebuildShardTaskBatchSize       int    `mapstructure:"misc.rebuild-shard-task-batch-size"`
	RebuildShardMinerTaskBatchSize  int    `mapstructure:"misc.rebuild-shard-miner-task-batch-size"`
	ExcludeAddrPrefix               string `mapstructure:"exclude-addr-prefix"`
}
