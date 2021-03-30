package ytrebuilder

const (
	//BindAddrField Field name of bind-addr config
	BindAddrField = "bind-addr"
	//AnalysisDBURLField Field name of analysisdb-url config
	AnalysisDBURLField = "analysisdb-url"
	//MaxOpenConnsField field name of max-open-conns
	MaxOpenConnsField = "max-open-conns"
	//MaxIdleConnsField field name of max-idle-conns
	MaxIdleConnsField = "max-idle-conns"
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
	//AuramqClientIDField Field name of auramq.client-id
	AuramqClientIDField = "auramq.client-id"

	//CompensationAllSyncURLsField Field name of compensation.all-sync-urls
	CompensationAllSyncURLsField = "compensation.all-sync-urls"
	//CompensationBatchSizeField Field name of compensation.batch-size
	CompensationBatchSizeField = "compensation.batch-size"
	//CompensationWaitTimeField Field name of compensation.wait-time
	CompensationWaitTimeField = "compensation.wait-time"
	//CompensationSkipTimeField Field name of compensation.skip-time
	CompensationSkipTimeField = "compensation.skip-time"

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
	//MiscRebuildingMinerCountPerBatchField Field name of misc.rebuilding-miner-count-per-batch
	MiscRebuildingMinerCountPerBatchField = "misc.rebuilding-miner-count-per-batch"
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
	//MiscRetryCountField Field name of misc.retry-count config
	MiscRetryCountField = "misc.retry-count"
	//MiscMaxCacheSizeField Field name of misc.max-cache-size config
	MiscMaxCacheSizeField = "misc.max-cache-size"
	//MiscFetchTaskRateField Field name of misc.fetch-task-rate
	MiscFetchTaskRateField = "misc.fetch-task-rate"
	//MiscSyncPoolLengthField Field name of misc.sync-pool-length config
	MiscSyncPoolLengthField = "misc.sync-pool-length"
	//MiscSyncQueueLengthField Field name of misc.sync-queue-length config
	MiscSyncQueueLengthField = "misc.sync-queue-length"
	//MiscWeightThresholdField Field name of misc.weight-threshold
	MiscWeightThresholdField = "misc.weight-threshold"
	//MiscMaxConcurrentTaskBuilderSizeField Field name of misc.max-concurrent-task-builder-size
	MiscMaxConcurrentTaskBuilderSizeField = "misc.max-concurrent-task-builder-size"
	//MiscMinerVersionThresholdField Field name of misc.miner-version-threshold
	MiscMinerVersionThresholdField = "misc.miner-version-threshold"
	//MiscTaskCacheLocationField Field name of misc.task-cache-location
	MiscTaskCacheLocationField = "misc.task-cache-location"
)

//Config system configuration
type Config struct {
	BindAddr       string              `mapstructure:"bind-addr"`
	AnalysisDBURL  string              `mapstructure:"analysisdb-url"`
	MaxOpenConns   int                 `mapstructure:"max-open-conns"`
	MaxIdleConns   int                 `mapstructure:"max-idle-conns"`
	RebuilderDBURL string              `mapstructure:"rebuilderdb-url"`
	AuraMQ         *AuraMQConfig       `mapstructure:"auramq"`
	Compensation   *CompensationConfig `mapstructure:"compensation"`
	Logger         *LogConfig          `mapstructure:"logger"`
	MiscConfig     *MiscConfig         `mapstructure:"misc"`
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
	ClientID             string   `mapstructure:"client-id"`
}

//CompensationConfig compensation configuration
type CompensationConfig struct {
	AllSyncURLs []string `mapstructure:"all-sync-urls"`
	BatchSize   int      `mapstructure:"batch-size"`
	WaitTime    int      `mapstructure:"wait-time"`
	SkipTime    int      `mapstructure:"skip-time"`
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
	RebuildingMinerCountPerBatch    int    `mapstructure:"rebuilding-miner-count-per-batch"`
	ProcessRebuildableMinerInterval int    `mapstructure:"process-rebuildable-miner-interval"`
	ProcessRebuildableShardInterval int    `mapstructure:"process-rebuildable-shard-interval"`
	ProcessReaperInterval           int    `mapstructure:"process-reaper-interval"`
	RebuildShardExpiredTime         int    `mapstructure:"rebuild-shard-expired-time"`
	RebuildShardTaskBatchSize       int    `mapstructure:"rebuild-shard-task-batch-size"`
	RebuildShardMinerTaskBatchSize  int    `mapstructure:"rebuild-shard-miner-task-batch-size"`
	RetryCount                      int    `mapstructure:"retry-count"`
	MaxCacheSize                    int64  `mapstructure:"max-cache-size"`
	FetchTaskRate                   int32  `mapstructure:"fetch-task-rate"`
	SyncPoolLength                  int    `mapstructure:"sync-pool-length"`
	SyncQueueLength                 int    `mapstructure:"sync-queue-length"`
	WeightThreshold                 int    `mapstructure:"weight-threshold"`
	MaxConcurrentTaskBuilderSize    int    `mapstructure:"max-concurrent-task-builder-size"`
	MinerVersionThreshold           int    `mapstructure:"miner-version-threshold"`
	TaskCacheLocation               string `mapstructure:"task-cache-location"`
}
