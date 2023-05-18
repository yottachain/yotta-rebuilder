package ytrebuilder

const (
	//BindAddrField Field name of bind-addr config
	BindAddrField = "bind-addr"
	//PDURLsField Field name of pd-urls config
	PDURLsField = "pd-urls"
	//RebuilderDBURLField Field name of rebuilderdb-url config
	RebuilderDBURLField = "rebuilderdb-url"

	//ESURLsField Field name of esconfig.urls config
	ESURLsField = "esconfig.urls"
	//ESUserNameField Field name of esconfig.username config
	ESUserNameField = "esconfig.username"
	//ESPasswordField Field name of esconfig.password config
	ESPasswordField = "esconfig.password"

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
	//AuramqSymmetricField Field name of auramq.symmetric
	AuramqSymmetricField = "auramq.symmetric"

	//CompensationAllSyncURLsField Field name of compensation.all-sync-urls
	CompensationAllSyncURLsField = "compensation.all-sync-urls"
	//CompensationSyncClientURLField Field name of compensation.sync-client-url
	CompensationSyncClientURLField = "compensation.sync-client-url"
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
	//MiscRemoveMinerAddrsField Field name of misc.remove-miner-addrs
	MiscRemoveMinerAddrsField = "misc.remove-miner-addrs"
	//MiscRoundThresholdField Field name of misc.round-threshold
	MiscRoundThresholdField = "misc.round-threshold"
)

//Config system configuration
type Config struct {
	BindAddr       string              `mapstructure:"bind-addr"`
	PDURLs         []string            `mapstructure:"pd-urls"`
	RebuilderDBURL string              `mapstructure:"rebuilderdb-url"`
	ESConfig       *ESConfig           `mapstructure:"esconfig"`
	AuraMQ         *AuraMQConfig       `mapstructure:"auramq"`
	Compensation   *CompensationConfig `mapstructure:"compensation"`
	Logger         *LogConfig          `mapstructure:"logger"`
	MiscConfig     *MiscConfig         `mapstructure:"misc"`
}

type ESConfig struct {
	URLs     []string `mapstructure:"urls"`
	UserName string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
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
	Symmetric            bool     `mapstructure:"symmetric"`
}

//CompensationConfig compensation configuration
type CompensationConfig struct {
	AllSyncURLs   []string `mapstructure:"all-sync-urls"`
	SyncClientURL string   `mapstructure:"sync-client-url"`
	BatchSize     int      `mapstructure:"batch-size"`
	WaitTime      int      `mapstructure:"wait-time"`
	SkipTime      int      `mapstructure:"skip-time"`
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
	RebuildableMinerTimeGap         int  `mapstructure:"rebuildable-miner-time-gap"`
	RebuildingMinerCountPerBatch    int  `mapstructure:"rebuilding-miner-count-per-batch"`
	ProcessRebuildableMinerInterval int  `mapstructure:"process-rebuildable-miner-interval"`
	ProcessRebuildableShardInterval int  `mapstructure:"process-rebuildable-shard-interval"`
	ProcessReaperInterval           int  `mapstructure:"process-reaper-interval"`
	RebuildShardExpiredTime         int  `mapstructure:"rebuild-shard-expired-time"`
	RebuildShardTaskBatchSize       int  `mapstructure:"rebuild-shard-task-batch-size"`
	RebuildShardMinerTaskBatchSize  int  `mapstructure:"rebuild-shard-miner-task-batch-size"`
	SyncPoolLength                  int  `mapstructure:"sync-pool-length"`
	SyncQueueLength                 int  `mapstructure:"sync-queue-length"`
	WeightThreshold                 int  `mapstructure:"weight-threshold"`
	MaxConcurrentTaskBuilderSize    int  `mapstructure:"max-concurrent-task-builder-size"`
	MinerVersionThreshold           int  `mapstructure:"miner-version-threshold"`
	RemoveMinerAddrs                bool `mapstructure:"remove-miner-addrs"`
	RoundThreshold                  int  `mapstructure:"round-threshold"`
}
