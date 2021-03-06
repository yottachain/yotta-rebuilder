package cmd

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	homedir "github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	ytrebuilder "github.com/yottachain/yotta-rebuilder"
	"github.com/yottachain/yotta-rebuilder/pb"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "yotta-rebuilder",
	Short: "rebuilder service of YottaChain",
	Long:  `yotta-rebuilder is an rebuilder service performing data rebuilding task.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		config := new(ytrebuilder.Config)
		if err := viper.Unmarshal(config); err != nil {
			panic(fmt.Sprintf("unable to decode into config struct, %v\n", err))
		}
		// if len(config.MongoDBURLs) != int(config.SNCount) {
		// 	panic("count of mongoDB URL is not equal to SN count\n")
		// }
		initLog(config)
		rebuilder, err := ytrebuilder.New(config.AnalysisDBURL, config.RebuilderDBURL, config.AuraMQ, config.MiscConfig)
		if err != nil {
			panic(fmt.Sprintf("fatal error when starting rebuilder service: %s\n", err))
		}
		rebuilder.Start()
		lis, err := net.Listen("tcp", config.BindAddr)
		if err != nil {
			log.Fatalf("failed to listen address %s: %s\n", config.BindAddr, err)
		}
		log.Infof("GRPC address: %s", config.BindAddr)
		grpcServer := grpc.NewServer()
		server := &ytrebuilder.Server{Rebuilder: rebuilder}
		pb.RegisterRebuilderServer(grpcServer, server)
		grpcServer.Serve(lis)
		log.Info("GRPC server started")
	},
}

func initLog(config *ytrebuilder.Config) {
	switch strings.ToLower(config.Logger.Output) {
	case "file":
		writer, _ := rotatelogs.New(
			config.Logger.FilePath+".%Y%m%d",
			rotatelogs.WithLinkName(config.Logger.FilePath),
			rotatelogs.WithMaxAge(time.Duration(config.Logger.MaxAge)*time.Hour),
			rotatelogs.WithRotationTime(time.Duration(config.Logger.RotationTime)*time.Hour),
		)
		log.SetOutput(writer)
	case "stdout":
		log.SetOutput(os.Stdout)
	default:
		fmt.Printf("no such option: %s, use stdout\n", config.Logger.Output)
		log.SetOutput(os.Stdout)
	}
	log.SetFormatter(&log.TextFormatter{})
	levelMap := make(map[string]log.Level)
	levelMap["panic"] = log.PanicLevel
	levelMap["fatal"] = log.FatalLevel
	levelMap["error"] = log.ErrorLevel
	levelMap["warn"] = log.WarnLevel
	levelMap["info"] = log.InfoLevel
	levelMap["debug"] = log.DebugLevel
	levelMap["trace"] = log.TraceLevel
	log.SetLevel(levelMap[strings.ToLower(config.Logger.Level)])
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/yotta-rebuilder.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	initFlag()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".yotta-rebuilder" (without extension).
		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigName("yotta-rebuilder")
		viper.SetConfigType("yaml")
	}

	// viper.AutomaticEnv() // read in environment variables that match
	// viper.SetEnvPrefix("analysis")
	// viper.SetEnvKeyReplacer(strings.NewReplacer("_", "."))

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			fmt.Println("Config file not found.")
		} else {
			// Config file was found but another error was produced
			fmt.Println("Error:", err.Error())
			os.Exit(1)
		}
	}
}

var (
	//DefaultBindAddr default value of BindAddr
	DefaultBindAddr string = ":8080"
	//DefaultAnalysisDBURL default value of AnalysisDBURL
	DefaultAnalysisDBURL string = "mongodb://127.0.0.1:27017/?connect=direct"
	//DefaultRebuilderDBURL default value of RebuilderDBURL
	DefaultRebuilderDBURL string = "mongodb://127.0.0.1:27017/?connect=direct"

	//DefaultAuramqSubscriberBufferSize default value of AuramqSubscriberBufferSize
	DefaultAuramqSubscriberBufferSize = 1024
	//DefaultAuramqPingWait default value of AuramqPingWait
	DefaultAuramqPingWait = 30
	//DefaultAuramqReadWait default value of AuramqReadWait
	DefaultAuramqReadWait = 60
	//DefaultAuramqWriteWait default value of AuramqWriteWait
	DefaultAuramqWriteWait = 10
	//DefaultAuramqMinerSyncTopic default value of AuramqMinerSyncTopic
	DefaultAuramqMinerSyncTopic = "sync"
	//DefaultAuramqAllSNURLs default value of AuramqAllSNURLs
	DefaultAuramqAllSNURLs = []string{}
	//DefaultAuramqAccount default value of AuramqAccount
	DefaultAuramqAccount = ""
	//DefaultAuramqPrivateKey default value of AuramqPrivateKey
	DefaultAuramqPrivateKey = ""

	//DefaultLoggerOutput default value of LoggerOutput
	DefaultLoggerOutput string = "stdout"
	//DefaultLoggerFilePath default value of LoggerFilePath
	DefaultLoggerFilePath string = "./rebuilder.log"
	//DefaultLoggerRotationTime default value of LoggerRotationTime
	DefaultLoggerRotationTime int64 = 24
	//DefaultLoggerMaxAge default value of LoggerMaxAge
	DefaultLoggerMaxAge int64 = 240
	//DefaultLoggerLevel default value of LoggerLevel
	DefaultLoggerLevel string = "Info"

	//DefaultMiscRebuildableMinerTimeGap default value of MiscRebuildableMinerTimeGap
	DefaultMiscRebuildableMinerTimeGap int = 14400
	//DefaultMiscProcessRebuildableMinerInterval default value of MiscProcessRebuildableMinerInterval
	DefaultMiscProcessRebuildableMinerInterval int = 10
	//DefaultMiscProcessRebuildableShardInterval default value of MiscProcessRebuildableShardInterval
	DefaultMiscProcessRebuildableShardInterval int = 10
	//DefaultMiscProcessReaperInterval default value of MiscProcessReaperInterval
	DefaultMiscProcessReaperInterval int = 60
	//DefaultMiscRebuildShardExpiredTime default value of MiscRebuildShardExpiredTime
	DefaultMiscRebuildShardExpiredTime int = 1200
	//DefaultMiscRebuildShardTaskBatchSize default value of MiscRebuildShardTaskBatchSize
	DefaultMiscRebuildShardTaskBatchSize int = 10000
	//DefaultMiscRebuildShardMinerTaskBatchSize default value of MiscRebuildShardMinerTaskBatchSize
	DefaultMiscRebuildShardMinerTaskBatchSize int = 1000
	//DefaultMiscExcludeAddrPrefix default value of MiscExcludeAddrPrefix
	DefaultMiscExcludeAddrPrefix string = ""
)

func initFlag() {
	//main config
	rootCmd.PersistentFlags().String(ytrebuilder.BindAddrField, DefaultBindAddr, "Binding address of GRPC server")
	viper.BindPFlag(ytrebuilder.BindAddrField, rootCmd.PersistentFlags().Lookup(ytrebuilder.BindAddrField))
	rootCmd.PersistentFlags().String(ytrebuilder.AnalysisDBURLField, DefaultAnalysisDBURL, "mongoDB URL of analysis database")
	viper.BindPFlag(ytrebuilder.AnalysisDBURLField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AnalysisDBURLField))
	//AuraMQ config
	rootCmd.PersistentFlags().Int(ytrebuilder.AuramqSubscriberBufferSizeField, DefaultAuramqSubscriberBufferSize, "subscriber buffer size")
	viper.BindPFlag(ytrebuilder.AuramqSubscriberBufferSizeField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AuramqSubscriberBufferSizeField))
	rootCmd.PersistentFlags().Int(ytrebuilder.AuramqPingWaitField, DefaultAuramqPingWait, "ping interval of MQ client")
	viper.BindPFlag(ytrebuilder.AuramqPingWaitField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AuramqPingWaitField))
	rootCmd.PersistentFlags().Int(ytrebuilder.AuramqReadWaitField, DefaultAuramqReadWait, "read wait of MQ client")
	viper.BindPFlag(ytrebuilder.AuramqReadWaitField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AuramqReadWaitField))
	rootCmd.PersistentFlags().Int(ytrebuilder.AuramqWriteWaitField, DefaultAuramqWriteWait, "write wait of MQ client")
	viper.BindPFlag(ytrebuilder.AuramqWriteWaitField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AuramqWriteWaitField))
	rootCmd.PersistentFlags().String(ytrebuilder.AuramqMinerSyncTopicField, DefaultAuramqMinerSyncTopic, "miner sync topic name")
	viper.BindPFlag(ytrebuilder.AuramqMinerSyncTopicField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AuramqMinerSyncTopicField))
	rootCmd.PersistentFlags().StringSlice(ytrebuilder.AuramqAllSNURLsField, DefaultAuramqAllSNURLs, "all URLs of MQ port, in the form of --auramq.all-sn-urls \"URL1,URL2,URL3\"")
	viper.BindPFlag(ytrebuilder.AuramqAllSNURLsField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AuramqAllSNURLsField))
	rootCmd.PersistentFlags().String(ytrebuilder.AuramqAccountField, DefaultAuramqAccount, "yottanalysis")
	viper.BindPFlag(ytrebuilder.AuramqAccountField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AuramqAccountField))
	rootCmd.PersistentFlags().String(ytrebuilder.AuramqPrivateKeyField, DefaultAuramqPrivateKey, "")
	viper.BindPFlag(ytrebuilder.AuramqPrivateKeyField, rootCmd.PersistentFlags().Lookup(ytrebuilder.AuramqPrivateKeyField))
	//logger config
	rootCmd.PersistentFlags().String(ytrebuilder.LoggerOutputField, DefaultLoggerOutput, "Output type of logger(stdout or file)")
	viper.BindPFlag(ytrebuilder.LoggerOutputField, rootCmd.PersistentFlags().Lookup(ytrebuilder.LoggerOutputField))
	rootCmd.PersistentFlags().String(ytrebuilder.LoggerFilePathField, DefaultLoggerFilePath, "Output path of log file")
	viper.BindPFlag(ytrebuilder.LoggerFilePathField, rootCmd.PersistentFlags().Lookup(ytrebuilder.LoggerFilePathField))
	rootCmd.PersistentFlags().Int64(ytrebuilder.LoggerRotationTimeField, DefaultLoggerRotationTime, "Rotation time(hour) of log file")
	viper.BindPFlag(ytrebuilder.LoggerRotationTimeField, rootCmd.PersistentFlags().Lookup(ytrebuilder.LoggerRotationTimeField))
	rootCmd.PersistentFlags().Int64(ytrebuilder.LoggerMaxAgeField, DefaultLoggerMaxAge, "Within the time(hour) of this value each log file will be kept")
	viper.BindPFlag(ytrebuilder.LoggerMaxAgeField, rootCmd.PersistentFlags().Lookup(ytrebuilder.LoggerMaxAgeField))
	rootCmd.PersistentFlags().String(ytrebuilder.LoggerLevelField, DefaultLoggerLevel, "Log level(Trace, Debug, Info, Warning, Error, Fatal, Panic)")
	viper.BindPFlag(ytrebuilder.LoggerLevelField, rootCmd.PersistentFlags().Lookup(ytrebuilder.LoggerLevelField))
	//Misc config
	rootCmd.PersistentFlags().Int(ytrebuilder.MiscRebuildableMinerTimeGapField, DefaultMiscRebuildableMinerTimeGap, "time gap between miner becoming rebuildable and starting rebuilding")
	viper.BindPFlag(ytrebuilder.MiscRebuildableMinerTimeGapField, rootCmd.PersistentFlags().Lookup(ytrebuilder.MiscRebuildableMinerTimeGapField))
	rootCmd.PersistentFlags().Int(ytrebuilder.MiscProcessRebuildableMinerIntervalField, DefaultMiscProcessRebuildableMinerInterval, "time interval of rebuildable miner fetching process")
	viper.BindPFlag(ytrebuilder.MiscProcessRebuildableMinerIntervalField, rootCmd.PersistentFlags().Lookup(ytrebuilder.MiscProcessRebuildableMinerIntervalField))
	rootCmd.PersistentFlags().Int(ytrebuilder.MiscProcessRebuildableShardIntervalField, DefaultMiscProcessRebuildableShardInterval, "time interval of rebuildable shard fetching process")
	viper.BindPFlag(ytrebuilder.MiscProcessRebuildableShardIntervalField, rootCmd.PersistentFlags().Lookup(ytrebuilder.MiscProcessRebuildableShardIntervalField))
	rootCmd.PersistentFlags().Int(ytrebuilder.MiscProcessReaperIntervalField, DefaultMiscProcessReaperInterval, "time interval of reaper process")
	viper.BindPFlag(ytrebuilder.MiscProcessReaperIntervalField, rootCmd.PersistentFlags().Lookup(ytrebuilder.MiscProcessReaperIntervalField))
	rootCmd.PersistentFlags().Int(ytrebuilder.MiscRebuildShardExpiredTimeField, DefaultMiscRebuildShardExpiredTime, "expire time of shard-rebuilding task")
	viper.BindPFlag(ytrebuilder.MiscRebuildShardExpiredTimeField, rootCmd.PersistentFlags().Lookup(ytrebuilder.MiscRebuildShardExpiredTimeField))
	rootCmd.PersistentFlags().Int(ytrebuilder.MiscRebuildShardTaskBatchSizeField, DefaultMiscRebuildShardTaskBatchSize, "batch size when fetching shard-rebuilding tasks")
	viper.BindPFlag(ytrebuilder.MiscRebuildShardTaskBatchSizeField, rootCmd.PersistentFlags().Lookup(ytrebuilder.MiscRebuildShardTaskBatchSizeField))
	rootCmd.PersistentFlags().Int(ytrebuilder.MiscRebuildShardMinerTaskBatchSizeField, DefaultMiscRebuildShardMinerTaskBatchSize, "batch size when sending shard-rebuilding tasks to miner")
	viper.BindPFlag(ytrebuilder.MiscRebuildShardMinerTaskBatchSizeField, rootCmd.PersistentFlags().Lookup(ytrebuilder.MiscRebuildShardMinerTaskBatchSizeField))
	rootCmd.PersistentFlags().String(ytrebuilder.MiscExcludeAddrPrefixField, DefaultMiscExcludeAddrPrefix, "Miners with this value as address prefix is considered as valid")
	viper.BindPFlag(ytrebuilder.MiscExcludeAddrPrefixField, rootCmd.PersistentFlags().Lookup(ytrebuilder.MiscExcludeAddrPrefixField))
}
