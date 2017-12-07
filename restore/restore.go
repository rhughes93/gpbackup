package restore

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/greenplum-db/gpbackup/utils"

	"github.com/pkg/errors"
)

/*
 * We define and initialize flags separately to avoid import conflicts in tests.
 * The flag variables, and setter functions for them, are in global_variables.go.
 */
func initializeFlags() {
	backupDir = flag.String("backupdir", "", "The absolute path of the directory in which the backup files to be restored are located")
	createdb = flag.Bool("createdb", false, "Create the database before metadata restore")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	flag.Var(&includeSchemas, "include-schema", "Restore only the specified schema(s). --include-schema can be specified multiple times.")
	includeTableFile = flag.String("include-table-file", "", "A file containing a list of fully-qualified tables to be restored")
	numJobs = flag.Int("jobs", 1, "Number of parallel connections to use when restoring table data")
	onErrorContinue = flag.Bool("on-error-continue", false, "Log errors and continue restore, instead of exiting on first error")
	printVersion = flag.Bool("version", false, "Print version number and exit")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	redirect = flag.String("redirect", "", "Restore to the specified database instead of the database that was backed up")
	restoreGlobals = flag.Bool("globals", false, "Restore global metadata")
	timestamp = flag.String("timestamp", "", "The timestamp to be restored, in the format YYYYMMDDHHMMSS")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
	withStats = flag.Bool("with-stats", false, "Restore query plan statistics")
}

// This function handles setup that can be done before parsing flags.
func DoInit() {
	SetLogger(utils.InitializeLogging("gprestore", ""))
	initializeFlags()
}

/*
* This function handles argument parsing and validation, e.g. checking that a passed filename exists.
* It should only validate; initialization with any sort of side effects should go in DoInit or DoSetup.
 */
func DoValidation() {
	if len(os.Args) == 1 {
		flag.PrintDefaults()
		os.Exit(0)
	}
	flag.Parse()
	if *printVersion {
		fmt.Printf("gprestore %s\n", version)
		os.Exit(0)
	}
	ValidateFlagCombinations()
	utils.ValidateBackupDir(*backupDir)
	if !utils.IsValidTimestamp(*timestamp) {
		logger.Fatal(errors.Errorf("Timestamp %s is invalid.  Timestamps must be in the format YYYYMMDDHHMMSS.", *timestamp), "")
	}
	logger.Info("Restore Key = %s", *timestamp)
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()

	InitializeConnection("postgres")
	DoPostgresValidation()
	if *createdb {
		createDatabase()
	}
	ConnectToRestoreDatabase()

	if *restoreGlobals {
		restoreGlobal()
	}

	/*
	 * We don't need to validate anything if we're creating the database; we
	 * should not error out for validation reasons once the restore database exists.
	 */
	if !*createdb {
		DoRestoreDatabaseValidation()
	}
	setSerialRestore()
}

func DoRestore() {
	if !backupConfig.DataOnly {
		restorePredata()
	}

	if !backupConfig.MetadataOnly {
		backupFileCount := 2 // 1 for the actual data file, 1 for the segment TOC file
		if !backupConfig.SingleDataFile {
			backupFileCount = len(globalTOC.DataEntries)
		}
		globalCluster.VerifyBackupFileCountOnSegments(backupFileCount)
		restoreData()
	}

	if !backupConfig.DataOnly && !backupConfig.TableFiltered {
		restorePostdata()
	}

	if *withStats && backupConfig.WithStatistics {
		restoreStatistics()
	}
}

func createDatabase() {
	objectTypes := []string{"SESSION GUCS", "GPDB4 SESSION GUCS", "DATABASE GUC", "DATABASE", "DATABASE METADATA"}
	globalFilename := globalCluster.GetGlobalFilePath()
	logger.Info("Creating database")
	statements := GetRestoreMetadataStatements(globalFilename, objectTypes, []string{}, []string{})
	if *redirect != "" {
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, *redirect)
	}
	ExecuteRestoreMetadataStatements(statements, "", false)
	logger.Info("Database creation complete")
}

func restoreGlobal() {
	objectTypes := []string{"SESSION GUCS", "GPDB4 SESSION GUCS", "DATABASE GUC", "DATABASE METADATA", "RESOURCE QUEUE", "RESOURCE GROUP", "ROLE", "ROLE GRANT", "TABLESPACE"}
	globalFilename := globalCluster.GetGlobalFilePath()
	logger.Info("Restoring global metadata from %s", globalCluster.GetGlobalFilePath())
	statements := GetRestoreMetadataStatements(globalFilename, objectTypes, []string{}, []string{})
	if *redirect != "" {
		statements = utils.SubstituteRedirectDatabaseInStatements(statements, backupConfig.DatabaseName, *redirect)
	}
	setGUCsForConnection()
	ExecuteRestoreMetadataStatements(statements, "Global objects", false)
	logger.Info("Global database metadata restore complete")
}

func restorePredata() {
	predataFilename := globalCluster.GetPredataFilePath()
	logger.Info("Restoring pre-data metadata from %s", predataFilename)
	statements := GetRestoreMetadataStatements(predataFilename, []string{}, includeSchemas, includeTables)
	setGUCsForConnection()
	ExecuteRestoreMetadataStatements(statements, "Pre-data objects", true)
	logger.Info("Pre-data metadata restore complete")
}

func restoreData() {
	if backupConfig.SingleDataFile {
		globalCluster.CopySegmentTOCs()
		defer globalCluster.CleanUpSegmentTOCs()
		globalCluster.CreateTablePipesOnAllHosts()
		defer globalCluster.CleanUpTablePipesOnAllHosts()
		globalCluster.WriteToSegmentPipes()
		defer globalCluster.CleanUpSegmentTailProcesses()
	}
	setParallelRestore()
	defer setSerialRestore()
	logger.Info("Restoring data")

	filteredMasterDataEntries := globalTOC.GetDataEntriesMatching(includeSchemas, includeTables)
	totalTables := len(filteredMasterDataEntries)
	dataProgressBar := utils.NewProgressBar(totalTables, "Tables restored: ", logger.GetVerbosity() == utils.LOGINFO)
	dataProgressBar.Start()

	if *numJobs == 1 {
		setGUCsForConnection()
		prevEntryOid := uint32(0)
		for i, entry := range filteredMasterDataEntries {
			restoreSingleTableData(entry, uint32(i)+1, totalTables, prevEntryOid)
			dataProgressBar.Increment()
			prevEntryOid = entry.Oid
		}
	} else {
		var tableNum uint32 = 1
		tasks := make(chan utils.MasterDataEntry, totalTables)
		var workerPool sync.WaitGroup
		for i := 0; i < *numJobs; i++ {
			workerPool.Add(1)
			go func() {
				setGUCsForConnection()
				for entry := range tasks {
					restoreSingleTableData(entry, tableNum, totalTables, 0)
					atomic.AddUint32(&tableNum, 1)
					dataProgressBar.Increment()
				}
				workerPool.Done()
			}()
		}
		for _, entry := range filteredMasterDataEntries {
			tasks <- entry
		}
		close(tasks)
		workerPool.Wait()
	}
	dataProgressBar.Finish()
	logger.Info("Data restore complete")
}

func restorePostdata() {
	postdataFilename := globalCluster.GetPostdataFilePath()
	logger.Info("Restoring post-data metadata from %s", postdataFilename)
	statements := GetRestoreMetadataStatements(postdataFilename, []string{}, includeSchemas, []string{})
	setGUCsForConnection()
	ExecuteRestoreMetadataStatements(statements, "Post-data objects", true)
	logger.Info("Post-data metadata restore complete")
}

func restoreStatistics() {
	statisticsFilename := globalCluster.GetStatisticsFilePath()
	logger.Info("Restoring query planner statistics from %s", statisticsFilename)
	statements := GetRestoreMetadataStatements(statisticsFilename, []string{}, includeSchemas, []string{})
	ExecuteRestoreMetadataStatements(statements, "Table statistics", false)
	logger.Info("Query planner statistics restore complete")
}

func DoTeardown() {
	errStr := ""
	if err := recover(); err != nil {
		errStr = fmt.Sprintf("%v", err)
		if connection != nil {
			if strings.Contains(errStr, fmt.Sprintf(`Database "%s" does not exist`, connection.DBName)) {
				errStr = fmt.Sprintf(`%s.  Use the --createdb flag to create "%s" as part of the restore process.`, errStr, connection.DBName)
			} else if strings.Contains(errStr, fmt.Sprintf(`Database "%s" already exists`, connection.DBName)) {
				errStr = fmt.Sprintf(`%s.  Run gprestore again without the --createdb flag.`, errStr)
			}
		}
		fmt.Println(errStr)
	}
	_, exitCode := utils.ParseErrorMessage(errStr)
	if connection != nil {
		connection.Close()
	}

	os.Exit(exitCode)
}
