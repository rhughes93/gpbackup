package restore

/*
 * This file contains structs and functions related to backing up data on the segments.
 */

import (
	"fmt"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	tableDelim = ","
)

func CopyTableIn(connection *utils.DBConn, tableName string, tableAttributes string, backupFile string, singleDataFile bool, oid uint32, prevOid uint32) {
	usingCompression, compressionProgram := utils.GetCompressionParameters()
	tocFile := globalCluster.GetSegmentTOCFilePath("<SEG_DATA_DIR>", "<SEGID>")
	copyCommand := ""
	if singleDataFile {
		helperCommand := fmt.Sprintf("$GPHOME/bin/gpbackup_helper --restore --toc-file=%s --oid=%d --previous-oid=%d --content=<SEGID>", tocFile, oid, prevOid)
		copyCommand = fmt.Sprintf("PROGRAM '%s < %s'", helperCommand, backupFile)
	} else if usingCompression {
		copyCommand = fmt.Sprintf("PROGRAM '%s < %s'", compressionProgram.DecompressCommand, backupFile)
	} else {
		copyCommand = fmt.Sprintf("'%s'", backupFile)
	}
	query := fmt.Sprintf("COPY %s%s FROM %s WITH CSV DELIMITER '%s' ON SEGMENT;", tableName, tableAttributes, copyCommand, tableDelim)
	_, err := connection.Exec(query)
	if err != nil {
		logger.Fatal(err, "Error loading data into table %s", tableName)
	}
}
