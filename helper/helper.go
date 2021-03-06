package helper

import (
	"bufio"
	"flag"
	"fmt"
	"io"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	content *int
	logger  *utils.Logger
	oid     *uint
	restore *bool
	tocFile *string
)

/*
 * Shared functions
 */

func DoHelper() {
	InitializeGlobals()
	if *restore {
		doRestoreHelper()
	} else {
		doBackupHelper()
	}
}

func InitializeGlobals() {
	content = flag.Int("content", -2, "Content ID of the corresponding segment")
	logger = utils.InitializeLogging("gpbackup_helper", "")
	oid = flag.Uint("oid", 0, "Oid of the table being processed")
	restore = flag.Bool("restore", false, "Read in table according to offset in table of contents file")
	tocFile = flag.String("toc-file", "", "Absolute path to the table of contents file")
	flag.Parse()
	utils.InitializeSystemFunctions()
}

func SetContent(id int) {
	content = &id
}

func SetFilename(name string) {
	tocFile = &name
}

func SetLogger(log *utils.Logger) {
	logger = log
}

func SetOid(newoid uint) {
	oid = &newoid
}

/*
 * Backup helper functions
 */

func doBackupHelper() {
	toc, lastRead := ReadOrCreateTOC()
	numBytes := ReadAndCountBytes()
	lastProcessed := lastRead + numBytes
	toc.AddSegmentDataEntry(*oid, lastRead, lastProcessed)
	toc.LastByteRead = lastProcessed
	toc.WriteToFile(*tocFile)
}

func ReadOrCreateTOC() (*utils.SegmentTOC, uint64) {
	var toc *utils.SegmentTOC
	var lastRead uint64
	if utils.FileExistsAndIsReadable(*tocFile) {
		toc = utils.NewSegmentTOC(*tocFile)
		lastRead = toc.LastByteRead
	} else {
		toc = &utils.SegmentTOC{}
		toc.DataEntries = make(map[uint]utils.SegmentDataEntry, 1)
		lastRead = 0
	}
	return toc, lastRead
}

func ReadAndCountBytes() uint64 {
	reader := bufio.NewReader(utils.System.Stdin)
	numBytes, _ := io.Copy(utils.System.Stdout, reader)
	return uint64(numBytes)
}

/*
 * Restore helper functions
 */

func doRestoreHelper() {
	toc := utils.NewSegmentTOC(*tocFile)
	startByte, endByte := GetBoundsForTable(toc)
	CopyByteRange(startByte, endByte)
}

func GetBoundsForTable(toc *utils.SegmentTOC) (int64, int64) {
	segmentDataEntry := toc.DataEntries[*oid]
	startByte := int64(segmentDataEntry.StartByte)
	endByte := int64(segmentDataEntry.EndByte)
	return startByte, endByte
}

func CopyByteRange(startByte int64, endByte int64) {
	reader := bufio.NewReader(utils.System.Stdin)
	discard := int(startByte)
	count := endByte - startByte
	log("Copying bytes for table with oid %d; discarding next %d bytes, copying %d bytes", *oid, discard, count)
	reader.Discard(discard)
	_, err := io.CopyN(utils.System.Stdout, reader, count)

	log("Finished copying bytes for table with oid %d", *oid)
	if err != nil {
		logger.Fatal(nil, "Segment %d: Error copying table with oid %d: %s", *content, *oid, err.Error())
	}

}

/*
 * Shared helper functions
 */

func log(s string, v ...interface{}) {
	s = fmt.Sprintf("Segment %d: %s", *content, s)
	logger.Verbose(s, v...)
}
