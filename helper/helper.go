package helper

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/greenplum-db/gpbackup/utils"
)

var (
	agent    *bool
	content  *int
	dataFile *string
	logger   *utils.Logger
	oid      *uint
	pipeFile *string
	prevOid  *uint
	restore  *bool
	tocFile  *string
)

/*
 * Shared functions
 */

func DoHelper() {
	InitializeGlobals()
	if *agent {
		fmt.Println("agent")
		doAgent()
	} else if *restore {
		doRestoreHelper()
	} else {
		doBackupHelper()
	}
}

func InitializeGlobals() {
	agent = flag.Bool("agent", false, "Use gpbackup_helper as an agent")
	content = flag.Int("content", -2, "Content ID of the corresponding segment")
	logger = utils.InitializeLogging("gpbackup_helper", "")
	oid = flag.Uint("oid", 0, "Oid of the table being processed")
	prevOid = flag.Uint("previous-oid", 0, "Oid of the previous table restored")
	restore = flag.Bool("restore", false, "Read in table according to offset in table of contents file")
	tocFile = flag.String("toc-file", "", "Absolute path to the table of contents file")
	pipeFile = flag.String("pipe-file", "", "Absolute path to the pipe file")
	dataFile = flag.String("data-file", "", "Absolute path to the data file")
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
 * Backup helper functions
 */

func checkerror(total int, err error) {
	if err != nil && !strings.Contains(err.Error(), "broken pipe") {
		fmt.Println("Read", total, "bytes")
		fmt.Println(err)
		os.Exit(1)
	}
}

func doAgent() {
	toc := utils.NewSegmentTOC(*tocFile)
	byteRanges := GetOrderedOidBounds(toc)
	lastByte := uint64(0)

	readHandle, err := os.Open(*dataFile)
	utils.CheckError(err)
	reader := bufio.NewReader(readHandle)

	for _, byteRange := range byteRanges {
		log("table start")
		writeHandle, err := os.OpenFile(*pipeFile, os.O_WRONLY, os.ModeNamedPipe)
		utils.CheckError(err)
		writer := bufio.NewWriter(writeHandle)

		start := byteRange.StartByte
		end := byteRange.EndByte
		log(fmt.Sprintf("bounds: start %d end %d lastByte %d", start, end, lastByte))
		reader.Discard(int(start - lastByte))
		log(fmt.Sprintf("discarded %d", start-lastByte))

		numBytes, err := io.CopyN(writer, reader, int64(end-start))
		utils.CheckError(err)
		err = writer.Flush()
		utils.CheckError(err)
		log(fmt.Sprintf("attempted to read %d, actually read %d", end-start, numBytes))
		err = writeHandle.Close()
		utils.CheckError(err)
		lastByte = end
		time.Sleep(100 * time.Millisecond)
	}
}

func GetOrderedOidBounds(toc *utils.SegmentTOC) []utils.SegmentDataEntry {
	oids := make([]int, 0)
	entries := make([]utils.SegmentDataEntry, len(toc.DataEntries))
	for oid := range toc.DataEntries {
		oids = append(oids, int(oid))
	}
	sort.Ints(oids)
	fmt.Println(oids)
	for i, oid := range oids {
		entries[i] = toc.DataEntries[uint(oid)]
	}
	return entries
}

/*
 * Restore helper functions
 */

func doRestoreHelper() {
	toc := utils.NewSegmentTOC(*tocFile)
	startByte, endByte := GetBoundsForTable(toc)
	lastByteRead := toc.DataEntries[*prevOid].EndByte
	CopyByteRange(startByte, endByte, lastByteRead)
}

func GetBoundsForTable(toc *utils.SegmentTOC) (int64, int64) {
	segmentDataEntry := toc.DataEntries[*oid]
	startByte := int64(segmentDataEntry.StartByte)
	endByte := int64(segmentDataEntry.EndByte)
	return startByte, endByte
}

func CopyByteRange(startByte int64, endByte int64, lastByteRead uint64) {
	discard := int(startByte - int64(lastByteRead))
	count := endByte - startByte
	log("Copying bytes for table with oid %d; discarding next %d bytes, copying %d bytes", *oid, discard, count)
	/*
	 * We shell out to dd here instead of using io.CopyN because it was closing
	 * the input pipe and discarding the rest of the input, instead of leaving
	 * it alone for later reads as it should have.  As we were unable to figure
	 * out why CopyN was exhibiting this behavior, we're using dd instead.
	 */
	cmd := exec.Command("dd", fmt.Sprintf("skip=%d", discard), fmt.Sprintf("count=%d", count), "bs=1")
	cmd.Stdin = utils.System.Stdin
	cmd.Stdout = utils.System.Stdout
	err := cmd.Run()
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
