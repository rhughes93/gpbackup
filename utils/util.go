package utils

/*
 * This file contains miscellaneous functions that are generally useful and
 * don't fit into any other file.
 */

import (
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"strings"
)

/*
 * Abort() is for handling critical errors.  It panic()s to unwind the call stack
 * until the panic is caught by the recover() in DoTeardown() in backup.go, at
 * which point any necessary cleanup is performed.
 *
 * log.Fatal() calls Abort() after logging its arguments, so generally that function
 * should be used instead of calling Abort() directly.
 */

func Abort(output ...interface{}) {
	errStr := ""
	if len(output) > 0 {
		errStr = fmt.Sprintf("%v", output[0])
		if len(output) > 1 {
			errStr = fmt.Sprintf(errStr, output[1:]...)
		}
	}
	panic(errStr)
}

func CheckError(err error) {
	if err != nil {
		logger.Fatal(err, "")
	}
}

/*
 * General helper functions
 */

func CurrentTimestamp() string {
	return System.Now().Format("20060102150405")
}

func TryEnv(varname string, defval string) string {
	val := System.Getenv(varname)
	if val == "" {
		return defval
	}
	return val
}

// Dollar-quoting logic is based on appendStringLiteralDQ() in pg_dump.
func DollarQuoteString(literal string) string {
	delimStr := "_XXXXXXX"
	quoteStr := ""
	for i := range delimStr {
		testStr := "$" + delimStr[0:i]
		if !strings.Contains(literal, testStr) {
			quoteStr = testStr + "$"
			break
		}
	}
	return quoteStr + literal + quoteStr
}

// This function assumes that all identifiers are already appropriately quoted
func MakeFQN(schema string, object string) string {
	return fmt.Sprintf("%s.%s", schema, object)
}

func ValidateFQNs(fqns []string) {
	unquotedIdentString := "[a-z_][a-z0-9_]*"
	validIdentString := fmt.Sprintf("(?:\"(.*)\"|(%s))", unquotedIdentString)
	validFormat := regexp.MustCompile(fmt.Sprintf(`^%s\.%s$`, validIdentString, validIdentString))
	var matches []string
	for _, fqn := range fqns {
		if matches = validFormat.FindStringSubmatch(fqn); len(matches) == 0 {
			logger.Fatal(errors.Errorf(`Table %s is not correctly fully-qualified.  Please ensure that it is in the format schema.table, it is quoted appropriately, and it has no preceding or trailing whitespace.`, fqn), "")
		}
	}
}
