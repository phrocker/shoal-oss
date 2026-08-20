package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../capi/include
#include "bridge.h"
*/
import "C"

import (
	"errors"

	"github.com/phrocker/shoal/accumulo"
)

//export shoal_logging_set_level
func shoal_logging_set_level(
	level C.shoal_log_level,
	outError **C.shoal_error,
) (status C.shoal_status) {
	clearError(outError)
	defer recoverStatus(&status, outError)
	if level < C.SHOAL_LOG_LEVEL_OFF || level > C.SHOAL_LOG_LEVEL_TRACE {
		return fail(outError, C.SHOAL_STATUS_INVALID_ARGUMENT, errors.New("shoal: invalid log level"))
	}
	accumulo.SetLogLevel(accumulo.LogLevel(level))
	return C.SHOAL_STATUS_OK
}

//export shoal_logging_get_level
func shoal_logging_get_level() C.shoal_log_level {
	return C.shoal_log_level(accumulo.CurrentLogLevel())
}
