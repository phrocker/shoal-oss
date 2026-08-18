package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../capi/include
#include "bridge.h"
*/
import "C"

import "errors"

func testErrorMessageAllocFailAfter(successfulAllocations uint) {
	C.shoal_bridge_test_error_message_alloc_fail_after(C.size_t(successfulAllocations))
}

func testErrorMessageAllocReset() {
	C.shoal_bridge_test_error_message_alloc_reset()
}

func testStringAllocFailAfter(successfulAllocations uint) {
	C.shoal_bridge_test_string_alloc_fail_after(C.size_t(successfulAllocations))
}

func testStringAllocReset() {
	C.shoal_bridge_test_string_alloc_reset()
}

func testFailStatusWhenErrorMessageAllocationFails(message string) (status int32, outErrorNil bool) {
	var outError *C.shoal_error
	result := fail(&outError, C.SHOAL_STATUS_INVALID_ARGUMENT, errors.New(message))
	if outError != nil {
		C.shoal_bridge_error_free(outError)
		return int32(result), false
	}
	return int32(result), true
}

func testAllocateWriteFailureReturnedNil(data writeFailureData) bool {
	failure := allocateWriteFailure(data)
	if failure != nil {
		C.shoal_bridge_write_failure_free(failure)
		return false
	}
	return true
}
