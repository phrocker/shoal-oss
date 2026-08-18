#ifndef SHOAL_H
#define SHOAL_H

#include "shoal_types.h"

#ifdef __cplusplus
extern "C" {
#endif

SHOAL_API uint32_t SHOAL_CALL shoal_abi_version(void);

SHOAL_API void SHOAL_CALL
shoal_connector_config_init(shoal_connector_config *config);

SHOAL_API void SHOAL_CALL
shoal_scanner_config_init(shoal_scanner_config *config);

SHOAL_API void SHOAL_CALL shoal_range_init(shoal_range *range);

SHOAL_API void SHOAL_CALL
shoal_batch_writer_config_init(shoal_batch_writer_config *config);

/*
 * Creates a connector and stores its owned handle in out_connector.
 *
 * out_connector is set to NULL before validation. out_error may be NULL; when
 * supplied, it is set to NULL on success or to an owned error on failure.
 * Initialize both output variables to NULL before calling.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_create(const shoal_connector_config *config,
                       shoal_connector **out_connector,
                       shoal_error **out_error);

/*
 * Closes connector-owned transports and bootstrap resources. Close is
 * idempotent while the handle remains alive.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_close(shoal_connector *connector, shoal_error **out_error);

/*
 * Closes and releases an owned connector handle, then sets *connector to NULL.
 * Passing NULL or a pointer whose value is NULL is a no-op. Call close first
 * when its error must be observed.
 */
SHOAL_API void SHOAL_CALL shoal_connector_free(shoal_connector **connector);

/*
 * Creates a scanner. The configuration and all nested values are copied.
 * Scanner handles remain valid after connector free, but operations then fail
 * with CLOSED because the underlying connector has been shut down.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_create_scanner(shoal_connector *connector,
                               const shoal_scanner_config *config,
                               shoal_scanner **out_scanner,
                               shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_create_batch_scanner(shoal_connector *connector,
                                     const shoal_scanner_config *config,
                                     shoal_batch_scanner **out_scanner,
                                     shoal_error **out_error);

/*
 * Close is idempotent and cancels then joins in-flight calls. Concurrent scan
 * and close calls are supported. Free/use of the same handle concurrently is
 * not supported.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_scanner_close(shoal_scanner *scanner, shoal_error **out_error);

SHOAL_API void SHOAL_CALL shoal_scanner_free(shoal_scanner **scanner);

SHOAL_API shoal_status SHOAL_CALL
shoal_batch_scanner_close(shoal_batch_scanner *scanner,
                          shoal_error **out_error);

SHOAL_API void SHOAL_CALL
shoal_batch_scanner_free(shoal_batch_scanner **scanner);

/*
 * timeout_ms is zero for no deadline and must not be negative. On any call
 * that produced entries, out_result may be non-NULL even when the status is a
 * failure (for example CLEANUP_FAILED or an error after partial batch results).
 * The caller owns every non-NULL result and must free it.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_scanner_scan(shoal_scanner *scanner, const shoal_range *range,
                   int64_t timeout_ms, shoal_scan_result **out_result,
                   shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_batch_scanner_scan(shoal_batch_scanner *scanner,
                         const shoal_range *ranges, size_t range_count,
                         int64_t timeout_ms, shoal_scan_result **out_result,
                         shoal_error **out_error);

SHOAL_API size_t SHOAL_CALL
shoal_scan_result_count(const shoal_scan_result *result);

SHOAL_API shoal_status SHOAL_CALL
shoal_scan_result_get(const shoal_scan_result *result, size_t index,
                      shoal_key_value_view *out_value,
                      shoal_error **out_error);

SHOAL_API void SHOAL_CALL
shoal_scan_result_free(shoal_scan_result **result);

/*
 * Mutation inputs are copied before each call returns. A mutation may be
 * reused or freed immediately after a successful BatchWriter add.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_mutation_create(shoal_bytes row, shoal_mutation **out_mutation,
                      shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_mutation_put(shoal_mutation *mutation, shoal_bytes column_family,
                   shoal_bytes column_qualifier,
                   shoal_bytes column_visibility, int64_t timestamp,
                   shoal_bytes value, shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_mutation_put_latest(shoal_mutation *mutation, shoal_bytes column_family,
                          shoal_bytes column_qualifier,
                          shoal_bytes column_visibility, shoal_bytes value,
                          shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_mutation_delete(shoal_mutation *mutation, shoal_bytes column_family,
                      shoal_bytes column_qualifier,
                      shoal_bytes column_visibility, int64_t timestamp,
                      shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_mutation_delete_latest(shoal_mutation *mutation,
                             shoal_bytes column_family,
                             shoal_bytes column_qualifier,
                             shoal_bytes column_visibility,
                             shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_mutation_size(const shoal_mutation *mutation, size_t *out_size,
                    shoal_error **out_error);

SHOAL_API void SHOAL_CALL shoal_mutation_free(shoal_mutation **mutation);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_create_batch_writer(
    shoal_connector *connector, const shoal_batch_writer_config *config,
    shoal_batch_writer **out_writer, shoal_error **out_error);

/*
 * timeout_ms is zero for no deadline and must not be negative. out_failure is
 * optional and receives owned structured details when the operation reaches
 * the write path. Free every non-NULL failure object.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_batch_writer_add(shoal_batch_writer *writer,
                       const shoal_mutation *mutation, int64_t timeout_ms,
                       shoal_write_failure **out_failure,
                       shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_batch_writer_flush(shoal_batch_writer *writer, int64_t timeout_ms,
                         shoal_write_failure **out_failure,
                         shoal_error **out_error);

/*
 * Close is idempotent, prevents new operations, cancels and joins in-flight
 * calls, then flushes remaining mutations. A timed-out close may be retried.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_batch_writer_close(shoal_batch_writer *writer, int64_t timeout_ms,
                         shoal_write_failure **out_failure,
                         shoal_error **out_error);

SHOAL_API void SHOAL_CALL
shoal_batch_writer_free(shoal_batch_writer **writer);

SHOAL_API shoal_write_failure_flags SHOAL_CALL
shoal_write_failure_get_flags(const shoal_write_failure *failure);

SHOAL_API size_t SHOAL_CALL
shoal_write_failure_failed_extent_count(const shoal_write_failure *failure);

SHOAL_API shoal_status SHOAL_CALL
shoal_write_failure_get_failed_extent(
    const shoal_write_failure *failure, size_t index,
    shoal_failed_extent_view *out_extent, shoal_error **out_error);

SHOAL_API size_t SHOAL_CALL shoal_write_failure_constraint_count(
    const shoal_write_failure *failure);

SHOAL_API shoal_status SHOAL_CALL
shoal_write_failure_get_constraint(
    const shoal_write_failure *failure, size_t index,
    shoal_constraint_violation_view *out_violation, shoal_error **out_error);

SHOAL_API size_t SHOAL_CALL shoal_write_failure_authorization_count(
    const shoal_write_failure *failure);

SHOAL_API shoal_status SHOAL_CALL
shoal_write_failure_get_authorization(
    const shoal_write_failure *failure, size_t index,
    shoal_authorization_failure_view *out_failure, shoal_error **out_error);

SHOAL_API size_t SHOAL_CALL
shoal_write_failure_cleanup_count(const shoal_write_failure *failure);

SHOAL_API shoal_status SHOAL_CALL
shoal_write_failure_get_cleanup(const shoal_write_failure *failure,
                                size_t index,
                                shoal_cleanup_failure_view *out_failure,
                                shoal_error **out_error);

SHOAL_API void SHOAL_CALL
shoal_write_failure_free(shoal_write_failure **failure);

/* Returns the stable status stored in error, or INVALID_ARGUMENT for NULL. */
SHOAL_API shoal_status SHOAL_CALL shoal_error_code(const shoal_error *error);

/*
 * Returns a borrowed, NUL-terminated message. It remains valid until error is
 * freed. The returned bytes are read-only and must not be freed directly.
 */
SHOAL_API const char *SHOAL_CALL
shoal_error_message(const shoal_error *error);

/*
 * Releases an owned error and sets *error to NULL. Passing NULL or a pointer
 * whose value is NULL is a no-op.
 */
SHOAL_API void SHOAL_CALL shoal_error_free(shoal_error **error);

#ifdef __cplusplus
}
#endif

#endif
