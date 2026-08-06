#ifndef SHOAL_H
#define SHOAL_H

#include "shoal_types.h"

#ifdef __cplusplus
extern "C" {
#endif

SHOAL_API uint32_t SHOAL_CALL shoal_abi_version(void);

SHOAL_API void SHOAL_CALL
shoal_connector_config_init(shoal_connector_config *config);

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
