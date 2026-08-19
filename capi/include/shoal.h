#ifndef SHOAL_H
#define SHOAL_H

#include "shoal_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Legacy compatibility-major query preserved for existing callers. Use the
 * version and capability queries below when negotiating additive features.
 */
SHOAL_API uint32_t SHOAL_CALL shoal_abi_version(void);

/*
 * These queries are deterministic, allocation-free, thread-safe, and valid
 * before any connector or other handle is created. When a caller may run
 * against an older library, every additive symbol not guaranteed by that
 * library must be dynamically resolved before use. Capability checks govern
 * feature availability but do not make hard symbol references load-safe.
 */
SHOAL_API uint32_t SHOAL_CALL shoal_abi_version_major(void);
SHOAL_API uint32_t SHOAL_CALL shoal_abi_version_minor(void);
SHOAL_API uint32_t SHOAL_CALL shoal_abi_version_patch(void);
SHOAL_API uint32_t SHOAL_CALL shoal_abi_version_packed(void);

/*
 * Capability identifiers are append-only. word_count reports how many 64-bit
 * words the current library uses. shoal_abi_capability_word() returns 0 for
 * word_index values beyond that range. shoal_abi_has_capability() returns 1
 * for supported capabilities and 0 for unsupported or unknown identifiers.
 */
SHOAL_API uint32_t SHOAL_CALL shoal_abi_capability_count(void);
SHOAL_API uint32_t SHOAL_CALL shoal_abi_capability_word_count(void);
SHOAL_API shoal_abi_capability_bits SHOAL_CALL
shoal_abi_capability_word(uint32_t word_index);
SHOAL_API uint8_t SHOAL_CALL
shoal_abi_has_capability(shoal_abi_capability_id capability_id);

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
 * idempotent while the handle remains alive, cancels active identity, table,
 * namespace, security, and split-administration calls, and waits for those
 * plus any in-flight scanner or batch-scanner calls to finish before tearing
 * down connector-owned resources.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_close(shoal_connector *connector, shoal_error **out_error);

/*
 * Closes and releases an owned connector handle, then sets *connector to NULL.
 * Passing NULL or a pointer whose value is NULL is a no-op. Call close first
 * when its error must be observed. Any scanner or batch-scanner created from
 * this connector permanently rejects new operations as soon as close/free
 * starts, while in-flight scan calls are allowed to finish before final
 * teardown.
 */
SHOAL_API void SHOAL_CALL shoal_connector_free(shoal_connector **connector);

/*
 * Returns the immutable instance name, instance ID, and authenticated
 * principal captured when the connector was created. timeout_ms is zero for
 * no deadline and must not be negative. The result owns all three strings.
 * Initialize out_identity with shoal_connector_identity_view_init before
 * calling shoal_connector_identity_get. Pointers in the view are borrowed
 * from the result and remain valid until the result is freed.
 */
SHOAL_API void SHOAL_CALL
shoal_connector_identity_view_init(shoal_connector_identity_view *view);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_get_identity(shoal_connector *connector, int64_t timeout_ms,
                             shoal_connector_identity_result **out_result,
                             shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_identity_get(const shoal_connector_identity_result *result,
                             shoal_connector_identity_view *out_identity,
                             shoal_error **out_error);

SHOAL_API void SHOAL_CALL
shoal_connector_identity_free(shoal_connector_identity_result **result);

/*
 * timeout_ms is zero for no deadline and must not be negative. The returned
 * list owns every table name/ID pair and stays valid until freed. Entries are
 * sorted by qualified table name.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_list_tables(shoal_connector *connector, int64_t timeout_ms,
                            shoal_table_list_result **out_result,
                            shoal_error **out_error);

SHOAL_API size_t SHOAL_CALL
shoal_table_list_count(const shoal_table_list_result *result);

SHOAL_API shoal_status SHOAL_CALL
shoal_table_list_get(const shoal_table_list_result *result, size_t index,
                     shoal_table_view *out_table, shoal_error **out_error);

SHOAL_API void SHOAL_CALL
shoal_table_list_free(shoal_table_list_result **result);

/*
 * timeout_ms is zero for no deadline and must not be negative. out_exists is
 * set to 0 before validation and to 1 only when the table is present.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_table_exists(shoal_connector *connector,
                             const char *table_name, int64_t timeout_ms,
                             uint8_t *out_exists, shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_create_table(shoal_connector *connector,
                             const char *table_name, int64_t timeout_ms,
                             shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_delete_table(shoal_connector *connector,
                             const char *table_name, int64_t timeout_ms,
                             shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_rename_table(shoal_connector *connector,
                             const char *table_name,
                             const char *new_table_name, int64_t timeout_ms,
                             shoal_error **out_error);

/*
 * wait must be 0 or 1. When set, the call waits until Accumulo reports the
 * full-table flush complete or the operation deadline expires.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_flush_table(shoal_connector *connector,
                            const char *table_name, uint8_t wait,
                            int64_t timeout_ms, shoal_error **out_error);

/*
 * property_value is required but may be the empty string; use
 * shoal_connector_remove_table_property to remove a property entirely.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_set_table_property(shoal_connector *connector,
                                   const char *table_name,
                                   const char *property_name,
                                   const char *property_value,
                                   int64_t timeout_ms,
                                   shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_remove_table_property(shoal_connector *connector,
                                      const char *table_name,
                                      const char *property_name,
                                      int64_t timeout_ms,
                                      shoal_error **out_error);

/*
 * timeout_ms is zero for no deadline and must not be negative. The returned
 * key/value pairs own their storage, are sorted by property key, and preserve
 * explicit empty string values until freed.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_effective_table_properties(
    shoal_connector *connector, const char *table_name, int64_t timeout_ms,
    shoal_table_properties_result **out_result, shoal_error **out_error);

SHOAL_API size_t SHOAL_CALL
shoal_table_properties_count(const shoal_table_properties_result *result);

SHOAL_API shoal_status SHOAL_CALL
shoal_table_properties_get(const shoal_table_properties_result *result,
                           size_t index, shoal_table_property_view *out_entry,
                           shoal_error **out_error);

SHOAL_API void SHOAL_CALL
shoal_table_properties_free(shoal_table_properties_result **result);

/* Namespace names and IDs are owned by the result and sorted by name. */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_list_namespaces(shoal_connector *connector, int64_t timeout_ms,
                                shoal_namespace_list_result **out_result,
                                shoal_error **out_error);
SHOAL_API size_t SHOAL_CALL
shoal_namespace_list_count(const shoal_namespace_list_result *result);
SHOAL_API shoal_status SHOAL_CALL
shoal_namespace_list_get(const shoal_namespace_list_result *result,
                         size_t index, shoal_namespace_view *out_namespace,
                         shoal_error **out_error);
SHOAL_API void SHOAL_CALL
shoal_namespace_list_free(shoal_namespace_list_result **result);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_namespace_exists(shoal_connector *connector,
                                 const char *namespace_name,
                                 int64_t timeout_ms, uint8_t *out_exists,
                                 shoal_error **out_error);
/*
 * namespace_name must be non-NULL. An empty namespace_name identifies
 * Accumulo's default namespace for existence, deletion, rename-source,
 * permission, and property operations. Creation and rename destinations
 * require non-empty names.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_create_namespace(shoal_connector *connector,
                                 const char *namespace_name,
                                 int64_t timeout_ms, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_delete_namespace(shoal_connector *connector,
                                 const char *namespace_name,
                                 int64_t timeout_ms, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_rename_namespace(shoal_connector *connector,
                                 const char *namespace_name,
                                 const char *new_namespace_name,
                                 int64_t timeout_ms, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_set_namespace_property(
    shoal_connector *connector, const char *namespace_name,
    const char *property_name, const char *property_value, int64_t timeout_ms,
    shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_remove_namespace_property(
    shoal_connector *connector, const char *namespace_name,
    const char *property_name, int64_t timeout_ms, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_effective_namespace_properties(
    shoal_connector *connector, const char *namespace_name,
    int64_t timeout_ms, shoal_namespace_properties_result **out_result,
    shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_namespace_properties(
    shoal_connector *connector, const char *namespace_name,
    int64_t timeout_ms, shoal_namespace_properties_result **out_result,
    shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_versioned_namespace_properties(
    shoal_connector *connector, const char *namespace_name,
    int64_t timeout_ms, shoal_versioned_properties_result **out_result,
    shoal_error **out_error);
SHOAL_API size_t SHOAL_CALL
shoal_namespace_properties_count(
    const shoal_namespace_properties_result *result);
SHOAL_API shoal_status SHOAL_CALL
shoal_namespace_properties_get(
    const shoal_namespace_properties_result *result, size_t index,
    shoal_table_property_view *out_entry, shoal_error **out_error);
SHOAL_API void SHOAL_CALL
shoal_namespace_properties_free(shoal_namespace_properties_result **result);
SHOAL_API int64_t SHOAL_CALL
shoal_versioned_properties_version(
    const shoal_versioned_properties_result *result);
SHOAL_API size_t SHOAL_CALL
shoal_versioned_properties_count(
    const shoal_versioned_properties_result *result);
SHOAL_API shoal_status SHOAL_CALL
shoal_versioned_properties_get(
    const shoal_versioned_properties_result *result, size_t index,
    shoal_table_property_view *out_entry, shoal_error **out_error);
SHOAL_API void SHOAL_CALL
shoal_versioned_properties_free(shoal_versioned_properties_result **result);

/*
 * Password, authorization, and split inputs are copied before return.
 * A non-NULL password struct with {NULL, 0} represents an empty password.
 */
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_create_user(shoal_connector *connector, const char *user,
                            const shoal_bytes *password, int64_t timeout_ms,
                            shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_drop_user(shoal_connector *connector, const char *user,
                          int64_t timeout_ms, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_change_password(shoal_connector *connector, const char *user,
                                const shoal_bytes *password,
                                int64_t timeout_ms, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_change_user_authorizations(
    shoal_connector *connector, const char *user,
    const shoal_bytes *authorizations, size_t authorization_count,
    int64_t timeout_ms, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_get_user_authorizations(
    shoal_connector *connector, const char *user, int64_t timeout_ms,
    shoal_bytes_list_result **out_result, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_has_system_permission(
    shoal_connector *connector, const char *user,
    shoal_system_permission permission, int64_t timeout_ms,
    uint8_t *out_has_permission, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_has_table_permission(
    shoal_connector *connector, const char *user, const char *table_name,
    shoal_table_permission permission, int64_t timeout_ms,
    uint8_t *out_has_permission, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_has_namespace_permission(
    shoal_connector *connector, const char *user, const char *namespace_name,
    shoal_namespace_permission permission, int64_t timeout_ms,
    uint8_t *out_has_permission, shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_grant_system_permission(
    shoal_connector *connector, const char *user,
    shoal_system_permission permission, int64_t timeout_ms,
    shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_revoke_system_permission(
    shoal_connector *connector, const char *user,
    shoal_system_permission permission, int64_t timeout_ms,
    shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_grant_table_permission(
    shoal_connector *connector, const char *user, const char *table_name,
    shoal_table_permission permission, int64_t timeout_ms,
    shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_revoke_table_permission(
    shoal_connector *connector, const char *user, const char *table_name,
    shoal_table_permission permission, int64_t timeout_ms,
    shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_grant_namespace_permission(
    shoal_connector *connector, const char *user, const char *namespace_name,
    shoal_namespace_permission permission, int64_t timeout_ms,
    shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_revoke_namespace_permission(
    shoal_connector *connector, const char *user, const char *namespace_name,
    shoal_namespace_permission permission, int64_t timeout_ms,
    shoal_error **out_error);

SHOAL_API shoal_status SHOAL_CALL
shoal_connector_list_table_splits(shoal_connector *connector,
                                  const char *table_name, int64_t timeout_ms,
                                  shoal_bytes_list_result **out_result,
                                  shoal_error **out_error);
SHOAL_API shoal_status SHOAL_CALL
shoal_connector_add_table_splits(shoal_connector *connector,
                                 const char *table_name,
                                 const shoal_bytes *splits,
                                 size_t split_count, int64_t timeout_ms,
                                 shoal_error **out_error);
SHOAL_API size_t SHOAL_CALL
shoal_bytes_list_count(const shoal_bytes_list_result *result);
SHOAL_API shoal_status SHOAL_CALL
shoal_bytes_list_get(const shoal_bytes_list_result *result, size_t index,
                     shoal_bytes *out_value, shoal_error **out_error);
SHOAL_API void SHOAL_CALL
shoal_bytes_list_free(shoal_bytes_list_result **result);

/*
 * Creates a scanner. The configuration and all nested values are copied.
 * Scanner handles remain valid after connector free, but once connector close
 * or free starts, new operations fail with CLOSED while already-started scans
 * are allowed to finish before connector teardown.
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
 * calls, then flushes remaining mutations. Repeated calls return the first
 * close result, including a deadline or cancellation failure.
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

/* Borrowed structured Accumulo security details, or NULL when not applicable. */
SHOAL_API const char *SHOAL_CALL
shoal_error_security_user(const shoal_error *error);
SHOAL_API const char *SHOAL_CALL
shoal_error_security_code(const shoal_error *error);

/*
 * Releases an owned error and sets *error to NULL. Passing NULL or a pointer
 * whose value is NULL is a no-op.
 */
SHOAL_API void SHOAL_CALL shoal_error_free(shoal_error **error);

#ifdef __cplusplus
}
#endif

#endif
