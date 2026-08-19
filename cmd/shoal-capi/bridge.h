#ifndef SHOAL_CAPI_BRIDGE_H
#define SHOAL_CAPI_BRIDGE_H

#include "shoal_types.h"

struct shoal_connector {
  uint64_t id;
};

struct shoal_scanner {
  uint64_t id;
};

struct shoal_batch_scanner {
  uint64_t id;
};

struct shoal_mutation {
  uint64_t id;
};

struct shoal_batch_writer {
  uint64_t id;
};

struct shoal_error {
  shoal_status code;
  char *message;
  char *security_user;
  char *security_code;
};

typedef struct shoal_bridge_scan_entry {
  uint8_t *row;
  size_t row_length;
  uint8_t *column_family;
  size_t column_family_length;
  uint8_t *column_qualifier;
  size_t column_qualifier_length;
  uint8_t *column_visibility;
  size_t column_visibility_length;
  int64_t timestamp;
  uint8_t *value;
  size_t value_length;
} shoal_bridge_scan_entry;

struct shoal_scan_result {
  size_t count;
  shoal_bridge_scan_entry *entries;
};

typedef struct shoal_bridge_table_entry {
  char *name;
  char *id;
} shoal_bridge_table_entry;

struct shoal_table_list_result {
  size_t count;
  shoal_bridge_table_entry *entries;
};

typedef struct shoal_bridge_extent {
  char *server;
  char *table_id;
  uint8_t *prev_row;
  size_t prev_row_length;
  uint8_t *end_row;
  size_t end_row_length;
  uint8_t has_prev_row;
  uint8_t has_end_row;
} shoal_bridge_extent;

typedef struct shoal_bridge_failed_extent {
  shoal_bridge_extent extent;
  size_t submitted;
  int64_t committed;
} shoal_bridge_failed_extent;

typedef struct shoal_bridge_constraint_violation {
  char *server;
  char *constraint_class;
  int16_t violation_code;
  char *description;
  int64_t violating_mutation_count;
} shoal_bridge_constraint_violation;

typedef struct shoal_bridge_authorization_failure {
  shoal_bridge_extent extent;
  char *code;
} shoal_bridge_authorization_failure;

typedef struct shoal_bridge_cleanup_failure {
  char *server;
  char *message;
} shoal_bridge_cleanup_failure;

struct shoal_write_failure {
  shoal_write_failure_flags flags;
  size_t failed_extent_count;
  shoal_bridge_failed_extent *failed_extents;
  size_t constraint_count;
  shoal_bridge_constraint_violation *constraints;
  size_t authorization_count;
  shoal_bridge_authorization_failure *authorizations;
  size_t cleanup_count;
  shoal_bridge_cleanup_failure *cleanups;
};

typedef struct shoal_bridge_table_property_entry {
  char *key;
  char *value;
} shoal_bridge_table_property_entry;

struct shoal_table_properties_result {
  size_t count;
  shoal_bridge_table_property_entry *entries;
};

struct shoal_namespace_list_result {
  size_t count;
  shoal_bridge_table_entry *entries;
};

struct shoal_namespace_properties_result {
  size_t count;
  shoal_bridge_table_property_entry *entries;
};

struct shoal_versioned_properties_result {
  int64_t version;
  size_t count;
  shoal_bridge_table_property_entry *entries;
};

typedef struct shoal_bridge_bytes_entry {
  uint8_t *data;
  size_t length;
} shoal_bridge_bytes_entry;

struct shoal_bytes_list_result {
  size_t count;
  shoal_bridge_bytes_entry *entries;
};

struct shoal_connector_identity_result {
  char *instance_name;
  char *instance_id;
  char *principal;
};

shoal_connector *shoal_bridge_connector_alloc(uint64_t id);
uint64_t shoal_bridge_connector_id(const shoal_connector *connector);
void shoal_bridge_connector_free(shoal_connector *connector);

shoal_scanner *shoal_bridge_scanner_alloc(uint64_t id);
uint64_t shoal_bridge_scanner_id(const shoal_scanner *scanner);
void shoal_bridge_scanner_free(shoal_scanner *scanner);

shoal_batch_scanner *shoal_bridge_batch_scanner_alloc(uint64_t id);
uint64_t shoal_bridge_batch_scanner_id(const shoal_batch_scanner *scanner);
void shoal_bridge_batch_scanner_free(shoal_batch_scanner *scanner);

shoal_mutation *shoal_bridge_mutation_alloc(uint64_t id);
uint64_t shoal_bridge_mutation_id(const shoal_mutation *mutation);
void shoal_bridge_mutation_free(shoal_mutation *mutation);

shoal_batch_writer *shoal_bridge_batch_writer_alloc(uint64_t id);
uint64_t shoal_bridge_batch_writer_id(const shoal_batch_writer *writer);
void shoal_bridge_batch_writer_free(shoal_batch_writer *writer);

char *shoal_bridge_string_alloc(const char *value, size_t length);
void shoal_bridge_string_free(char *value);
#ifdef SHOAL_CAPI_TEST
void shoal_bridge_test_string_alloc_fail_after(size_t successful_allocations);
void shoal_bridge_test_string_alloc_reset(void);
void shoal_bridge_test_result_alloc_fail_after(size_t successful_allocations);
void shoal_bridge_test_result_alloc_reset(void);
void shoal_bridge_test_error_alloc_fail_after(size_t successful_allocations);
void shoal_bridge_test_error_alloc_reset(void);
void shoal_bridge_test_error_message_alloc_fail_after(
    size_t successful_allocations);
void shoal_bridge_test_error_message_alloc_reset(void);
#endif

shoal_scan_result *shoal_bridge_scan_result_alloc(size_t count);
int shoal_bridge_scan_result_set(
    shoal_scan_result *result, size_t index, const uint8_t *row,
    size_t row_length, const uint8_t *column_family,
    size_t column_family_length, const uint8_t *column_qualifier,
    size_t column_qualifier_length, const uint8_t *column_visibility,
    size_t column_visibility_length, int64_t timestamp, const uint8_t *value,
    size_t value_length);
size_t shoal_bridge_scan_result_count(const shoal_scan_result *result);
int shoal_bridge_scan_result_get(const shoal_scan_result *result, size_t index,
                                 shoal_key_value_view *out_value);
void shoal_bridge_scan_result_free(shoal_scan_result *result);

shoal_connector_identity_result *shoal_bridge_connector_identity_alloc(
    char *instance_name, char *instance_id, char *principal);
int shoal_bridge_connector_identity_get(
    const shoal_connector_identity_result *result,
    shoal_connector_identity_view *out_identity);
void shoal_bridge_connector_identity_free(
    shoal_connector_identity_result *result);

shoal_table_list_result *shoal_bridge_table_list_alloc(size_t count);
int shoal_bridge_table_list_set(shoal_table_list_result *result, size_t index,
                                const char *name, const char *id);
size_t shoal_bridge_table_list_count(const shoal_table_list_result *result);
int shoal_bridge_table_list_get(const shoal_table_list_result *result,
                                size_t index, shoal_table_view *out_table);
void shoal_bridge_table_list_free(shoal_table_list_result *result);

shoal_write_failure *shoal_bridge_write_failure_alloc(
    shoal_write_failure_flags flags, size_t failed_extent_count,
    size_t constraint_count, size_t authorization_count, size_t cleanup_count);
int shoal_bridge_write_failure_set_failed_extent(
    shoal_write_failure *failure, size_t index, const char *server,
    const char *table_id, const uint8_t *prev_row, size_t prev_row_length,
    uint8_t has_prev_row, const uint8_t *end_row, size_t end_row_length,
    uint8_t has_end_row, size_t submitted, int64_t committed);
int shoal_bridge_write_failure_set_constraint(
    shoal_write_failure *failure, size_t index, const char *server,
    const char *constraint_class, int16_t violation_code,
    const char *description, int64_t violating_mutation_count);
int shoal_bridge_write_failure_set_authorization(
    shoal_write_failure *failure, size_t index, const char *server,
    const char *table_id, const uint8_t *prev_row, size_t prev_row_length,
    uint8_t has_prev_row, const uint8_t *end_row, size_t end_row_length,
    uint8_t has_end_row, const char *code);
int shoal_bridge_write_failure_set_cleanup(shoal_write_failure *failure,
                                           size_t index, const char *server,
                                           const char *message);
shoal_write_failure_flags shoal_bridge_write_failure_flags(
    const shoal_write_failure *failure);
size_t shoal_bridge_write_failure_failed_extent_count(
    const shoal_write_failure *failure);
int shoal_bridge_write_failure_get_failed_extent(
    const shoal_write_failure *failure, size_t index,
    shoal_failed_extent_view *out_extent);
size_t shoal_bridge_write_failure_constraint_count(
    const shoal_write_failure *failure);
int shoal_bridge_write_failure_get_constraint(
    const shoal_write_failure *failure, size_t index,
    shoal_constraint_violation_view *out_violation);
size_t shoal_bridge_write_failure_authorization_count(
    const shoal_write_failure *failure);
int shoal_bridge_write_failure_get_authorization(
    const shoal_write_failure *failure, size_t index,
    shoal_authorization_failure_view *out_failure);
size_t shoal_bridge_write_failure_cleanup_count(
    const shoal_write_failure *failure);
int shoal_bridge_write_failure_get_cleanup(
    const shoal_write_failure *failure, size_t index,
    shoal_cleanup_failure_view *out_failure);
void shoal_bridge_write_failure_free(shoal_write_failure *failure);

shoal_table_properties_result *shoal_bridge_table_properties_alloc(size_t count);
int shoal_bridge_table_properties_set(shoal_table_properties_result *result,
                                      size_t index, const char *key,
                                      const char *value);
size_t shoal_bridge_table_properties_count(
    const shoal_table_properties_result *result);
int shoal_bridge_table_properties_get(
    const shoal_table_properties_result *result, size_t index,
    shoal_table_property_view *out_property);
void shoal_bridge_table_properties_free(shoal_table_properties_result *result);

shoal_namespace_list_result *shoal_bridge_namespace_list_alloc(size_t count);
int shoal_bridge_namespace_list_set(shoal_namespace_list_result *result,
                                    size_t index, const char *name,
                                    const char *id);
size_t shoal_bridge_namespace_list_count(
    const shoal_namespace_list_result *result);
int shoal_bridge_namespace_list_get(const shoal_namespace_list_result *result,
                                    size_t index,
                                    shoal_namespace_view *out_namespace);
void shoal_bridge_namespace_list_free(shoal_namespace_list_result *result);

shoal_namespace_properties_result *
shoal_bridge_namespace_properties_alloc(size_t count);
int shoal_bridge_namespace_properties_set(
    shoal_namespace_properties_result *result, size_t index, const char *key,
    const char *value);
size_t shoal_bridge_namespace_properties_count(
    const shoal_namespace_properties_result *result);
int shoal_bridge_namespace_properties_get(
    const shoal_namespace_properties_result *result, size_t index,
    shoal_table_property_view *out_property);
void shoal_bridge_namespace_properties_free(
    shoal_namespace_properties_result *result);

shoal_versioned_properties_result *
shoal_bridge_versioned_properties_alloc(int64_t version, size_t count);
int shoal_bridge_versioned_properties_set(
    shoal_versioned_properties_result *result, size_t index, const char *key,
    const char *value);
int64_t shoal_bridge_versioned_properties_version(
    const shoal_versioned_properties_result *result);
size_t shoal_bridge_versioned_properties_count(
    const shoal_versioned_properties_result *result);
int shoal_bridge_versioned_properties_get(
    const shoal_versioned_properties_result *result, size_t index,
    shoal_table_property_view *out_property);
void shoal_bridge_versioned_properties_free(
    shoal_versioned_properties_result *result);

shoal_bytes_list_result *shoal_bridge_bytes_list_alloc(size_t count);
int shoal_bridge_bytes_list_set(shoal_bytes_list_result *result, size_t index,
                                const uint8_t *data, size_t length);
size_t shoal_bridge_bytes_list_count(const shoal_bytes_list_result *result);
int shoal_bridge_bytes_list_get(const shoal_bytes_list_result *result,
                                size_t index, shoal_bytes *out_value);
void shoal_bridge_bytes_list_free(shoal_bytes_list_result *result);

shoal_error *shoal_bridge_error_alloc(
    shoal_status code, const char *message, size_t message_length,
    const char *security_user, size_t security_user_length,
    const char *security_code, size_t security_code_length);
shoal_status shoal_bridge_error_code(const shoal_error *error);
char *shoal_bridge_error_message(const shoal_error *error);
char *shoal_bridge_error_security_user(const shoal_error *error);
char *shoal_bridge_error_security_code(const shoal_error *error);
void shoal_bridge_error_free(shoal_error *error);

void shoal_bridge_connector_config_init(shoal_connector_config *config);
uint32_t shoal_bridge_connector_config_v1_size(void);
void shoal_bridge_scanner_config_init(shoal_scanner_config *config);
uint32_t shoal_bridge_scanner_config_v1_size(void);
void shoal_bridge_range_init(shoal_range *range);
uint32_t shoal_bridge_range_v1_size(void);
void shoal_bridge_batch_writer_config_init(shoal_batch_writer_config *config);
uint32_t shoal_bridge_batch_writer_config_v1_size(void);

#endif
