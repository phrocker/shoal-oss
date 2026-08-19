#include "bridge.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

shoal_connector *shoal_bridge_connector_alloc(uint64_t id) {
  shoal_connector *connector = (shoal_connector *)malloc(sizeof(*connector));
  if (connector != NULL) {
    connector->id = id;
  }
  return connector;
}

uint64_t shoal_bridge_connector_id(const shoal_connector *connector) {
  return connector == NULL ? 0 : connector->id;
}

void shoal_bridge_connector_free(shoal_connector *connector) {
  if (connector != NULL) {
    connector->id = 0;
    free(connector);
  }
}

shoal_scanner *shoal_bridge_scanner_alloc(uint64_t id) {
  shoal_scanner *scanner = (shoal_scanner *)malloc(sizeof(*scanner));
  if (scanner != NULL) {
    scanner->id = id;
  }
  return scanner;
}

uint64_t shoal_bridge_scanner_id(const shoal_scanner *scanner) {
  return scanner == NULL ? 0 : scanner->id;
}

void shoal_bridge_scanner_free(shoal_scanner *scanner) {
  if (scanner != NULL) {
    scanner->id = 0;
    free(scanner);
  }
}

shoal_batch_scanner *shoal_bridge_batch_scanner_alloc(uint64_t id) {
  shoal_batch_scanner *scanner =
      (shoal_batch_scanner *)malloc(sizeof(*scanner));
  if (scanner != NULL) {
    scanner->id = id;
  }
  return scanner;
}

uint64_t
shoal_bridge_batch_scanner_id(const shoal_batch_scanner *scanner) {
  return scanner == NULL ? 0 : scanner->id;
}

void shoal_bridge_batch_scanner_free(shoal_batch_scanner *scanner) {
  if (scanner != NULL) {
    scanner->id = 0;
    free(scanner);
  }
}

shoal_mutation *shoal_bridge_mutation_alloc(uint64_t id) {
  shoal_mutation *mutation = (shoal_mutation *)malloc(sizeof(*mutation));
  if (mutation != NULL) {
    mutation->id = id;
  }
  return mutation;
}

uint64_t shoal_bridge_mutation_id(const shoal_mutation *mutation) {
  return mutation == NULL ? 0 : mutation->id;
}

void shoal_bridge_mutation_free(shoal_mutation *mutation) {
  if (mutation != NULL) {
    mutation->id = 0;
    free(mutation);
  }
}

shoal_batch_writer *shoal_bridge_batch_writer_alloc(uint64_t id) {
  shoal_batch_writer *writer =
      (shoal_batch_writer *)malloc(sizeof(*writer));
  if (writer != NULL) {
    writer->id = id;
  }
  return writer;
}

uint64_t shoal_bridge_batch_writer_id(const shoal_batch_writer *writer) {
  return writer == NULL ? 0 : writer->id;
}

void shoal_bridge_batch_writer_free(shoal_batch_writer *writer) {
  if (writer != NULL) {
    writer->id = 0;
    free(writer);
  }
}

#ifdef SHOAL_CAPI_TEST
static _Atomic size_t shoal_bridge_string_alloc_fail_after = SIZE_MAX;
static _Atomic size_t shoal_bridge_result_alloc_fail_after = SIZE_MAX;
static _Atomic size_t shoal_bridge_error_alloc_fail_after = SIZE_MAX;
static _Atomic size_t shoal_bridge_error_message_alloc_fail_after = SIZE_MAX;

static int shoal_bridge_allocation_allowed(_Atomic size_t *fail_after) {
  size_t remaining =
      atomic_load_explicit(fail_after, memory_order_relaxed);
  while (remaining != SIZE_MAX) {
    if (remaining == 0) {
      return 0;
    }
    if (atomic_compare_exchange_weak_explicit(
            fail_after, &remaining, remaining - 1, memory_order_relaxed,
            memory_order_relaxed)) {
      break;
    }
  }
  return 1;
}

#define SHOAL_BRIDGE_TEST_ALLOC_GUARD(counter)                              \
  do {                                                                      \
    if (!shoal_bridge_allocation_allowed(&(counter))) {                     \
      return NULL;                                                          \
    }                                                                       \
  } while (0)
#else
#define SHOAL_BRIDGE_TEST_ALLOC_GUARD(counter) do { } while (0)
#endif

char *shoal_bridge_string_alloc(const char *value, size_t length) {
  if ((value == NULL && length != 0) || length == SIZE_MAX) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_string_alloc_fail_after);
  char *copy = (char *)malloc(length + 1);
  if (copy == NULL) {
    return NULL;
  }
  if (length != 0) {
    memcpy(copy, value, length);
  }
  copy[length] = '\0';
  return copy;
}

void shoal_bridge_string_free(char *value) {
  free(value);
}

shoal_connector_identity_result *shoal_bridge_connector_identity_alloc(
    char *instance_name, char *instance_id, char *principal) {
  if (instance_name == NULL || instance_id == NULL || principal == NULL) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  shoal_connector_identity_result *result =
      (shoal_connector_identity_result *)malloc(sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  result->instance_name = instance_name;
  result->instance_id = instance_id;
  result->principal = principal;
  return result;
}

int shoal_bridge_connector_identity_get(
    const shoal_connector_identity_result *result,
    shoal_connector_identity_view *out_identity) {
  if (result == NULL || out_identity == NULL) {
    return 0;
  }
  out_identity->instance_name = result->instance_name;
  out_identity->instance_id = result->instance_id;
  out_identity->principal = result->principal;
  return 1;
}

void shoal_bridge_connector_identity_free(
    shoal_connector_identity_result *result) {
  if (result == NULL) {
    return;
  }
  free(result->instance_name);
  free(result->instance_id);
  free(result->principal);
  result->instance_name = NULL;
  result->instance_id = NULL;
  result->principal = NULL;
  free(result);
}

#ifdef SHOAL_CAPI_TEST
void shoal_bridge_test_string_alloc_fail_after(size_t successful_allocations) {
  atomic_store_explicit(&shoal_bridge_string_alloc_fail_after,
                        successful_allocations, memory_order_relaxed);
}

void shoal_bridge_test_string_alloc_reset(void) {
  atomic_store_explicit(&shoal_bridge_string_alloc_fail_after, SIZE_MAX,
                        memory_order_relaxed);
}

void shoal_bridge_test_result_alloc_fail_after(
    size_t successful_allocations) {
  atomic_store_explicit(&shoal_bridge_result_alloc_fail_after,
                        successful_allocations, memory_order_relaxed);
}

void shoal_bridge_test_result_alloc_reset(void) {
  atomic_store_explicit(&shoal_bridge_result_alloc_fail_after, SIZE_MAX,
                        memory_order_relaxed);
}

void shoal_bridge_test_error_alloc_fail_after(
    size_t successful_allocations) {
  atomic_store_explicit(&shoal_bridge_error_alloc_fail_after,
                        successful_allocations, memory_order_relaxed);
}

void shoal_bridge_test_error_alloc_reset(void) {
  atomic_store_explicit(&shoal_bridge_error_alloc_fail_after, SIZE_MAX,
                        memory_order_relaxed);
}

void shoal_bridge_test_error_message_alloc_fail_after(
    size_t successful_allocations) {
  atomic_store_explicit(&shoal_bridge_error_message_alloc_fail_after,
                        successful_allocations, memory_order_relaxed);
}

void shoal_bridge_test_error_message_alloc_reset(void) {
  atomic_store_explicit(&shoal_bridge_error_message_alloc_fail_after, SIZE_MAX,
                        memory_order_relaxed);
}
#endif

static uint8_t *shoal_bridge_copy_bytes(const uint8_t *value, size_t length) {
  if (length == 0) {
    return NULL;
  }
  if (value == NULL) {
    return NULL;
  }
  uint8_t *copy = (uint8_t *)malloc(length);
  if (copy != NULL) {
    memcpy(copy, value, length);
  }
  return copy;
}

static char *shoal_bridge_copy_string(const char *value) {
  if (value == NULL) {
    return NULL;
  }
  size_t length = strlen(value);
  if (length == SIZE_MAX) {
    return NULL;
  }
  char *copy = (char *)malloc(length + 1);
  if (copy != NULL) {
    memcpy(copy, value, length + 1);
  }
  return copy;
}

static void shoal_bridge_scan_entry_clear(shoal_bridge_scan_entry *entry) {
  if (entry == NULL) {
    return;
  }
  free(entry->row);
  free(entry->column_family);
  free(entry->column_qualifier);
  free(entry->column_visibility);
  free(entry->value);
  memset(entry, 0, sizeof(*entry));
}

static int shoal_bridge_key_entry_set(
    shoal_bridge_scan_entry *entry, const uint8_t *row, size_t row_length,
    const uint8_t *column_family, size_t column_family_length,
    const uint8_t *column_qualifier, size_t column_qualifier_length,
    const uint8_t *column_visibility, size_t column_visibility_length,
    int64_t timestamp) {
  if (entry == NULL || (row == NULL && row_length != 0) ||
      (column_family == NULL && column_family_length != 0) ||
      (column_qualifier == NULL && column_qualifier_length != 0) ||
      (column_visibility == NULL && column_visibility_length != 0)) {
    return 0;
  }
  shoal_bridge_scan_entry next;
  memset(&next, 0, sizeof(next));
  next.row = shoal_bridge_copy_bytes(row, row_length);
  next.column_family =
      shoal_bridge_copy_bytes(column_family, column_family_length);
  next.column_qualifier =
      shoal_bridge_copy_bytes(column_qualifier, column_qualifier_length);
  next.column_visibility =
      shoal_bridge_copy_bytes(column_visibility, column_visibility_length);
  if ((row_length != 0 && next.row == NULL) ||
      (column_family_length != 0 && next.column_family == NULL) ||
      (column_qualifier_length != 0 && next.column_qualifier == NULL) ||
      (column_visibility_length != 0 && next.column_visibility == NULL)) {
    shoal_bridge_scan_entry_clear(&next);
    return 0;
  }
  next.row_length = row_length;
  next.column_family_length = column_family_length;
  next.column_qualifier_length = column_qualifier_length;
  next.column_visibility_length = column_visibility_length;
  next.timestamp = timestamp;
  shoal_bridge_scan_entry_clear(entry);
  *entry = next;
  return 1;
}

shoal_scan_result *shoal_bridge_scan_result_alloc(size_t count) {
  if (count > SIZE_MAX / sizeof(shoal_bridge_scan_entry)) {
    return NULL;
  }
  shoal_scan_result *result =
      (shoal_scan_result *)calloc(1, sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  if (count != 0) {
    result->entries =
        (shoal_bridge_scan_entry *)calloc(count, sizeof(*result->entries));
    if (result->entries == NULL) {
      free(result);
      return NULL;
    }
  }
  result->count = count;
  return result;
}

int shoal_bridge_scan_result_set(
    shoal_scan_result *result, size_t index, const uint8_t *row,
    size_t row_length, const uint8_t *column_family,
    size_t column_family_length, const uint8_t *column_qualifier,
    size_t column_qualifier_length, const uint8_t *column_visibility,
    size_t column_visibility_length, int64_t timestamp, const uint8_t *value,
    size_t value_length) {
  if (result == NULL || index >= result->count ||
      (row == NULL && row_length != 0) ||
      (column_family == NULL && column_family_length != 0) ||
      (column_qualifier == NULL && column_qualifier_length != 0) ||
      (column_visibility == NULL && column_visibility_length != 0) ||
      (value == NULL && value_length != 0)) {
    return 0;
  }

  shoal_bridge_scan_entry next;
  memset(&next, 0, sizeof(next));
  if (!shoal_bridge_key_entry_set(
          &next, row, row_length, column_family, column_family_length,
          column_qualifier, column_qualifier_length, column_visibility,
          column_visibility_length, timestamp)) {
    return 0;
  }
  next.value = shoal_bridge_copy_bytes(value, value_length);
  if (value_length != 0 && next.value == NULL) {
    shoal_bridge_scan_entry_clear(&next);
    return 0;
  }
  next.value_length = value_length;

  shoal_bridge_scan_entry_clear(&result->entries[index]);
  result->entries[index] = next;
  return 1;
}

size_t shoal_bridge_scan_result_count(const shoal_scan_result *result) {
  return result == NULL ? 0 : result->count;
}

int shoal_bridge_scan_result_get(const shoal_scan_result *result, size_t index,
                                 shoal_key_value_view *out_value) {
  if (result == NULL || index >= result->count || out_value == NULL) {
    return 0;
  }
  const shoal_bridge_scan_entry *entry = &result->entries[index];
  memset(out_value, 0, sizeof(*out_value));
  out_value->row.data = entry->row;
  out_value->row.length = entry->row_length;
  out_value->column_family.data = entry->column_family;
  out_value->column_family.length = entry->column_family_length;
  out_value->column_qualifier.data = entry->column_qualifier;
  out_value->column_qualifier.length = entry->column_qualifier_length;
  out_value->column_visibility.data = entry->column_visibility;
  out_value->column_visibility.length = entry->column_visibility_length;
  out_value->timestamp = entry->timestamp;
  out_value->value.data = entry->value;
  out_value->value.length = entry->value_length;
  return 1;
}

void shoal_bridge_scan_result_free(shoal_scan_result *result) {
  if (result == NULL) {
    return;
  }
  for (size_t index = 0; index < result->count; index++) {
    shoal_bridge_scan_entry_clear(&result->entries[index]);
  }
  free(result->entries);
  result->entries = NULL;
  result->count = 0;
  free(result);
}

static void shoal_bridge_key_entry_get(const shoal_bridge_scan_entry *entry,
                                       shoal_key *out_key) {
  memset(out_key, 0, sizeof(*out_key));
  out_key->row.data = entry->row;
  out_key->row.length = entry->row_length;
  out_key->column_family.data = entry->column_family;
  out_key->column_family.length = entry->column_family_length;
  out_key->column_qualifier.data = entry->column_qualifier;
  out_key->column_qualifier.length = entry->column_qualifier_length;
  out_key->column_visibility.data = entry->column_visibility;
  out_key->column_visibility.length = entry->column_visibility_length;
  out_key->timestamp = entry->timestamp;
}

shoal_range_result *shoal_bridge_range_result_alloc(void) {
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  return (shoal_range_result *)calloc(1, sizeof(shoal_range_result));
}

int shoal_bridge_range_result_set_start(
    shoal_range_result *result, const uint8_t *row, size_t row_length,
    const uint8_t *column_family, size_t column_family_length,
    const uint8_t *column_qualifier, size_t column_qualifier_length,
    const uint8_t *column_visibility, size_t column_visibility_length,
    int64_t timestamp) {
  if (result == NULL) {
    return 0;
  }
  if (!shoal_bridge_key_entry_set(
          &result->start_key, row, row_length, column_family,
          column_family_length, column_qualifier, column_qualifier_length,
          column_visibility, column_visibility_length, timestamp)) {
    return 0;
  }
  result->has_start_key = 1;
  return 1;
}

int shoal_bridge_range_result_set_end(
    shoal_range_result *result, const uint8_t *row, size_t row_length,
    const uint8_t *column_family, size_t column_family_length,
    const uint8_t *column_qualifier, size_t column_qualifier_length,
    const uint8_t *column_visibility, size_t column_visibility_length,
    int64_t timestamp) {
  if (result == NULL) {
    return 0;
  }
  if (!shoal_bridge_key_entry_set(
          &result->end_key, row, row_length, column_family,
          column_family_length, column_qualifier, column_qualifier_length,
          column_visibility, column_visibility_length, timestamp)) {
    return 0;
  }
  result->has_end_key = 1;
  return 1;
}

void shoal_bridge_range_result_set_metadata(
    shoal_range_result *result, shoal_range_bound_kind start_kind,
    shoal_range_bound_kind end_kind, uint8_t start_inclusive,
    uint8_t end_inclusive) {
  if (result != NULL) {
    result->start_kind = start_kind;
    result->end_kind = end_kind;
    result->start_inclusive = start_inclusive;
    result->end_inclusive = end_inclusive;
  }
}

int shoal_bridge_range_result_get(const shoal_range_result *result,
                                  shoal_range_view *out_range) {
  if (result == NULL || out_range == NULL) {
    return 0;
  }
  out_range->start_kind = result->start_kind;
  out_range->has_start_key = result->has_start_key;
  if (result->has_start_key) {
    shoal_bridge_key_entry_get(&result->start_key, &out_range->start_key);
  } else {
    memset(&out_range->start_key, 0, sizeof(out_range->start_key));
  }
  out_range->end_kind = result->end_kind;
  out_range->has_end_key = result->has_end_key;
  if (result->has_end_key) {
    shoal_bridge_key_entry_get(&result->end_key, &out_range->end_key);
  } else {
    memset(&out_range->end_key, 0, sizeof(out_range->end_key));
  }
  out_range->start_inclusive = result->start_inclusive;
  out_range->end_inclusive = result->end_inclusive;
  return 1;
}

void shoal_bridge_range_result_free(shoal_range_result *result) {
  if (result == NULL) {
    return;
  }
  shoal_bridge_scan_entry_clear(&result->start_key);
  shoal_bridge_scan_entry_clear(&result->end_key);
  free(result);
}

shoal_iterator_setting_result *
shoal_bridge_iterator_setting_result_alloc(size_t option_count) {
  if (option_count > SIZE_MAX / sizeof(shoal_iterator_option)) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  shoal_iterator_setting_result *result =
      (shoal_iterator_setting_result *)calloc(1, sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  if (option_count != 0) {
    result->options =
        (shoal_iterator_option *)calloc(option_count, sizeof(*result->options));
    if (result->options == NULL) {
      free(result);
      return NULL;
    }
  }
  result->option_count = option_count;
  return result;
}

int shoal_bridge_iterator_setting_result_set_identity(
    shoal_iterator_setting_result *result, const char *name,
    const char *class_name, int32_t priority) {
  if (result == NULL || name == NULL || class_name == NULL) {
    return 0;
  }
  char *name_copy = shoal_bridge_copy_string(name);
  char *class_copy = shoal_bridge_copy_string(class_name);
  if (name_copy == NULL || class_copy == NULL) {
    free(name_copy);
    free(class_copy);
    return 0;
  }
  free(result->name);
  free(result->class_name);
  result->name = name_copy;
  result->class_name = class_copy;
  result->priority = priority;
  return 1;
}

int shoal_bridge_iterator_setting_result_set_option(
    shoal_iterator_setting_result *result, size_t index, const char *key,
    const char *value) {
  if (result == NULL || index >= result->option_count || key == NULL ||
      value == NULL) {
    return 0;
  }
  char *key_copy = shoal_bridge_copy_string(key);
  char *value_copy = shoal_bridge_copy_string(value);
  if (key_copy == NULL || value_copy == NULL) {
    free(key_copy);
    free(value_copy);
    return 0;
  }
  free((void *)result->options[index].key);
  free((void *)result->options[index].value);
  result->options[index].key = key_copy;
  result->options[index].value = value_copy;
  return 1;
}

int shoal_bridge_iterator_setting_result_get(
    const shoal_iterator_setting_result *result,
    shoal_iterator_setting_view *out_setting) {
  if (result == NULL || out_setting == NULL || result->name == NULL ||
      result->class_name == NULL) {
    return 0;
  }
  out_setting->name = result->name;
  out_setting->class_name = result->class_name;
  out_setting->priority = result->priority;
  out_setting->options = result->options;
  out_setting->option_count = result->option_count;
  return 1;
}

void shoal_bridge_iterator_setting_result_free(
    shoal_iterator_setting_result *result) {
  if (result == NULL) {
    return;
  }
  free(result->name);
  free(result->class_name);
  for (size_t index = 0; index < result->option_count; ++index) {
    free((void *)result->options[index].key);
    free((void *)result->options[index].value);
  }
  free(result->options);
  free(result);
}

static void shoal_bridge_table_entry_clear(shoal_bridge_table_entry *entry) {
  if (entry == NULL) {
    return;
  }
  free(entry->name);
  free(entry->id);
  memset(entry, 0, sizeof(*entry));
}

shoal_table_list_result *shoal_bridge_table_list_alloc(size_t count) {
  if (count > SIZE_MAX / sizeof(shoal_bridge_table_entry)) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  shoal_table_list_result *result =
      (shoal_table_list_result *)calloc(1, sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  if (count != 0) {
    result->entries =
        (shoal_bridge_table_entry *)calloc(count, sizeof(*result->entries));
    if (result->entries == NULL) {
      free(result);
      return NULL;
    }
  }
  result->count = count;
  return result;
}

int shoal_bridge_table_list_set(shoal_table_list_result *result, size_t index,
                                const char *name, const char *id) {
  if (result == NULL || index >= result->count || name == NULL || id == NULL) {
    return 0;
  }
  shoal_bridge_table_entry next;
  memset(&next, 0, sizeof(next));
  next.name = shoal_bridge_copy_string(name);
  next.id = shoal_bridge_copy_string(id);
  if (next.name == NULL || next.id == NULL) {
    shoal_bridge_table_entry_clear(&next);
    return 0;
  }
  shoal_bridge_table_entry_clear(&result->entries[index]);
  result->entries[index] = next;
  return 1;
}

size_t shoal_bridge_table_list_count(const shoal_table_list_result *result) {
  return result == NULL ? 0 : result->count;
}

int shoal_bridge_table_list_get(const shoal_table_list_result *result,
                                size_t index, shoal_table_view *out_table) {
  if (result == NULL || index >= result->count || out_table == NULL) {
    return 0;
  }
  const shoal_bridge_table_entry *entry = &result->entries[index];
  memset(out_table, 0, sizeof(*out_table));
  out_table->name = entry->name;
  out_table->id = entry->id;
  return 1;
}

void shoal_bridge_table_list_free(shoal_table_list_result *result) {
  if (result == NULL) {
    return;
  }
  for (size_t index = 0; index < result->count; index++) {
    shoal_bridge_table_entry_clear(&result->entries[index]);
  }
  free(result->entries);
  result->entries = NULL;
  result->count = 0;
  free(result);
}

static void shoal_bridge_extent_clear(shoal_bridge_extent *extent) {
  if (extent == NULL) {
    return;
  }
  free(extent->server);
  free(extent->table_id);
  free(extent->prev_row);
  free(extent->end_row);
  memset(extent, 0, sizeof(*extent));
}

static int shoal_bridge_extent_set(
    shoal_bridge_extent *extent, const char *server, const char *table_id,
    const uint8_t *prev_row, size_t prev_row_length, uint8_t has_prev_row,
    const uint8_t *end_row, size_t end_row_length, uint8_t has_end_row) {
  if (extent == NULL || server == NULL || table_id == NULL ||
      has_prev_row > 1 || has_end_row > 1 ||
      (!has_prev_row && (prev_row != NULL || prev_row_length != 0)) ||
      (!has_end_row && (end_row != NULL || end_row_length != 0)) ||
      (has_prev_row && prev_row == NULL && prev_row_length != 0) ||
      (has_end_row && end_row == NULL && end_row_length != 0)) {
    return 0;
  }
  shoal_bridge_extent next;
  memset(&next, 0, sizeof(next));
  next.server = shoal_bridge_copy_string(server);
  next.table_id = shoal_bridge_copy_string(table_id);
  if (has_prev_row) {
    next.prev_row = shoal_bridge_copy_bytes(prev_row, prev_row_length);
  }
  if (has_end_row) {
    next.end_row = shoal_bridge_copy_bytes(end_row, end_row_length);
  }
  if (next.server == NULL || next.table_id == NULL ||
      (has_prev_row && prev_row_length != 0 && next.prev_row == NULL) ||
      (has_end_row && end_row_length != 0 && next.end_row == NULL)) {
    shoal_bridge_extent_clear(&next);
    return 0;
  }
  next.prev_row_length = prev_row_length;
  next.end_row_length = end_row_length;
  next.has_prev_row = has_prev_row;
  next.has_end_row = has_end_row;
  shoal_bridge_extent_clear(extent);
  *extent = next;
  return 1;
}

static void shoal_bridge_failed_extent_clear(
    shoal_bridge_failed_extent *extent) {
  if (extent == NULL) {
    return;
  }
  shoal_bridge_extent_clear(&extent->extent);
  extent->submitted = 0;
  extent->committed = 0;
}

static void shoal_bridge_constraint_clear(
    shoal_bridge_constraint_violation *violation) {
  if (violation == NULL) {
    return;
  }
  free(violation->server);
  free(violation->constraint_class);
  free(violation->description);
  memset(violation, 0, sizeof(*violation));
}

static void shoal_bridge_authorization_clear(
    shoal_bridge_authorization_failure *failure) {
  if (failure == NULL) {
    return;
  }
  shoal_bridge_extent_clear(&failure->extent);
  free(failure->code);
  failure->code = NULL;
}

static void shoal_bridge_cleanup_clear(
    shoal_bridge_cleanup_failure *failure) {
  if (failure == NULL) {
    return;
  }
  free(failure->server);
  free(failure->message);
  memset(failure, 0, sizeof(*failure));
}

static void *shoal_bridge_calloc_array(size_t count, size_t element_size) {
  if (count == 0) {
    return NULL;
  }
  if (element_size != 0 && count > SIZE_MAX / element_size) {
    return NULL;
  }
  return calloc(count, element_size);
}

shoal_write_failure *shoal_bridge_write_failure_alloc(
    shoal_write_failure_flags flags, size_t failed_extent_count,
    size_t constraint_count, size_t authorization_count, size_t cleanup_count) {
  shoal_write_failure *failure =
      (shoal_write_failure *)calloc(1, sizeof(*failure));
  if (failure == NULL) {
    return NULL;
  }
  failure->failed_extents = (shoal_bridge_failed_extent *)
      shoal_bridge_calloc_array(failed_extent_count,
                                sizeof(*failure->failed_extents));
  failure->constraints = (shoal_bridge_constraint_violation *)
      shoal_bridge_calloc_array(constraint_count, sizeof(*failure->constraints));
  failure->authorizations = (shoal_bridge_authorization_failure *)
      shoal_bridge_calloc_array(authorization_count,
                                sizeof(*failure->authorizations));
  failure->cleanups = (shoal_bridge_cleanup_failure *)
      shoal_bridge_calloc_array(cleanup_count, sizeof(*failure->cleanups));
  if ((failed_extent_count != 0 && failure->failed_extents == NULL) ||
      (constraint_count != 0 && failure->constraints == NULL) ||
      (authorization_count != 0 && failure->authorizations == NULL) ||
      (cleanup_count != 0 && failure->cleanups == NULL)) {
    shoal_bridge_write_failure_free(failure);
    return NULL;
  }
  failure->flags = flags;
  failure->failed_extent_count = failed_extent_count;
  failure->constraint_count = constraint_count;
  failure->authorization_count = authorization_count;
  failure->cleanup_count = cleanup_count;
  return failure;
}

int shoal_bridge_write_failure_set_failed_extent(
    shoal_write_failure *failure, size_t index, const char *server,
    const char *table_id, const uint8_t *prev_row, size_t prev_row_length,
    uint8_t has_prev_row, const uint8_t *end_row, size_t end_row_length,
    uint8_t has_end_row, size_t submitted, int64_t committed) {
  if (failure == NULL || index >= failure->failed_extent_count) {
    return 0;
  }
  shoal_bridge_failed_extent *entry = &failure->failed_extents[index];
  if (!shoal_bridge_extent_set(&entry->extent, server, table_id, prev_row,
                               prev_row_length, has_prev_row, end_row,
                               end_row_length, has_end_row)) {
    return 0;
  }
  entry->submitted = submitted;
  entry->committed = committed;
  return 1;
}

int shoal_bridge_write_failure_set_constraint(
    shoal_write_failure *failure, size_t index, const char *server,
    const char *constraint_class, int16_t violation_code,
    const char *description, int64_t violating_mutation_count) {
  if (failure == NULL || index >= failure->constraint_count ||
      server == NULL || constraint_class == NULL || description == NULL) {
    return 0;
  }
  shoal_bridge_constraint_violation next;
  memset(&next, 0, sizeof(next));
  next.server = shoal_bridge_copy_string(server);
  next.constraint_class = shoal_bridge_copy_string(constraint_class);
  next.description = shoal_bridge_copy_string(description);
  if (next.server == NULL || next.constraint_class == NULL ||
      next.description == NULL) {
    shoal_bridge_constraint_clear(&next);
    return 0;
  }
  next.violation_code = violation_code;
  next.violating_mutation_count = violating_mutation_count;
  shoal_bridge_constraint_clear(&failure->constraints[index]);
  failure->constraints[index] = next;
  return 1;
}

int shoal_bridge_write_failure_set_authorization(
    shoal_write_failure *failure, size_t index, const char *server,
    const char *table_id, const uint8_t *prev_row, size_t prev_row_length,
    uint8_t has_prev_row, const uint8_t *end_row, size_t end_row_length,
    uint8_t has_end_row, const char *code) {
  if (failure == NULL || index >= failure->authorization_count ||
      code == NULL) {
    return 0;
  }
  shoal_bridge_authorization_failure next;
  memset(&next, 0, sizeof(next));
  if (!shoal_bridge_extent_set(&next.extent, server, table_id, prev_row,
                               prev_row_length, has_prev_row, end_row,
                               end_row_length, has_end_row)) {
    return 0;
  }
  next.code = shoal_bridge_copy_string(code);
  if (next.code == NULL) {
    shoal_bridge_authorization_clear(&next);
    return 0;
  }
  shoal_bridge_authorization_clear(&failure->authorizations[index]);
  failure->authorizations[index] = next;
  return 1;
}

int shoal_bridge_write_failure_set_cleanup(shoal_write_failure *failure,
                                           size_t index, const char *server,
                                           const char *message) {
  if (failure == NULL || index >= failure->cleanup_count || server == NULL ||
      message == NULL) {
    return 0;
  }
  shoal_bridge_cleanup_failure next;
  memset(&next, 0, sizeof(next));
  next.server = shoal_bridge_copy_string(server);
  next.message = shoal_bridge_copy_string(message);
  if (next.server == NULL || next.message == NULL) {
    shoal_bridge_cleanup_clear(&next);
    return 0;
  }
  shoal_bridge_cleanup_clear(&failure->cleanups[index]);
  failure->cleanups[index] = next;
  return 1;
}

shoal_write_failure_flags shoal_bridge_write_failure_flags(
    const shoal_write_failure *failure) {
  return failure == NULL ? 0 : failure->flags;
}

size_t shoal_bridge_write_failure_failed_extent_count(
    const shoal_write_failure *failure) {
  return failure == NULL ? 0 : failure->failed_extent_count;
}

int shoal_bridge_write_failure_get_failed_extent(
    const shoal_write_failure *failure, size_t index,
    shoal_failed_extent_view *out_extent) {
  if (failure == NULL || index >= failure->failed_extent_count ||
      out_extent == NULL) {
    return 0;
  }
  const shoal_bridge_failed_extent *entry = &failure->failed_extents[index];
  memset(out_extent, 0, sizeof(*out_extent));
  out_extent->server = entry->extent.server;
  out_extent->table_id = entry->extent.table_id;
  out_extent->prev_row.data = entry->extent.prev_row;
  out_extent->prev_row.length = entry->extent.prev_row_length;
  out_extent->end_row.data = entry->extent.end_row;
  out_extent->end_row.length = entry->extent.end_row_length;
  out_extent->has_prev_row = entry->extent.has_prev_row;
  out_extent->has_end_row = entry->extent.has_end_row;
  out_extent->submitted = entry->submitted;
  out_extent->committed = entry->committed;
  return 1;
}

size_t shoal_bridge_write_failure_constraint_count(
    const shoal_write_failure *failure) {
  return failure == NULL ? 0 : failure->constraint_count;
}

int shoal_bridge_write_failure_get_constraint(
    const shoal_write_failure *failure, size_t index,
    shoal_constraint_violation_view *out_violation) {
  if (failure == NULL || index >= failure->constraint_count ||
      out_violation == NULL) {
    return 0;
  }
  const shoal_bridge_constraint_violation *entry =
      &failure->constraints[index];
  memset(out_violation, 0, sizeof(*out_violation));
  out_violation->server = entry->server;
  out_violation->constraint_class = entry->constraint_class;
  out_violation->violation_code = entry->violation_code;
  out_violation->description = entry->description;
  out_violation->violating_mutation_count = entry->violating_mutation_count;
  return 1;
}

size_t shoal_bridge_write_failure_authorization_count(
    const shoal_write_failure *failure) {
  return failure == NULL ? 0 : failure->authorization_count;
}

int shoal_bridge_write_failure_get_authorization(
    const shoal_write_failure *failure, size_t index,
    shoal_authorization_failure_view *out_failure) {
  if (failure == NULL || index >= failure->authorization_count ||
      out_failure == NULL) {
    return 0;
  }
  const shoal_bridge_authorization_failure *entry =
      &failure->authorizations[index];
  memset(out_failure, 0, sizeof(*out_failure));
  out_failure->server = entry->extent.server;
  out_failure->table_id = entry->extent.table_id;
  out_failure->prev_row.data = entry->extent.prev_row;
  out_failure->prev_row.length = entry->extent.prev_row_length;
  out_failure->end_row.data = entry->extent.end_row;
  out_failure->end_row.length = entry->extent.end_row_length;
  out_failure->has_prev_row = entry->extent.has_prev_row;
  out_failure->has_end_row = entry->extent.has_end_row;
  out_failure->code = entry->code;
  return 1;
}

size_t shoal_bridge_write_failure_cleanup_count(
    const shoal_write_failure *failure) {
  return failure == NULL ? 0 : failure->cleanup_count;
}

int shoal_bridge_write_failure_get_cleanup(
    const shoal_write_failure *failure, size_t index,
    shoal_cleanup_failure_view *out_failure) {
  if (failure == NULL || index >= failure->cleanup_count ||
      out_failure == NULL) {
    return 0;
  }
  const shoal_bridge_cleanup_failure *entry = &failure->cleanups[index];
  memset(out_failure, 0, sizeof(*out_failure));
  out_failure->server = entry->server;
  out_failure->message = entry->message;
  return 1;
}

void shoal_bridge_write_failure_free(shoal_write_failure *failure) {
  if (failure == NULL) {
    return;
  }
  for (size_t index = 0; index < failure->failed_extent_count; index++) {
    shoal_bridge_failed_extent_clear(&failure->failed_extents[index]);
  }
  for (size_t index = 0; index < failure->constraint_count; index++) {
    shoal_bridge_constraint_clear(&failure->constraints[index]);
  }
  for (size_t index = 0; index < failure->authorization_count; index++) {
    shoal_bridge_authorization_clear(&failure->authorizations[index]);
  }
  for (size_t index = 0; index < failure->cleanup_count; index++) {
    shoal_bridge_cleanup_clear(&failure->cleanups[index]);
  }
  free(failure->failed_extents);
  free(failure->constraints);
  free(failure->authorizations);
  free(failure->cleanups);
  memset(failure, 0, sizeof(*failure));
  free(failure);
}

static void shoal_bridge_table_property_entry_clear(
    shoal_bridge_table_property_entry *entry) {
  if (entry == NULL) {
    return;
  }
  free(entry->key);
  free(entry->value);
  memset(entry, 0, sizeof(*entry));
}

shoal_table_properties_result *shoal_bridge_table_properties_alloc(size_t count) {
  if (count > SIZE_MAX / sizeof(shoal_bridge_table_property_entry)) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  shoal_table_properties_result *result =
      (shoal_table_properties_result *)calloc(1, sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  if (count != 0) {
    result->entries = (shoal_bridge_table_property_entry *)calloc(
        count, sizeof(*result->entries));
    if (result->entries == NULL) {
      free(result);
      return NULL;
    }
  }
  result->count = count;
  return result;
}

int shoal_bridge_table_properties_set(shoal_table_properties_result *result,
                                      size_t index, const char *key,
                                      const char *value) {
  if (result == NULL || index >= result->count || key == NULL ||
      value == NULL) {
    return 0;
  }
  shoal_bridge_table_property_entry next;
  memset(&next, 0, sizeof(next));
  next.key = shoal_bridge_copy_string(key);
  next.value = shoal_bridge_copy_string(value);
  if (next.key == NULL || next.value == NULL) {
    shoal_bridge_table_property_entry_clear(&next);
    return 0;
  }
  shoal_bridge_table_property_entry_clear(&result->entries[index]);
  result->entries[index] = next;
  return 1;
}

size_t shoal_bridge_table_properties_count(
    const shoal_table_properties_result *result) {
  return result == NULL ? 0 : result->count;
}

int shoal_bridge_table_properties_get(
    const shoal_table_properties_result *result, size_t index,
    shoal_table_property_view *out_property) {
  if (result == NULL || index >= result->count || out_property == NULL) {
    return 0;
  }
  const shoal_bridge_table_property_entry *entry = &result->entries[index];
  memset(out_property, 0, sizeof(*out_property));
  out_property->key = entry->key;
  out_property->value = entry->value;
  return 1;
}

void shoal_bridge_table_properties_free(shoal_table_properties_result *result) {
  if (result == NULL) {
    return;
  }
  for (size_t index = 0; index < result->count; index++) {
    shoal_bridge_table_property_entry_clear(&result->entries[index]);
  }
  free(result->entries);
  result->entries = NULL;
  result->count = 0;
  free(result);
}

shoal_namespace_list_result *shoal_bridge_namespace_list_alloc(size_t count) {
  if (count > SIZE_MAX / sizeof(shoal_bridge_table_entry)) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  shoal_namespace_list_result *result =
      (shoal_namespace_list_result *)calloc(1, sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  if (count != 0) {
    result->entries =
        (shoal_bridge_table_entry *)calloc(count, sizeof(*result->entries));
    if (result->entries == NULL) {
      free(result);
      return NULL;
    }
  }
  result->count = count;
  return result;
}

int shoal_bridge_namespace_list_set(shoal_namespace_list_result *result,
                                    size_t index, const char *name,
                                    const char *id) {
  if (result == NULL || index >= result->count || name == NULL || id == NULL) {
    return 0;
  }
  shoal_bridge_table_entry next;
  memset(&next, 0, sizeof(next));
  next.name = shoal_bridge_copy_string(name);
  next.id = shoal_bridge_copy_string(id);
  if (next.name == NULL || next.id == NULL) {
    shoal_bridge_table_entry_clear(&next);
    return 0;
  }
  shoal_bridge_table_entry_clear(&result->entries[index]);
  result->entries[index] = next;
  return 1;
}

size_t shoal_bridge_namespace_list_count(
    const shoal_namespace_list_result *result) {
  return result == NULL ? 0 : result->count;
}

int shoal_bridge_namespace_list_get(const shoal_namespace_list_result *result,
                                    size_t index,
                                    shoal_namespace_view *out_namespace) {
  if (result == NULL || index >= result->count || out_namespace == NULL) {
    return 0;
  }
  const shoal_bridge_table_entry *entry = &result->entries[index];
  memset(out_namespace, 0, sizeof(*out_namespace));
  out_namespace->name = entry->name;
  out_namespace->id = entry->id;
  return 1;
}

void shoal_bridge_namespace_list_free(shoal_namespace_list_result *result) {
  if (result == NULL) {
    return;
  }
  for (size_t index = 0; index < result->count; index++) {
    shoal_bridge_table_entry_clear(&result->entries[index]);
  }
  free(result->entries);
  memset(result, 0, sizeof(*result));
  free(result);
}

shoal_namespace_properties_result *
shoal_bridge_namespace_properties_alloc(size_t count) {
  if (count > SIZE_MAX / sizeof(shoal_bridge_table_property_entry)) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  shoal_namespace_properties_result *result =
      (shoal_namespace_properties_result *)calloc(1, sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  if (count != 0) {
    result->entries = (shoal_bridge_table_property_entry *)calloc(
        count, sizeof(*result->entries));
    if (result->entries == NULL) {
      free(result);
      return NULL;
    }
  }
  result->count = count;
  return result;
}

int shoal_bridge_namespace_properties_set(
    shoal_namespace_properties_result *result, size_t index, const char *key,
    const char *value) {
  if (result == NULL || index >= result->count || key == NULL ||
      value == NULL) {
    return 0;
  }
  shoal_bridge_table_property_entry next;
  memset(&next, 0, sizeof(next));
  next.key = shoal_bridge_copy_string(key);
  next.value = shoal_bridge_copy_string(value);
  if (next.key == NULL || next.value == NULL) {
    shoal_bridge_table_property_entry_clear(&next);
    return 0;
  }
  shoal_bridge_table_property_entry_clear(&result->entries[index]);
  result->entries[index] = next;
  return 1;
}

size_t shoal_bridge_namespace_properties_count(
    const shoal_namespace_properties_result *result) {
  return result == NULL ? 0 : result->count;
}

int shoal_bridge_namespace_properties_get(
    const shoal_namespace_properties_result *result, size_t index,
    shoal_table_property_view *out_property) {
  if (result == NULL || index >= result->count || out_property == NULL) {
    return 0;
  }
  const shoal_bridge_table_property_entry *entry = &result->entries[index];
  memset(out_property, 0, sizeof(*out_property));
  out_property->key = entry->key;
  out_property->value = entry->value;
  return 1;
}

void shoal_bridge_namespace_properties_free(
    shoal_namespace_properties_result *result) {
  if (result == NULL) {
    return;
  }
  for (size_t index = 0; index < result->count; index++) {
    shoal_bridge_table_property_entry_clear(&result->entries[index]);
  }
  free(result->entries);
  memset(result, 0, sizeof(*result));
  free(result);
}

shoal_versioned_properties_result *
shoal_bridge_versioned_properties_alloc(int64_t version, size_t count) {
  if (count > SIZE_MAX / sizeof(shoal_bridge_table_property_entry)) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  shoal_versioned_properties_result *result =
      (shoal_versioned_properties_result *)calloc(1, sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  if (count != 0) {
    result->entries = (shoal_bridge_table_property_entry *)calloc(
        count, sizeof(*result->entries));
    if (result->entries == NULL) {
      free(result);
      return NULL;
    }
  }
  result->version = version;
  result->count = count;
  return result;
}

int shoal_bridge_versioned_properties_set(
    shoal_versioned_properties_result *result, size_t index, const char *key,
    const char *value) {
  if (result == NULL || index >= result->count || key == NULL ||
      value == NULL) {
    return 0;
  }
  shoal_bridge_table_property_entry next;
  memset(&next, 0, sizeof(next));
  next.key = shoal_bridge_copy_string(key);
  next.value = shoal_bridge_copy_string(value);
  if (next.key == NULL || next.value == NULL) {
    shoal_bridge_table_property_entry_clear(&next);
    return 0;
  }
  shoal_bridge_table_property_entry_clear(&result->entries[index]);
  result->entries[index] = next;
  return 1;
}

int64_t shoal_bridge_versioned_properties_version(
    const shoal_versioned_properties_result *result) {
  return result == NULL ? 0 : result->version;
}

size_t shoal_bridge_versioned_properties_count(
    const shoal_versioned_properties_result *result) {
  return result == NULL ? 0 : result->count;
}

int shoal_bridge_versioned_properties_get(
    const shoal_versioned_properties_result *result, size_t index,
    shoal_table_property_view *out_property) {
  if (result == NULL || index >= result->count || out_property == NULL) {
    return 0;
  }
  memset(out_property, 0, sizeof(*out_property));
  out_property->key = result->entries[index].key;
  out_property->value = result->entries[index].value;
  return 1;
}

void shoal_bridge_versioned_properties_free(
    shoal_versioned_properties_result *result) {
  if (result == NULL) {
    return;
  }
  for (size_t index = 0; index < result->count; index++) {
    shoal_bridge_table_property_entry_clear(&result->entries[index]);
  }
  free(result->entries);
  memset(result, 0, sizeof(*result));
  free(result);
}

shoal_bytes_list_result *shoal_bridge_bytes_list_alloc(size_t count) {
  if (count > SIZE_MAX / sizeof(shoal_bridge_bytes_entry)) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_result_alloc_fail_after);
  shoal_bytes_list_result *result =
      (shoal_bytes_list_result *)calloc(1, sizeof(*result));
  if (result == NULL) {
    return NULL;
  }
  if (count != 0) {
    result->entries =
        (shoal_bridge_bytes_entry *)calloc(count, sizeof(*result->entries));
    if (result->entries == NULL) {
      free(result);
      return NULL;
    }
  }
  result->count = count;
  return result;
}

int shoal_bridge_bytes_list_set(shoal_bytes_list_result *result, size_t index,
                                const uint8_t *data, size_t length) {
  if (result == NULL || index >= result->count ||
      (data == NULL && length != 0)) {
    return 0;
  }
  uint8_t *copy = shoal_bridge_copy_bytes(data, length);
  if (length != 0 && copy == NULL) {
    return 0;
  }
  free(result->entries[index].data);
  result->entries[index].data = copy;
  result->entries[index].length = length;
  return 1;
}

size_t shoal_bridge_bytes_list_count(const shoal_bytes_list_result *result) {
  return result == NULL ? 0 : result->count;
}

int shoal_bridge_bytes_list_get(const shoal_bytes_list_result *result,
                                size_t index, shoal_bytes *out_value) {
  if (result == NULL || index >= result->count || out_value == NULL) {
    return 0;
  }
  out_value->data = result->entries[index].data;
  out_value->length = result->entries[index].length;
  return 1;
}

void shoal_bridge_bytes_list_free(shoal_bytes_list_result *result) {
  if (result == NULL) {
    return;
  }
  for (size_t index = 0; index < result->count; index++) {
    free(result->entries[index].data);
  }
  free(result->entries);
  memset(result, 0, sizeof(*result));
  free(result);
}

shoal_error *shoal_bridge_error_alloc(
    shoal_status code, const char *message, size_t message_length,
    const char *security_user, size_t security_user_length,
    const char *security_code, size_t security_code_length) {
  if ((message == NULL && message_length != 0) || message_length == SIZE_MAX ||
      (security_user == NULL && security_user_length != 0) ||
      security_user_length == SIZE_MAX ||
      (security_code == NULL && security_code_length != 0) ||
      security_code_length == SIZE_MAX) {
    return NULL;
  }
  SHOAL_BRIDGE_TEST_ALLOC_GUARD(shoal_bridge_error_alloc_fail_after);
  shoal_error *error = (shoal_error *)malloc(sizeof(*error));
  if (error == NULL) {
    return NULL;
  }
#ifdef SHOAL_CAPI_TEST
  if (!shoal_bridge_allocation_allowed(
          &shoal_bridge_error_message_alloc_fail_after)) {
    free(error);
    return NULL;
  }
#endif
  memset(error, 0, sizeof(*error));
  error->message = (char *)malloc(message_length + 1);
  error->security_user = (char *)malloc(security_user_length + 1);
  error->security_code = (char *)malloc(security_code_length + 1);
  if (error->message == NULL || error->security_user == NULL ||
      error->security_code == NULL) {
    free(error->message);
    free(error->security_user);
    free(error->security_code);
    free(error);
    return NULL;
  }
  if (message_length != 0) {
    memcpy(error->message, message, message_length);
  }
  if (security_user_length != 0) {
    memcpy(error->security_user, security_user, security_user_length);
  }
  if (security_code_length != 0) {
    memcpy(error->security_code, security_code, security_code_length);
  }
  error->message[message_length] = '\0';
  error->security_user[security_user_length] = '\0';
  error->security_code[security_code_length] = '\0';
  error->code = code;
  return error;
}

shoal_status shoal_bridge_error_code(const shoal_error *error) {
  return error == NULL ? SHOAL_STATUS_INVALID_ARGUMENT : error->code;
}

char *shoal_bridge_error_message(const shoal_error *error) {
  static char empty[] = "";
  return error == NULL ? empty : error->message;
}

char *shoal_bridge_error_security_user(const shoal_error *error) {
  return error == NULL || error->security_user[0] == '\0'
             ? NULL
             : error->security_user;
}

char *shoal_bridge_error_security_code(const shoal_error *error) {
  return error == NULL || error->security_code[0] == '\0'
             ? NULL
             : error->security_code;
}

void shoal_bridge_error_free(shoal_error *error) {
  if (error != NULL) {
    free(error->message);
    free(error->security_user);
    free(error->security_code);
    error->message = NULL;
    error->security_user = NULL;
    error->security_code = NULL;
    free(error);
  }
}

void shoal_bridge_connector_config_init(shoal_connector_config *config) {
  if (config != NULL) {
    memset(config, 0, SHOAL_CONNECTOR_CONFIG_V1_SIZE);
    config->struct_size = SHOAL_CONNECTOR_CONFIG_V1_SIZE;
  }
}

uint32_t shoal_bridge_connector_config_v1_size(void) {
  return SHOAL_CONNECTOR_CONFIG_V1_SIZE;
}

void shoal_bridge_scanner_config_init(shoal_scanner_config *config) {
  if (config != NULL) {
    memset(config, 0, SHOAL_SCANNER_CONFIG_V1_SIZE);
    config->struct_size = SHOAL_SCANNER_CONFIG_V1_SIZE;
  }
}

uint32_t shoal_bridge_scanner_config_v1_size(void) {
  return SHOAL_SCANNER_CONFIG_V1_SIZE;
}

void shoal_bridge_range_init(shoal_range *range) {
  if (range != NULL) {
    memset(range, 0, SHOAL_RANGE_V1_SIZE);
    range->struct_size = SHOAL_RANGE_V1_SIZE;
  }
}

uint32_t shoal_bridge_range_v1_size(void) {
  return SHOAL_RANGE_V1_SIZE;
}

void shoal_bridge_batch_writer_config_init(shoal_batch_writer_config *config) {
  if (config != NULL) {
    memset(config, 0, SHOAL_BATCH_WRITER_CONFIG_V1_SIZE);
    config->struct_size = SHOAL_BATCH_WRITER_CONFIG_V1_SIZE;
  }
}

uint32_t shoal_bridge_batch_writer_config_v1_size(void) {
  return SHOAL_BATCH_WRITER_CONFIG_V1_SIZE;
}
