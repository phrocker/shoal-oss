#include "bridge.h"

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
  next.row = shoal_bridge_copy_bytes(row, row_length);
  next.column_family =
      shoal_bridge_copy_bytes(column_family, column_family_length);
  next.column_qualifier =
      shoal_bridge_copy_bytes(column_qualifier, column_qualifier_length);
  next.column_visibility =
      shoal_bridge_copy_bytes(column_visibility, column_visibility_length);
  next.value = shoal_bridge_copy_bytes(value, value_length);
  if ((row_length != 0 && next.row == NULL) ||
      (column_family_length != 0 && next.column_family == NULL) ||
      (column_qualifier_length != 0 && next.column_qualifier == NULL) ||
      (column_visibility_length != 0 && next.column_visibility == NULL) ||
      (value_length != 0 && next.value == NULL)) {
    shoal_bridge_scan_entry_clear(&next);
    return 0;
  }
  next.row_length = row_length;
  next.column_family_length = column_family_length;
  next.column_qualifier_length = column_qualifier_length;
  next.column_visibility_length = column_visibility_length;
  next.timestamp = timestamp;
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

shoal_error *shoal_bridge_error_alloc(shoal_status code, const char *message,
                                      size_t message_length) {
  if (message_length == SIZE_MAX) {
    return NULL;
  }
  shoal_error *error = (shoal_error *)malloc(sizeof(*error));
  if (error == NULL) {
    return NULL;
  }
  error->message = (char *)malloc(message_length + 1);
  if (error->message == NULL) {
    free(error);
    return NULL;
  }
  if (message_length != 0) {
    memcpy(error->message, message, message_length);
  }
  error->message[message_length] = '\0';
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

void shoal_bridge_error_free(shoal_error *error) {
  if (error != NULL) {
    free(error->message);
    error->message = NULL;
    free(error);
  }
}

void shoal_bridge_connector_config_init(shoal_connector_config *config) {
  if (config != NULL) {
    memset(config, 0, sizeof(*config));
    config->struct_size = (uint32_t)sizeof(*config);
  }
}

uint32_t shoal_bridge_connector_config_v1_size(void) {
  return SHOAL_CONNECTOR_CONFIG_V1_SIZE;
}

void shoal_bridge_scanner_config_init(shoal_scanner_config *config) {
  if (config != NULL) {
    memset(config, 0, sizeof(*config));
    config->struct_size = (uint32_t)sizeof(*config);
  }
}

uint32_t shoal_bridge_scanner_config_v1_size(void) {
  return SHOAL_SCANNER_CONFIG_V1_SIZE;
}

void shoal_bridge_range_init(shoal_range *range) {
  if (range != NULL) {
    memset(range, 0, sizeof(*range));
    range->struct_size = (uint32_t)sizeof(*range);
  }
}

uint32_t shoal_bridge_range_v1_size(void) {
  return SHOAL_RANGE_V1_SIZE;
}
