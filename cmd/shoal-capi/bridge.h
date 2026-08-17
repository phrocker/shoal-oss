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

struct shoal_error {
  shoal_status code;
  char *message;
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

shoal_connector *shoal_bridge_connector_alloc(uint64_t id);
uint64_t shoal_bridge_connector_id(const shoal_connector *connector);
void shoal_bridge_connector_free(shoal_connector *connector);

shoal_scanner *shoal_bridge_scanner_alloc(uint64_t id);
uint64_t shoal_bridge_scanner_id(const shoal_scanner *scanner);
void shoal_bridge_scanner_free(shoal_scanner *scanner);

shoal_batch_scanner *shoal_bridge_batch_scanner_alloc(uint64_t id);
uint64_t shoal_bridge_batch_scanner_id(const shoal_batch_scanner *scanner);
void shoal_bridge_batch_scanner_free(shoal_batch_scanner *scanner);

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

shoal_error *shoal_bridge_error_alloc(shoal_status code, const char *message,
                                      size_t message_length);
shoal_status shoal_bridge_error_code(const shoal_error *error);
char *shoal_bridge_error_message(const shoal_error *error);
void shoal_bridge_error_free(shoal_error *error);

void shoal_bridge_connector_config_init(shoal_connector_config *config);
uint32_t shoal_bridge_connector_config_v1_size(void);
void shoal_bridge_scanner_config_init(shoal_scanner_config *config);
uint32_t shoal_bridge_scanner_config_v1_size(void);
void shoal_bridge_range_init(shoal_range *range);
uint32_t shoal_bridge_range_v1_size(void);

#endif
