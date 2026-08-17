#ifndef SHOAL_TYPES_H
#define SHOAL_TYPES_H

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define SHOAL_CALL __cdecl
#if defined(SHOAL_BUILDING_LIBRARY)
#define SHOAL_API __declspec(dllexport)
#else
#define SHOAL_API __declspec(dllimport)
#endif
#else
#define SHOAL_CALL
#define SHOAL_API __attribute__((visibility("default")))
#endif

#define SHOAL_ABI_VERSION 1u

typedef int32_t shoal_status;

enum {
  SHOAL_STATUS_OK = 0,
  SHOAL_STATUS_INVALID_ARGUMENT = 1,
  SHOAL_STATUS_INVALID_HANDLE = 2,
  SHOAL_STATUS_OUT_OF_MEMORY = 3,
  SHOAL_STATUS_UNSUPPORTED = 4,
  SHOAL_STATUS_BOOTSTRAP_FAILED = 5,
  SHOAL_STATUS_CLOSED = 6,
  SHOAL_STATUS_CANCELLED = 7,
  SHOAL_STATUS_DEADLINE_EXCEEDED = 8,
  SHOAL_STATUS_NOT_FOUND = 9,
  SHOAL_STATUS_PERMISSION_DENIED = 10,
  SHOAL_STATUS_DISCOVERY_UNAVAILABLE = 11,
  SHOAL_STATUS_TABLET_UNAVAILABLE = 12,
  SHOAL_STATUS_RANGE_SPANS_TABLETS = 13,
  SHOAL_STATUS_CLEANUP_FAILED = 14,
  SHOAL_STATUS_OPERATION_FAILED = 15,
  SHOAL_STATUS_INTERNAL = 255
};

typedef int32_t shoal_bootstrap;

enum {
  SHOAL_BOOTSTRAP_UNSPECIFIED = 0,
  SHOAL_BOOTSTRAP_STATIC = 1,
  SHOAL_BOOTSTRAP_ZOOKEEPER = 2
};

typedef struct shoal_connector shoal_connector;
typedef struct shoal_scanner shoal_scanner;
typedef struct shoal_batch_scanner shoal_batch_scanner;
typedef struct shoal_scan_result shoal_scan_result;
typedef struct shoal_error shoal_error;

typedef struct shoal_bytes {
  const uint8_t *data;
  size_t length;
} shoal_bytes;

typedef struct shoal_column {
  shoal_bytes family;
  shoal_bytes qualifier;
  uint8_t has_qualifier;
} shoal_column;

typedef struct shoal_iterator_option {
  const char *key;
  const char *value;
} shoal_iterator_option;

typedef struct shoal_iterator_setting {
  const char *name;
  const char *class_name;
  int32_t priority;
  const shoal_iterator_option *options;
  size_t option_count;
} shoal_iterator_setting;

typedef struct shoal_key {
  shoal_bytes row;
  shoal_bytes column_family;
  shoal_bytes column_qualifier;
  shoal_bytes column_visibility;
  int64_t timestamp;
} shoal_key;

typedef int32_t shoal_range_bound_kind;

enum {
  SHOAL_RANGE_BOUND_UNBOUNDED = 0,
  SHOAL_RANGE_BOUND_ROW = 1,
  SHOAL_RANGE_BOUND_KEY = 2
};

typedef struct shoal_range_bound {
  shoal_range_bound_kind kind;
  shoal_bytes row;
  shoal_key key;
} shoal_range_bound;

/*
 * All pointer fields are borrowed for the duration of
 * shoal_connector_create. The library copies every value it retains.
 *
 * Set struct_size with shoal_connector_config_init. Future ABI versions may
 * append fields; version 1 readers ignore bytes beyond the known structure.
 */
typedef struct shoal_connector_config {
  uint32_t struct_size;
  shoal_bootstrap bootstrap;
  const char *instance_name;
  const char *instance_id;
  const char *zookeeper_servers;
  const char *principal;
  const uint8_t *password;
  size_t password_length;
  const char *accumulo_version;
  int64_t zookeeper_session_timeout_ms;
  int64_t bootstrap_timeout_ms;
  const char *instance_secret;
  int64_t dial_timeout_ms;
} shoal_connector_config;

#define SHOAL_CONNECTOR_CONFIG_V1_SIZE                                      \
  ((uint32_t)(offsetof(shoal_connector_config, dial_timeout_ms) +            \
              sizeof(((shoal_connector_config *)0)->dial_timeout_ms)))

/*
 * Exactly one of table_name and table_id must be non-NULL and non-empty.
 * Arrays and all memory they reference are borrowed only during scanner
 * creation. Shoal copies every retained value.
 */
typedef struct shoal_scanner_config {
  uint32_t struct_size;
  const char *table_name;
  const char *table_id;
  int32_t batch_size;
  const shoal_bytes *authorizations;
  size_t authorization_count;
  const shoal_column *columns;
  size_t column_count;
  const shoal_iterator_setting *iterators;
  size_t iterator_count;
  int32_t parallelism;
  uint8_t use_multi_scan;
} shoal_scanner_config;

#define SHOAL_SCANNER_CONFIG_V1_SIZE                                        \
  ((uint32_t)(offsetof(shoal_scanner_config, use_multi_scan) +               \
              sizeof(((shoal_scanner_config *)0)->use_multi_scan)))

/*
 * ROW bounds include/exclude an entire row. KEY bounds use full Accumulo key
 * ordering. A range may not mix ROW and KEY bound kinds, though either side
 * may be UNBOUNDED. Pointer fields are borrowed only during a scan call.
 */
typedef struct shoal_range {
  uint32_t struct_size;
  shoal_range_bound start;
  shoal_range_bound end;
  uint8_t start_inclusive;
  uint8_t end_inclusive;
} shoal_range;

#define SHOAL_RANGE_V1_SIZE                                                  \
  ((uint32_t)(offsetof(shoal_range, end_inclusive) +                         \
              sizeof(((shoal_range *)0)->end_inclusive)))

/*
 * Every pointer in a key/value view is borrowed from its shoal_scan_result
 * and remains valid until that result is freed.
 */
typedef struct shoal_key_value_view {
  shoal_bytes row;
  shoal_bytes column_family;
  shoal_bytes column_qualifier;
  shoal_bytes column_visibility;
  int64_t timestamp;
  shoal_bytes value;
} shoal_key_value_view;

#endif
