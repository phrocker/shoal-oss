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
  SHOAL_STATUS_INTERNAL = 255
};

typedef int32_t shoal_bootstrap;

enum {
  SHOAL_BOOTSTRAP_UNSPECIFIED = 0,
  SHOAL_BOOTSTRAP_STATIC = 1,
  SHOAL_BOOTSTRAP_ZOOKEEPER = 2
};

typedef struct shoal_connector shoal_connector;
typedef struct shoal_error shoal_error;

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

#endif
