#ifndef SHOAL_CAPI_BRIDGE_H
#define SHOAL_CAPI_BRIDGE_H

#include "shoal_types.h"

struct shoal_connector {
  uint64_t id;
};

struct shoal_error {
  shoal_status code;
  char *message;
};

shoal_connector *shoal_bridge_connector_alloc(uint64_t id);
uint64_t shoal_bridge_connector_id(const shoal_connector *connector);
void shoal_bridge_connector_free(shoal_connector *connector);

shoal_error *shoal_bridge_error_alloc(shoal_status code, const char *message,
                                      size_t message_length);
shoal_status shoal_bridge_error_code(const shoal_error *error);
char *shoal_bridge_error_message(const shoal_error *error);
void shoal_bridge_error_free(shoal_error *error);

void shoal_bridge_connector_config_init(shoal_connector_config *config);
uint32_t shoal_bridge_connector_config_v1_size(void);

#endif
