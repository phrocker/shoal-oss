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
