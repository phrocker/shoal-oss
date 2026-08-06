#include "shoal.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

static void expect_error(shoal_status status, shoal_status expected,
                         shoal_error **error, const char *message_part) {
  assert(status == expected);
  assert(error != NULL);
  assert(*error != NULL);
  assert(shoal_error_code(*error) == expected);
  assert(strstr(shoal_error_message(*error), message_part) != NULL);
  shoal_error_free(error);
  assert(*error == NULL);
}

int main(void) {
  shoal_connector *connector = NULL;
  shoal_error *error = NULL;

  assert(shoal_abi_version() == SHOAL_ABI_VERSION);
  expect_error(shoal_connector_create(NULL, &connector, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "config");
  assert(connector == NULL);

  shoal_connector_config config;
  shoal_connector_config_init(&config);
  assert(config.struct_size == sizeof(config));
  assert(config.struct_size >= SHOAL_CONNECTOR_CONFIG_V1_SIZE);

  config.bootstrap = SHOAL_BOOTSTRAP_STATIC;
  config.instance_name = "accumulo";
  config.instance_id = "00000000-0000-0000-0000-000000000001";
  config.principal = "root";
  {
    static const uint8_t password[] = {'s', 'e', 'c', '\0', 'r', 'e', 't'};
    config.password = password;
    config.password_length = sizeof(password);
  }

  config.accumulo_version = "2.1.6";
  expect_error(shoal_connector_create(&config, &connector, &error),
               SHOAL_STATUS_UNSUPPORTED, &error, "Accumulo 4");
  assert(connector == NULL);

  config.accumulo_version = NULL;
  assert(shoal_connector_create(&config, &connector, &error) ==
         SHOAL_STATUS_OK);
  assert(connector != NULL);
  assert(error == NULL);

  assert(shoal_connector_close(connector, &error) == SHOAL_STATUS_OK);
  assert(error == NULL);
  assert(shoal_connector_close(connector, &error) == SHOAL_STATUS_OK);
  assert(error == NULL);

  shoal_connector_free(&connector);
  assert(connector == NULL);
  shoal_connector_free(&connector);

  shoal_connector_config_init(&config);
  config.struct_size = SHOAL_CONNECTOR_CONFIG_V1_SIZE - 1;
  expect_error(shoal_connector_create(&config, &connector, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "struct_size");

  return 0;
}
