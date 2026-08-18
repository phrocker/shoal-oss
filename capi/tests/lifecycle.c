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
  shoal_scanner *scanner = NULL;
  shoal_batch_scanner *batch_scanner = NULL;
  shoal_scan_result *result = NULL;
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

  shoal_scanner_config scanner_config;
  shoal_scanner_config_init(&scanner_config);
  assert(scanner_config.struct_size == sizeof(scanner_config));
  assert(scanner_config.struct_size >= SHOAL_SCANNER_CONFIG_V1_SIZE);
  scanner_config.table_name = "events";

  expect_error(
      shoal_connector_create_scanner(connector, &scanner_config, &scanner,
                                     &error),
      SHOAL_STATUS_DISCOVERY_UNAVAILABLE, &error, "discovery unavailable");
  assert(scanner == NULL);
  expect_error(shoal_connector_create_batch_scanner(
                   connector, &scanner_config, &batch_scanner, &error),
               SHOAL_STATUS_DISCOVERY_UNAVAILABLE, &error,
               "discovery unavailable");
  assert(batch_scanner == NULL);

  scanner_config.table_id = "1";
  expect_error(
      shoal_connector_create_scanner(connector, &scanner_config, &scanner,
                                     &error),
      SHOAL_STATUS_INVALID_ARGUMENT, &error, "exactly one");
  scanner_config.table_id = NULL;

  scanner_config.authorization_count = 1;
  expect_error(
      shoal_connector_create_scanner(connector, &scanner_config, &scanner,
                                     &error),
      SHOAL_STATUS_INVALID_ARGUMENT, &error, "authorizations");
  scanner_config.authorization_count = 0;

  scanner_config.use_multi_scan = 2;
  expect_error(
      shoal_connector_create_scanner(connector, &scanner_config, &scanner,
                                     &error),
      SHOAL_STATUS_INVALID_ARGUMENT, &error, "use_multi_scan");
  scanner_config.use_multi_scan = 0;

  shoal_range range;
  shoal_range_init(&range);
  assert(range.struct_size == sizeof(range));
  assert(range.struct_size >= SHOAL_RANGE_V1_SIZE);

  assert(shoal_scan_result_count(NULL) == 0);
  expect_error(shoal_scan_result_get(NULL, 0, NULL, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "result");
  shoal_scan_result_free(&result);
  assert(result == NULL);

  expect_error(shoal_scanner_close(NULL, &error), SHOAL_STATUS_INVALID_HANDLE,
               &error, "NULL");
  expect_error(shoal_batch_scanner_close(NULL, &error),
               SHOAL_STATUS_INVALID_HANDLE, &error, "NULL");
  shoal_scanner_free(&scanner);
  shoal_batch_scanner_free(&batch_scanner);

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
