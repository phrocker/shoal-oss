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
  shoal_mutation *mutation = NULL;
  shoal_batch_writer *writer = NULL;
  shoal_write_failure *write_failure = NULL;
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

  shoal_batch_writer_config writer_config;
  shoal_batch_writer_config_init(&writer_config);
  assert(writer_config.struct_size == sizeof(writer_config));
  assert(writer_config.struct_size >= SHOAL_BATCH_WRITER_CONFIG_V1_SIZE);
  writer_config.table_name = "events";
  expect_error(shoal_connector_create_batch_writer(
                   connector, &writer_config, &writer, &error),
               SHOAL_STATUS_DISCOVERY_UNAVAILABLE, &error,
               "discovery unavailable");
  assert(writer == NULL);

  writer_config.durability = 99;
  expect_error(shoal_connector_create_batch_writer(
                   connector, &writer_config, &writer, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "durability");
  writer_config.durability = SHOAL_DURABILITY_DEFAULT;
  writer_config.max_latency_ms = -1;
  expect_error(shoal_connector_create_batch_writer(
                   connector, &writer_config, &writer, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "max_latency_ms");
  writer_config.max_latency_ms = 0;

  static const uint8_t mutation_row[] = {'r', '\0', 'w'};
  static const uint8_t family[] = {'c', 'f'};
  static const uint8_t qualifier[] = {'c', 'q'};
  static const uint8_t visibility[] = {'A', '&', 'B'};
  static const uint8_t mutation_value[] = {'v', '\0', 'l'};
  shoal_bytes row_bytes = {mutation_row, sizeof(mutation_row)};
  shoal_bytes family_bytes = {family, sizeof(family)};
  shoal_bytes qualifier_bytes = {qualifier, sizeof(qualifier)};
  shoal_bytes visibility_bytes = {visibility, sizeof(visibility)};
  shoal_bytes value_bytes = {mutation_value, sizeof(mutation_value)};
  size_t mutation_size = SIZE_MAX;

  assert(shoal_mutation_create(row_bytes, &mutation, &error) ==
         SHOAL_STATUS_OK);
  assert(mutation != NULL && error == NULL);
  assert(shoal_mutation_size(mutation, &mutation_size, &error) ==
         SHOAL_STATUS_OK);
  assert(mutation_size == 0);
  assert(shoal_mutation_put(mutation, family_bytes, qualifier_bytes,
                            visibility_bytes, 42, value_bytes,
                            &error) == SHOAL_STATUS_OK);
  assert(shoal_mutation_delete_latest(mutation, family_bytes, qualifier_bytes,
                                      visibility_bytes,
                                      &error) == SHOAL_STATUS_OK);
  assert(shoal_mutation_size(mutation, &mutation_size, &error) ==
         SHOAL_STATUS_OK);
  assert(mutation_size == 2);

  shoal_bytes malformed = {NULL, 1};
  expect_error(shoal_mutation_put_latest(
                   mutation, family_bytes, qualifier_bytes, visibility_bytes,
                   malformed, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "value");
  shoal_mutation_free(&mutation);
  assert(mutation == NULL);
  shoal_mutation_free(&mutation);

  expect_error(shoal_batch_writer_close(NULL, 0, &write_failure, &error),
               SHOAL_STATUS_INVALID_HANDLE, &error, "NULL");
  assert(write_failure == NULL);
  assert(shoal_write_failure_get_flags(NULL) == 0);
  assert(shoal_write_failure_failed_extent_count(NULL) == 0);
  shoal_failed_extent_view failed_extent;
  expect_error(shoal_write_failure_get_failed_extent(
                   NULL, 0, &failed_extent, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "write failure");
  shoal_batch_writer_free(&writer);
  shoal_write_failure_free(&write_failure);

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
