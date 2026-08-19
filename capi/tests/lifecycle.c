#include "shoal.h"
#include "test_seam.h"

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

_Static_assert(SHOAL_ABI_VERSION == 1u, "unexpected compatibility ABI version");
_Static_assert(SHOAL_ABI_VERSION_MAJOR == 1u, "unexpected ABI major");
_Static_assert(SHOAL_ABI_VERSION_MINOR == 1u, "unexpected ABI minor");
_Static_assert(SHOAL_ABI_VERSION_PATCH == 0u, "unexpected ABI patch");
_Static_assert(SHOAL_ABI_VERSION_PACKED == 0x00010100u,
               "unexpected packed ABI version");
_Static_assert(SHOAL_ABI_CAPABILITY_CONNECTOR == 0u,
               "unexpected connector capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_BOOTSTRAP == 1u,
               "unexpected bootstrap capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_ERROR == 2u,
               "unexpected error capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_SCANNER == 3u,
               "unexpected scanner capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_BATCH_SCANNER == 4u,
               "unexpected batch scanner capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_OWNED_SCAN_RESULT == 5u,
               "unexpected scan result capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_MUTATION == 6u,
               "unexpected mutation capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_BATCH_WRITER == 7u,
               "unexpected batch writer capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_STRUCTURED_WRITE_FAILURE == 8u,
               "unexpected structured write failure capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_TABLE_ADMIN == 9u,
               "unexpected table admin capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_NAMESPACE_ADMIN == 10u,
               "unexpected namespace admin capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_SECURITY_ADMIN == 11u,
               "unexpected security admin capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_TABLE_SPLITS == 12u,
               "unexpected table splits capability id");
_Static_assert(SHOAL_ABI_CAPABILITY_COUNT == 13u,
               "unexpected capability count");
_Static_assert(SHOAL_ABI_CAPABILITY_WORD_COUNT == 1u,
               "unexpected capability word count");
_Static_assert(SHOAL_ABI_CAPABILITY_WORD0 == UINT64_C(0x1fff),
               "unexpected capability word 0");

#define ASSERT_PERMISSION_VALUE(name, value)                                  \
  _Static_assert(name == value, "unexpected permission ordinal: " #name)

ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_GRANT, 0);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_CREATE_TABLE, 1);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_DROP_TABLE, 2);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_ALTER_TABLE, 3);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_CREATE_USER, 4);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_DROP_USER, 5);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_ALTER_USER, 6);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_SYSTEM, 7);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_CREATE_NAMESPACE, 8);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_DROP_NAMESPACE, 9);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_ALTER_NAMESPACE, 10);
ASSERT_PERMISSION_VALUE(SHOAL_SYSTEM_PERMISSION_OBTAIN_DELEGATION_TOKEN, 11);
ASSERT_PERMISSION_VALUE(SHOAL_TABLE_PERMISSION_READ, 2);
ASSERT_PERMISSION_VALUE(SHOAL_TABLE_PERMISSION_WRITE, 3);
ASSERT_PERMISSION_VALUE(SHOAL_TABLE_PERMISSION_BULK_IMPORT, 4);
ASSERT_PERMISSION_VALUE(SHOAL_TABLE_PERMISSION_ALTER_TABLE, 5);
ASSERT_PERMISSION_VALUE(SHOAL_TABLE_PERMISSION_GRANT, 6);
ASSERT_PERMISSION_VALUE(SHOAL_TABLE_PERMISSION_DROP_TABLE, 7);
ASSERT_PERMISSION_VALUE(SHOAL_TABLE_PERMISSION_GET_SUMMARIES, 8);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_READ, 0);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_WRITE, 1);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_ALTER_NAMESPACE, 2);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_GRANT, 3);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_ALTER_TABLE, 4);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_CREATE_TABLE, 5);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_DROP_TABLE, 6);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_BULK_IMPORT, 7);
ASSERT_PERMISSION_VALUE(SHOAL_NAMESPACE_PERMISSION_DROP_NAMESPACE, 8);

#undef ASSERT_PERMISSION_VALUE

static void expect_error(shoal_status status, shoal_status expected,
                         shoal_error **error, const char *message_part) {
  if (status != expected) {
    fprintf(stderr, "status %d, expected %d for %s\n", status, expected,
            message_part);
  }
  assert(status == expected);
  assert(error != NULL);
  assert(*error != NULL);
  assert(shoal_error_code(*error) == expected);
  assert(strstr(shoal_error_message(*error), message_part) != NULL);
  shoal_error_free(error);
  assert(*error == NULL);
}

static void expect_v1_init(const void *value, size_t allocation_size,
                           uint32_t v1_size) {
  const uint8_t *bytes = (const uint8_t *)value;
  uint32_t struct_size = 0;
  memcpy(&struct_size, value, sizeof(struct_size));
  assert(struct_size == v1_size);
  for (size_t i = sizeof(struct_size); i < v1_size; ++i) {
    assert(bytes[i] == 0);
  }
  for (size_t i = v1_size; i < allocation_size; ++i) {
    assert(bytes[i] == UINT8_C(0xa5));
  }
}

static void test_v1_initializers(void) {
#define CHECK_V1_INIT(type, init, v1_size)                                   \
  do {                                                                       \
    struct {                                                                 \
      type value;                                                            \
      uint8_t guard[16];                                                     \
    } allocation;                                                            \
    memset(&allocation, 0xa5, sizeof(allocation));                            \
    init(&allocation.value);                                                 \
    expect_v1_init(&allocation.value, sizeof(allocation), v1_size);           \
  } while (0)

  CHECK_V1_INIT(shoal_connector_config, shoal_connector_config_init,
                SHOAL_CONNECTOR_CONFIG_V1_SIZE);
  CHECK_V1_INIT(shoal_scanner_config, shoal_scanner_config_init,
                SHOAL_SCANNER_CONFIG_V1_SIZE);
  CHECK_V1_INIT(shoal_range, shoal_range_init, SHOAL_RANGE_V1_SIZE);
  CHECK_V1_INIT(shoal_batch_writer_config, shoal_batch_writer_config_init,
                SHOAL_BATCH_WRITER_CONFIG_V1_SIZE);

#undef CHECK_V1_INIT
}

int main(void) {
  shoal_connector *connector = NULL;
  shoal_connector *admin_connector = NULL;
  shoal_scanner *scanner = NULL;
  shoal_batch_scanner *batch_scanner = NULL;
  shoal_scan_result *result = NULL;
  shoal_table_list_result *table_list = NULL;
  shoal_mutation *mutation = NULL;
  shoal_batch_writer *writer = NULL;
  shoal_write_failure *write_failure = NULL;
  shoal_table_properties_result *properties = NULL;
  shoal_namespace_list_result *namespace_list = NULL;
  shoal_namespace_properties_result *namespace_properties = NULL;
  shoal_versioned_properties_result *versioned_properties = NULL;
  shoal_bytes_list_result *bytes_list = NULL;
  shoal_error *error = NULL;

  test_v1_initializers();
  assert(shoal_abi_version() == SHOAL_ABI_VERSION);
  assert(shoal_abi_version_major() == SHOAL_ABI_VERSION_MAJOR);
  assert(shoal_abi_version_minor() == SHOAL_ABI_VERSION_MINOR);
  assert(shoal_abi_version_patch() == SHOAL_ABI_VERSION_PATCH);
  assert(shoal_abi_version_packed() == SHOAL_ABI_VERSION_PACKED);
  assert(shoal_abi_capability_count() == SHOAL_ABI_CAPABILITY_COUNT);
  assert(shoal_abi_capability_word_count() == SHOAL_ABI_CAPABILITY_WORD_COUNT);
  assert(shoal_abi_capability_word(0) == SHOAL_ABI_CAPABILITY_WORD0);
  assert(shoal_abi_capability_word(1) == 0);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_CONNECTOR) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_BOOTSTRAP) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_ERROR) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_SCANNER) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_BATCH_SCANNER) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_OWNED_SCAN_RESULT) ==
         1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_MUTATION) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_BATCH_WRITER) == 1);
  assert(shoal_abi_has_capability(
             SHOAL_ABI_CAPABILITY_STRUCTURED_WRITE_FAILURE) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_TABLE_ADMIN) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_COUNT) == 0);
  assert(shoal_abi_has_capability(63u) == 0);
  assert(shoal_abi_has_capability(64u) == 0);
  expect_error(shoal_connector_create(NULL, &connector, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "config");
  assert(connector == NULL);

  shoal_connector_config config;
  shoal_connector_config_init(&config);
  assert(config.struct_size == SHOAL_CONNECTOR_CONFIG_V1_SIZE);

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

  assert(shoal_test_connector_create(&admin_connector));
  assert(admin_connector != NULL);

  shoal_table_view table_view;
  assert(shoal_connector_list_tables(admin_connector, 0, &table_list, &error) ==
         SHOAL_STATUS_OK);
  assert(table_list != NULL && error == NULL);
  assert(shoal_table_list_count(table_list) == 2);
  assert(shoal_table_list_get(table_list, 0, &table_view, &error) ==
         SHOAL_STATUS_OK);
  assert(strcmp(table_view.name, "analytics.orders") == 0);
  assert(strcmp(table_view.id, "2") == 0);
  assert(shoal_table_list_get(table_list, 1, &table_view, &error) ==
         SHOAL_STATUS_OK);
  assert(strcmp(table_view.name, "events") == 0);
  assert(strcmp(table_view.id, "1") == 0);
  expect_error(shoal_table_list_get(table_list, 2, &table_view, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "out of bounds");
  shoal_table_list_free(&table_list);
  assert(table_list == NULL);

  shoal_test_result_alloc_fail_after(0);
  expect_error(shoal_connector_list_tables(admin_connector, 0, &table_list,
                                           &error),
               SHOAL_STATUS_OUT_OF_MEMORY, &error,
               "allocate table list result");
  assert(table_list == NULL);
  shoal_test_result_alloc_reset();

  shoal_test_result_alloc_fail_after(0);
  shoal_test_error_alloc_fail_after(0);
  assert(shoal_connector_list_tables(admin_connector, 0, &table_list, &error) ==
         SHOAL_STATUS_OUT_OF_MEMORY);
  assert(table_list == NULL && error == NULL);
  shoal_test_result_alloc_reset();
  shoal_test_error_alloc_reset();
  shoal_test_result_alloc_fail_after(0);
  shoal_test_error_alloc_fail_after(1);
  expect_error(shoal_connector_list_tables(admin_connector, 0, &table_list,
                                           &error),
               SHOAL_STATUS_OUT_OF_MEMORY, &error,
               "allocate table list result");
  assert(table_list == NULL);
  shoal_test_result_alloc_reset();
  shoal_test_error_alloc_reset();
  assert(shoal_connector_list_tables(admin_connector, 0, &table_list, &error) ==
         SHOAL_STATUS_OK);
  assert(table_list != NULL && error == NULL);
  shoal_table_list_free(&table_list);
  assert(table_list == NULL);

  shoal_test_string_alloc_fail_after(1);
  expect_error(shoal_connector_list_tables(admin_connector, 0, &table_list,
                                           &error),
               SHOAL_STATUS_OUT_OF_MEMORY, &error, "table 0 id");
  assert(table_list == NULL);
  shoal_test_string_alloc_reset();

  uint8_t exists = 99;
  assert(shoal_connector_table_exists(admin_connector, "events", 0, &exists,
                                      &error) == SHOAL_STATUS_OK);
  assert(exists == 1 && error == NULL);
  exists = 99;
  assert(shoal_connector_table_exists(admin_connector, "missing", 0, &exists,
                                      &error) == SHOAL_STATUS_OK);
  assert(exists == 0 && error == NULL);
  expect_error(shoal_connector_table_exists(admin_connector, "block", 1,
                                            &exists, &error),
               SHOAL_STATUS_DEADLINE_EXCEEDED, &error, "deadline exceeded");

  assert(shoal_connector_create_table(admin_connector, "created", 0, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_connector_table_exists(admin_connector, "created", 0, &exists,
                                      &error) == SHOAL_STATUS_OK);
  assert(exists == 1);
  expect_error(shoal_connector_create_table(admin_connector, "events", 0,
                                            &error),
               SHOAL_STATUS_ALREADY_EXISTS, &error, "table exists");
  assert(shoal_connector_rename_table(admin_connector, "created", "renamed", 0,
                                      &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_table_exists(admin_connector, "created", 0, &exists,
                                      &error) == SHOAL_STATUS_OK);
  assert(exists == 0);
  assert(shoal_connector_table_exists(admin_connector, "renamed", 0, &exists,
                                      &error) == SHOAL_STATUS_OK);
  assert(exists == 1);
  expect_error(shoal_connector_delete_table(admin_connector, "missing", 0,
                                            &error),
               SHOAL_STATUS_NOT_FOUND, &error, "table not found");
  assert(shoal_connector_delete_table(admin_connector, "renamed", 0, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_connector_table_exists(admin_connector, "renamed", 0, &exists,
                                      &error) == SHOAL_STATUS_OK);
  assert(exists == 0);

  expect_error(shoal_connector_flush_table(admin_connector, "events", 2, 0,
                                           &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "wait must be 0 or 1");
  expect_error(shoal_connector_flush_table(admin_connector, "events", 0, -1,
                                           &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error,
               "timeout_ms must not be negative");
  shoal_test_error_message_alloc_fail_after(0);
  assert(shoal_connector_flush_table(admin_connector, "events", 2, 0, &error) ==
         SHOAL_STATUS_OUT_OF_MEMORY);
  assert(error == NULL);
  shoal_test_error_message_alloc_reset();
  assert(shoal_connector_flush_table(admin_connector, "events", 0, 0, &error) ==
         SHOAL_STATUS_OK);
  assert(error == NULL);
  assert(shoal_connector_flush_table(admin_connector, "events", 1, 0, &error) ==
         SHOAL_STATUS_OK);
  assert(error == NULL);
  assert(shoal_test_connector_flush_wait_count(admin_connector, 0) == 1);
  assert(shoal_test_connector_flush_wait_count(admin_connector, 1) == 1);
  expect_error(shoal_connector_flush_table(admin_connector, "down", 0, 0,
                                           &error),
               SHOAL_STATUS_UNAVAILABLE, &error, "manager unavailable");

  assert(shoal_connector_set_table_property(
             admin_connector, "events", "table.custom.alpha", "alpha", 0,
             &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_set_table_property(
             admin_connector, "events", "table.custom.empty", "", 0,
             &error) == SHOAL_STATUS_OK);
  expect_error(shoal_connector_set_table_property(
                   admin_connector, "events", "invalid", "x", 0, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "invalid property");
  assert(shoal_connector_remove_table_property(
             admin_connector, "events", "table.custom.alpha", 0,
             &error) == SHOAL_STATUS_OK);

  shoal_table_property_view property_view;
  assert(shoal_connector_effective_table_properties(admin_connector, "events",
                                                    0, &properties,
                                                    &error) ==
         SHOAL_STATUS_OK);
  assert(properties != NULL && error == NULL);
  assert(shoal_table_properties_count(properties) == 2);
  assert(shoal_table_properties_get(properties, 0, &property_view, &error) ==
         SHOAL_STATUS_OK);
  assert(strcmp(property_view.key, "table.custom.empty") == 0);
  assert(strcmp(property_view.value, "") == 0);
  assert(shoal_table_properties_get(properties, 1, &property_view, &error) ==
         SHOAL_STATUS_OK);
  assert(strcmp(property_view.key, "table.custom.mode") == 0);
  assert(strcmp(property_view.value, "stream") == 0);
  expect_error(shoal_table_properties_get(properties, 2, &property_view,
                                          &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "out of bounds");
  shoal_table_properties_free(&properties);
  assert(properties == NULL);
  shoal_test_result_alloc_fail_after(0);
  shoal_test_error_alloc_fail_after(1);
  shoal_test_error_message_alloc_fail_after(0);
  assert(shoal_connector_effective_table_properties(
             admin_connector, "events", 0, &properties, &error) ==
         SHOAL_STATUS_OUT_OF_MEMORY);
  assert(properties == NULL && error == NULL);
  shoal_test_result_alloc_reset();
  shoal_test_error_alloc_reset();
  shoal_test_error_message_alloc_reset();
  assert(shoal_connector_effective_table_properties(
             admin_connector, "events", 0, &properties, &error) ==
         SHOAL_STATUS_OK);
  assert(properties != NULL && error == NULL);
  shoal_table_properties_free(&properties);
  assert(properties == NULL);
  shoal_test_string_alloc_fail_after(3);
  expect_error(shoal_connector_effective_table_properties(
                   admin_connector, "events", 0, &properties, &error),
               SHOAL_STATUS_OUT_OF_MEMORY, &error, "property 1 value");
  assert(properties == NULL);
  shoal_test_string_alloc_reset();
  expect_error(shoal_connector_effective_table_properties(
                   admin_connector, "down", 0, &properties, &error),
               SHOAL_STATUS_UNAVAILABLE, &error,
               "client service unavailable");
  expect_error(shoal_connector_effective_table_properties(
                   admin_connector, "denied", 0, &properties, &error),
               SHOAL_STATUS_PERMISSION_DENIED, &error, "permission denied");

  assert(shoal_connector_list_namespaces(admin_connector, 0, &namespace_list,
                                         &error) == SHOAL_STATUS_OK);
  assert(shoal_namespace_list_count(namespace_list) == 2);
  shoal_namespace_view namespace_view = {0};
  assert(shoal_namespace_list_get(namespace_list, 0, &namespace_view, &error) ==
         SHOAL_STATUS_OK);
  assert(strcmp(namespace_view.name, "") == 0);
  assert(strcmp(namespace_view.id, "+default") == 0);
  shoal_namespace_list_free(&namespace_list);
  assert(namespace_list == NULL);
  shoal_namespace_list_free(&namespace_list);
  shoal_test_result_alloc_fail_after(0);
  expect_error(shoal_connector_list_namespaces(
                   admin_connector, 0, &namespace_list, &error),
               SHOAL_STATUS_OUT_OF_MEMORY, &error, "allocate");
  shoal_test_result_alloc_reset();

  exists = 0;
  assert(shoal_connector_namespace_exists(admin_connector, "analytics", 0,
                                          &exists, &error) == SHOAL_STATUS_OK);
  assert(exists == 1);
  expect_error(shoal_connector_delete_namespace(admin_connector, "", 0,
                                                &error),
               SHOAL_STATUS_NAMESPACE_NOT_EMPTY, &error,
               "namespace not empty");
  assert(shoal_connector_create_namespace(admin_connector, "scratch", 0,
                                          &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_rename_namespace(admin_connector, "scratch", "work",
                                          0, &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_set_namespace_property(
             admin_connector, "work", "table.custom.mode", "", 0, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_connector_namespace_properties(
             admin_connector, "work", 0, &namespace_properties, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_namespace_properties_count(namespace_properties) == 1);
  assert(shoal_namespace_properties_get(namespace_properties, 0, &property_view,
                                        &error) == SHOAL_STATUS_OK);
  assert(strcmp(property_view.key, "table.custom.mode") == 0);
  assert(strcmp(property_view.value, "") == 0);
  shoal_namespace_properties_free(&namespace_properties);
  shoal_namespace_properties_free(&namespace_properties);
  assert(shoal_connector_effective_namespace_properties(
             admin_connector, "work", 0, &namespace_properties, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_namespace_properties_count(namespace_properties) == 1);
  shoal_namespace_properties_free(&namespace_properties);
  assert(shoal_connector_remove_namespace_property(
             admin_connector, "work", "table.custom.mode", 0, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_connector_namespace_properties(
             admin_connector, "work", 0, &namespace_properties, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_namespace_properties_count(namespace_properties) == 0);
  shoal_namespace_properties_free(&namespace_properties);
  assert(shoal_connector_versioned_namespace_properties(
             admin_connector, "analytics", 0, &versioned_properties, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_versioned_properties_version(versioned_properties) == 7);
  assert(shoal_versioned_properties_count(versioned_properties) == 1);
  shoal_versioned_properties_free(&versioned_properties);
  shoal_versioned_properties_free(&versioned_properties);
  assert(shoal_connector_delete_namespace(admin_connector, "work", 0,
                                          &error) == SHOAL_STATUS_OK);
  expect_error(shoal_connector_create_namespace(admin_connector, "block", 1,
                                                &error),
               SHOAL_STATUS_DEADLINE_EXCEEDED, &error, "deadline");

  shoal_bytes empty_password = {NULL, 0};
  assert(shoal_connector_create_user(admin_connector, "alice", &empty_password,
                                     0, &error) == SHOAL_STATUS_OK);
  expect_error(shoal_connector_change_user_authorizations(
                   admin_connector, "alice", NULL, 1, 0, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "authorizations");
  const uint8_t auth_a[] = {'A', 0, 'B'};
  shoal_bytes auths[] = {{auth_a, sizeof(auth_a)}};
  assert(shoal_connector_change_user_authorizations(
             admin_connector, "alice", auths, 1, 0, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_connector_get_user_authorizations(
             admin_connector, "alice", 0, &bytes_list, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_bytes_list_count(bytes_list) == 1);
  shoal_bytes bytes_view = {0};
  assert(shoal_bytes_list_get(bytes_list, 0, &bytes_view, &error) ==
         SHOAL_STATUS_OK);
  assert(bytes_view.length == sizeof(auth_a));
  assert(memcmp(bytes_view.data, auth_a, sizeof(auth_a)) == 0);
  shoal_bytes_list_free(&bytes_list);
  shoal_bytes_list_free(&bytes_list);
  assert(shoal_connector_grant_table_permission(
             admin_connector, "alice", "events", SHOAL_TABLE_PERMISSION_READ,
             0, &error) == SHOAL_STATUS_OK);
  uint8_t has_permission = 0;
  assert(shoal_connector_has_table_permission(
             admin_connector, "alice", "events", SHOAL_TABLE_PERMISSION_READ,
             0, &has_permission, &error) == SHOAL_STATUS_OK);
  assert(has_permission == 1);
  assert(shoal_connector_revoke_table_permission(
             admin_connector, "alice", "events", SHOAL_TABLE_PERMISSION_READ,
             0, &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_has_table_permission(
             admin_connector, "alice", "events", SHOAL_TABLE_PERMISSION_READ,
             0, &has_permission, &error) == SHOAL_STATUS_OK);
  assert(has_permission == 0);
  assert(shoal_connector_grant_system_permission(
             admin_connector, "alice", SHOAL_SYSTEM_PERMISSION_CREATE_TABLE, 0,
             &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_has_system_permission(
             admin_connector, "alice", SHOAL_SYSTEM_PERMISSION_CREATE_TABLE, 0,
             &has_permission, &error) == SHOAL_STATUS_OK);
  assert(has_permission == 1);
  assert(shoal_connector_revoke_system_permission(
             admin_connector, "alice", SHOAL_SYSTEM_PERMISSION_CREATE_TABLE, 0,
             &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_has_system_permission(
             admin_connector, "alice", SHOAL_SYSTEM_PERMISSION_CREATE_TABLE, 0,
             &has_permission, &error) == SHOAL_STATUS_OK);
  assert(has_permission == 0);
  assert(shoal_connector_grant_namespace_permission(
             admin_connector, "alice", "analytics",
             SHOAL_NAMESPACE_PERMISSION_READ, 0, &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_has_namespace_permission(
             admin_connector, "alice", "analytics",
             SHOAL_NAMESPACE_PERMISSION_READ, 0, &has_permission, &error) ==
         SHOAL_STATUS_OK);
  assert(has_permission == 1);
  assert(shoal_connector_revoke_namespace_permission(
             admin_connector, "alice", "analytics",
             SHOAL_NAMESPACE_PERMISSION_READ, 0, &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_has_namespace_permission(
             admin_connector, "alice", "analytics",
             SHOAL_NAMESPACE_PERMISSION_READ, 0, &has_permission, &error) ==
         SHOAL_STATUS_OK);
  assert(has_permission == 0);
  assert(shoal_connector_change_password(admin_connector, "missing",
                                         &empty_password, 0, &error) ==
         SHOAL_STATUS_USER_NOT_FOUND);
  assert(error != NULL);
  assert(strstr(shoal_error_message(error), "security error") != NULL);
  assert(strcmp(shoal_error_security_user(error), "missing") == 0);
  assert(strcmp(shoal_error_security_code(error), "USER_DOESNT_EXIST") == 0);
  shoal_error_free(&error);
  assert(shoal_connector_drop_user(admin_connector, "alice", 0, &error) ==
         SHOAL_STATUS_OK);

  assert(shoal_connector_list_table_splits(admin_connector, "events", 0,
                                           &bytes_list, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_bytes_list_count(bytes_list) == 1);
  shoal_bytes_list_free(&bytes_list);
  const uint8_t split_one[] = {0, 'x'};
  const uint8_t split_two[] = {'z'};
  shoal_bytes splits[] = {{split_one, sizeof(split_one)},
                          {split_two, sizeof(split_two)}};
  assert(shoal_connector_add_table_splits(admin_connector, "events", splits, 2,
                                          0, &error) == SHOAL_STATUS_OK);
  assert(shoal_connector_list_table_splits(admin_connector, "events", 0,
                                           &bytes_list, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_bytes_list_count(bytes_list) == 3);
  shoal_bytes_list_free(&bytes_list);
  expect_error(shoal_connector_add_table_splits(admin_connector, "events",
                                                NULL, 0, 0, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "table split");

  shoal_scanner_config scanner_config;
  shoal_scanner_config_init(&scanner_config);
  assert(scanner_config.struct_size == SHOAL_SCANNER_CONFIG_V1_SIZE);
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
  assert(range.struct_size == SHOAL_RANGE_V1_SIZE);

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
  assert(writer_config.struct_size == SHOAL_BATCH_WRITER_CONFIG_V1_SIZE);
  writer_config.table_name = "events";
  uint32_t writer_config_size = writer_config.struct_size;
  writer_config.struct_size = SHOAL_BATCH_WRITER_CONFIG_V1_SIZE - 1;
  expect_error(shoal_connector_create_batch_writer(
                   connector, &writer_config, &writer, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "struct_size");
  writer_config.struct_size = writer_config_size;
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

  assert(shoal_test_batch_writer_create(SHOAL_TEST_WRITER_SUCCESS, &writer));
  assert(writer != NULL);
  shoal_mutation *empty_mutation = NULL;
  static const uint8_t empty_row[] = {'e'};
  shoal_bytes empty_row_bytes = {empty_row, sizeof(empty_row)};
  assert(shoal_mutation_create(empty_row_bytes, &empty_mutation, &error) ==
         SHOAL_STATUS_OK);
  expect_error(shoal_batch_writer_add(writer, empty_mutation, 0,
                                      &write_failure, &error),
               SHOAL_STATUS_OPERATION_FAILED, &error,
               "at least one update");
  assert(write_failure == NULL);
  shoal_mutation_free(&empty_mutation);
  assert(shoal_batch_writer_add(writer, mutation, 0, &write_failure, &error) ==
         SHOAL_STATUS_OK);
  assert(write_failure == NULL && error == NULL);
  assert(shoal_batch_writer_flush(writer, 0, &write_failure, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_batch_writer_close(writer, 0, &write_failure, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_batch_writer_close(writer, 0, &write_failure, &error) ==
         SHOAL_STATUS_OK);
  shoal_batch_writer_free(&writer);
  assert(writer == NULL);

  assert(shoal_test_batch_writer_create(
      SHOAL_TEST_WRITER_STRUCTURED_FAILURE, &writer));
  expect_error(shoal_batch_writer_flush(writer, 0, &write_failure, &error),
               SHOAL_STATUS_AMBIGUOUS_WRITE, &error, "batch writer failed");
  assert(write_failure != NULL);
  assert(shoal_write_failure_get_flags(write_failure) ==
         (SHOAL_WRITE_FAILURE_AMBIGUOUS_COMMIT |
          SHOAL_WRITE_FAILURE_RETRY_EXHAUSTED |
          SHOAL_WRITE_FAILURE_AUTOMATIC_FLUSH));
  assert(shoal_write_failure_failed_extent_count(write_failure) == 1);
  assert(shoal_write_failure_constraint_count(write_failure) == 1);
  assert(shoal_write_failure_authorization_count(write_failure) == 1);
  assert(shoal_write_failure_cleanup_count(write_failure) == 1);
  shoal_failed_extent_view structured_extent;
  assert(shoal_write_failure_get_failed_extent(
             write_failure, 0, &structured_extent, &error) == SHOAL_STATUS_OK);
  assert(strcmp(structured_extent.server, "server:9997") == 0);
  assert(structured_extent.submitted == 3);
  assert(structured_extent.committed == 2);
  shoal_write_failure_free(&write_failure);
  shoal_batch_writer_free(&writer);

  {
    static const struct {
      size_t fail_after;
      const char *message_part;
    } write_failure_alloc_cases[] = {
        {0, "failed extent 0 server"},
        {1, "failed extent 0 table id"},
        {2, "constraint 0 server"},
        {3, "constraint 0 class"},
        {4, "constraint 0 description"},
        {5, "authorization 0 server"},
        {6, "authorization 0 table id"},
        {7, "authorization 0 code"},
        {8, "cleanup 0 server"},
        {9, "cleanup 0 message"},
    };
    for (size_t i = 0; i < sizeof(write_failure_alloc_cases) /
                               sizeof(write_failure_alloc_cases[0]);
         ++i) {
      shoal_test_string_alloc_fail_after(write_failure_alloc_cases[i].fail_after);
      assert(shoal_test_batch_writer_create(
          SHOAL_TEST_WRITER_STRUCTURED_FAILURE, &writer));
      expect_error(shoal_batch_writer_flush(writer, 0, &write_failure, &error),
                   SHOAL_STATUS_OUT_OF_MEMORY, &error,
                   write_failure_alloc_cases[i].message_part);
      assert(write_failure == NULL);
      shoal_batch_writer_free(&writer);
      assert(writer == NULL);
      shoal_test_string_alloc_reset();
    }
  }

  assert(shoal_test_batch_writer_create(SHOAL_TEST_WRITER_STICKY_DEADLINE,
                                        &writer));
  expect_error(shoal_batch_writer_close(writer, 1, &write_failure, &error),
               SHOAL_STATUS_DEADLINE_EXCEEDED, &error, "deadline exceeded");
  expect_error(shoal_batch_writer_close(writer, 1000, &write_failure, &error),
               SHOAL_STATUS_DEADLINE_EXCEEDED, &error, "deadline exceeded");
  shoal_batch_writer_free(&writer);

  assert(shoal_test_batch_writer_create(SHOAL_TEST_WRITER_CONNECTOR_CLOSED,
                                        &writer));
  expect_error(shoal_batch_writer_add(writer, mutation, 0, &write_failure,
                                      &error),
               SHOAL_STATUS_CLOSED, &error, "connector is closed");
  assert(write_failure == NULL);
  shoal_batch_writer_free(&writer);

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

  assert(shoal_connector_close(admin_connector, &error) == SHOAL_STATUS_OK);
  expect_error(shoal_connector_list_tables(admin_connector, 0, &table_list,
                                           &error),
               SHOAL_STATUS_CLOSED, &error, "connector is closed");
  expect_error(shoal_connector_list_namespaces(
                   admin_connector, 0, &namespace_list, &error),
               SHOAL_STATUS_CLOSED, &error, "connector is closed");
  expect_error(shoal_connector_get_user_authorizations(
                   admin_connector, "root", 0, &bytes_list, &error),
               SHOAL_STATUS_CLOSED, &error, "connector is closed");
  expect_error(shoal_connector_list_table_splits(
                   admin_connector, "events", 0, &bytes_list, &error),
               SHOAL_STATUS_CLOSED, &error, "connector is closed");

  shoal_connector_free(&connector);
  assert(connector == NULL);
  shoal_connector_free(&connector);
  shoal_connector_free(&admin_connector);
  assert(admin_connector == NULL);
  shoal_connector_free(&admin_connector);

  shoal_connector_config_init(&config);
  config.struct_size = SHOAL_CONNECTOR_CONFIG_V1_SIZE - 1;
  expect_error(shoal_connector_create(&config, &connector, &error),
               SHOAL_STATUS_INVALID_ARGUMENT, &error, "struct_size");

  return 0;
}
