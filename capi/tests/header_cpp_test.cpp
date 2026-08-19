#include "shoal.h"

#include <cassert>
#include <cstdint>
#include <type_traits>

static_assert(std::is_same<shoal_status, std::int32_t>::value,
              "shoal_status must remain 32-bit");
static_assert(std::is_same<shoal_abi_capability_bits, std::uint64_t>::value,
              "capability bitset words must remain 64-bit");
static_assert(SHOAL_ABI_VERSION == 1u, "unexpected ABI version");
static_assert(SHOAL_ABI_VERSION_MAJOR == 1u, "unexpected ABI major");
static_assert(SHOAL_ABI_VERSION_MINOR == 6u, "unexpected ABI minor");
static_assert(SHOAL_ABI_VERSION_PATCH == 0u, "unexpected ABI patch");
static_assert(SHOAL_ABI_VERSION_PACKED ==
                  SHOAL_ABI_PACK_VERSION(SHOAL_ABI_VERSION_MAJOR,
                                         SHOAL_ABI_VERSION_MINOR,
                                         SHOAL_ABI_VERSION_PATCH),
              "packed ABI version drifted");
static_assert(SHOAL_ABI_CAPABILITY_CONNECTOR == 0u,
              "unexpected connector capability id");
static_assert(SHOAL_ABI_CAPABILITY_TABLE_ADMIN == 9u,
              "unexpected table admin capability id");
static_assert(SHOAL_ABI_CAPABILITY_NAMESPACE_ADMIN == 10u,
              "unexpected namespace admin capability id");
static_assert(SHOAL_ABI_CAPABILITY_SECURITY_ADMIN == 11u,
              "unexpected security admin capability id");
static_assert(SHOAL_ABI_CAPABILITY_TABLE_SPLITS == 12u,
              "unexpected table splits capability id");
static_assert(SHOAL_ABI_CAPABILITY_CONNECTOR_IDENTITY == 13u,
              "unexpected connector identity capability id");
static_assert(SHOAL_ABI_CAPABILITY_DATA_DESCRIPTORS == 14u,
              "unexpected data descriptors capability id");
static_assert(SHOAL_ABI_CAPABILITY_CONFIGURATION_TOPOLOGY == 15u,
              "unexpected configuration topology capability id");
static_assert(SHOAL_ABI_CAPABILITY_RFILE == 16u,
              "unexpected RFile capability id");
static_assert(SHOAL_ABI_CAPABILITY_DATA_VALUES == 17u,
              "unexpected data values capability id");
static_assert(SHOAL_ABI_CAPABILITY_COUNT == 18u,
              "unexpected capability count");
static_assert(SHOAL_ABI_CAPABILITY_WORD0 == 0x000000000003ffffull,
              "unexpected capability word 0");
static_assert(std::is_standard_layout<shoal_connector_identity_view>::value,
              "identity view must remain standard-layout");
static_assert(std::is_standard_layout<shoal_range_view>::value,
              "range view must remain standard-layout");
static_assert(std::is_standard_layout<shoal_iterator_setting_view>::value,
              "iterator setting view must remain standard-layout");
static_assert(std::is_standard_layout<shoal_server_view>::value,
              "server view must remain standard-layout");
static_assert(std::is_standard_layout<shoal_rfile_writer_config>::value,
              "RFile writer config must remain standard-layout");
static_assert(std::is_standard_layout<shoal_rfile_merge_config>::value,
              "RFile merge config must remain standard-layout");
static_assert(std::is_standard_layout<shoal_rfile_entry_view>::value,
              "RFile entry view must remain standard-layout");
static_assert(std::is_standard_layout<shoal_key_value>::value,
              "key/value input must remain standard-layout");

#define ASSERT_PERMISSION_VALUE(name, value)                                 \
  static_assert(name == value, "unexpected permission ordinal: " #name)

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

int main() {
  shoal_connector *connector = nullptr;
  shoal_error *error = nullptr;
  shoal_table_list_result *tables = nullptr;
  shoal_table_properties_result *properties = nullptr;
  shoal_table_view table{};
  shoal_table_property_view property{};
  shoal_namespace_list_result *namespaces = nullptr;
  shoal_namespace_properties_result *namespace_properties = nullptr;
  shoal_versioned_properties_result *versioned_properties = nullptr;
  shoal_bytes_list_result *bytes = nullptr;
  shoal_connector_identity_result *identity = nullptr;
  shoal_connector_identity_view identity_view{};
  shoal_range_result *range_result = nullptr;
  shoal_range_view range_view{};
  shoal_iterator_setting_result *iterator_result = nullptr;
  shoal_iterator_setting_view iterator_view{};
  shoal_scanner *scanner = nullptr;
  shoal_batch_scanner *batch_scanner = nullptr;
  shoal_scan_result *scan_result = nullptr;
  shoal_mutation *mutation = nullptr;
  shoal_batch_writer *writer = nullptr;
  shoal_write_failure *write_failure = nullptr;
  shoal_configuration *configuration = nullptr;
  shoal_bytes_result *bytes_result = nullptr;
  shoal_string_list_result *strings = nullptr;
  shoal_server_list_result *servers = nullptr;
  shoal_server_view server_view{};
  shoal_rfile_writer *rfile_writer = nullptr;
  shoal_rfile_reader *rfile_reader = nullptr;
  shoal_rfile_seekable *rfile_seekable = nullptr;
  shoal_rfile_entry_result *rfile_entry = nullptr;
  shoal_rfile_entry_view rfile_entry_view{};
  shoal_authorizations *authorizations = nullptr;
  shoal_key_value_result *key_value_result = nullptr;
  shoal_key_value key_value{};
  assert(shoal_abi_version() == SHOAL_ABI_VERSION);
  assert(shoal_abi_version_major() == SHOAL_ABI_VERSION_MAJOR);
  assert(shoal_abi_version_minor() == SHOAL_ABI_VERSION_MINOR);
  assert(shoal_abi_version_patch() == SHOAL_ABI_VERSION_PATCH);
  assert(shoal_abi_version_packed() == SHOAL_ABI_VERSION_PACKED);
  assert(shoal_abi_capability_count() == SHOAL_ABI_CAPABILITY_COUNT);
  assert(shoal_abi_capability_word_count() == SHOAL_ABI_CAPABILITY_WORD_COUNT);
  assert(shoal_abi_capability_word(0) == SHOAL_ABI_CAPABILITY_WORD0);
  assert(shoal_abi_capability_word(1) == 0);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_BATCH_WRITER) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_TABLE_ADMIN) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_NAMESPACE_ADMIN) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_SECURITY_ADMIN) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_TABLE_SPLITS) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_CONNECTOR_IDENTITY) ==
         1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_DATA_DESCRIPTORS) == 1);
  assert(shoal_abi_has_capability(
             SHOAL_ABI_CAPABILITY_CONFIGURATION_TOPOLOGY) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_RFILE) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_DATA_VALUES) == 1);
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_COUNT) == 0);
  assert(shoal_versioned_properties_version(versioned_properties) == 0);
  assert(shoal_versioned_properties_count(versioned_properties) == 0);
  assert(shoal_versioned_properties_get(versioned_properties, 0, &property,
                                        &error) ==
         SHOAL_STATUS_INVALID_ARGUMENT);
  assert(error != nullptr);
  shoal_error_free(&error);
  assert(shoal_configuration_create(&configuration, &error) == SHOAL_STATUS_OK);
  const std::uint8_t key_data[] = {'k', '\0'};
  const std::uint8_t value_data[] = {'v', '\0'};
  shoal_bytes key{key_data, sizeof(key_data)};
  shoal_bytes value{value_data, sizeof(value_data)};
  assert(shoal_configuration_set(configuration, key, value, &error) ==
         SHOAL_STATUS_OK);
  assert(shoal_configuration_get(configuration, key, &bytes_result, &error) ==
         SHOAL_STATUS_OK);
  shoal_bytes copied = shoal_bytes_result_get(bytes_result);
  assert(copied.length == sizeof(value_data));
  shoal_server_view_init(&server_view);
  assert(server_view.struct_size == SHOAL_SERVER_VIEW_V1_SIZE);
  shoal_rfile_entry_view_init(&rfile_entry_view);
  assert(rfile_entry_view.struct_size == SHOAL_RFILE_ENTRY_VIEW_V1_SIZE);
  shoal_key_value_init(&key_value);
  assert(key_value.struct_size == SHOAL_KEY_VALUE_V1_SIZE);
  assert(error == nullptr);
  shoal_connector_identity_view_init(&identity_view);
  assert(identity_view.struct_size == SHOAL_CONNECTOR_IDENTITY_VIEW_V1_SIZE);
  assert(shoal_connector_get_identity(connector, 0, &identity, &error) ==
         SHOAL_STATUS_INVALID_HANDLE);
  shoal_error_free(&error);
  shoal_range_view_init(&range_view);
  assert(shoal_range_get(range_result, &range_view, &error) ==
         SHOAL_STATUS_INVALID_ARGUMENT);
  shoal_error_free(&error);
  shoal_iterator_setting_view_init(&iterator_view);
  assert(shoal_iterator_setting_get(iterator_result, &iterator_view, &error) ==
         SHOAL_STATUS_INVALID_ARGUMENT);
  shoal_error_free(&error);
  assert(shoal_connector_identity_get(identity, &identity_view, &error) ==
         SHOAL_STATUS_INVALID_ARGUMENT);
  shoal_error_free(&error);
  (void)table;
  (void)property;
  shoal_connector_free(&connector);
  shoal_table_list_free(&tables);
  shoal_table_properties_free(&properties);
  shoal_namespace_list_free(&namespaces);
  shoal_namespace_properties_free(&namespace_properties);
  shoal_versioned_properties_free(&versioned_properties);
  shoal_bytes_list_free(&bytes);
  shoal_connector_identity_free(&identity);
  shoal_range_free(&range_result);
  shoal_iterator_setting_free(&iterator_result);
  shoal_scanner_free(&scanner);
  shoal_batch_scanner_free(&batch_scanner);
  shoal_scan_result_free(&scan_result);
  shoal_mutation_free(&mutation);
  shoal_batch_writer_free(&writer);
  shoal_write_failure_free(&write_failure);
  shoal_configuration_free(&configuration);
  shoal_bytes_result_free(&bytes_result);
  shoal_string_list_free(&strings);
  shoal_server_list_free(&servers);
  shoal_rfile_writer_free(&rfile_writer);
  shoal_rfile_reader_free(&rfile_reader);
  shoal_rfile_seekable_free(&rfile_seekable);
  shoal_rfile_entry_result_free(&rfile_entry);
  shoal_authorizations_free(&authorizations);
  shoal_key_value_result_free(&key_value_result);
  shoal_error_free(&error);
  return 0;
}
