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
static_assert(SHOAL_ABI_VERSION_MINOR == 1u, "unexpected ABI minor");
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
static_assert(SHOAL_ABI_CAPABILITY_COUNT == 13u,
              "unexpected capability count");
static_assert(SHOAL_ABI_CAPABILITY_WORD0 == 0x0000000000001fffull,
              "unexpected capability word 0");

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
  assert(shoal_abi_has_capability(SHOAL_ABI_CAPABILITY_COUNT) == 0);
  (void)table;
  (void)property;
  shoal_connector_free(&connector);
  shoal_table_list_free(&tables);
  shoal_table_properties_free(&properties);
  shoal_namespace_list_free(&namespaces);
  shoal_namespace_properties_free(&namespace_properties);
  shoal_versioned_properties_free(&versioned_properties);
  shoal_bytes_list_free(&bytes);
  shoal_error_free(&error);
  return 0;
}
