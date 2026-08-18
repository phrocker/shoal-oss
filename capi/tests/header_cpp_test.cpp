#include "shoal.h"

#include <cstdint>
#include <type_traits>

static_assert(std::is_same<shoal_status, std::int32_t>::value,
              "shoal_status must remain 32-bit");
static_assert(SHOAL_ABI_VERSION == 1u, "unexpected ABI version");

void header_compiles_as_cpp() {
  shoal_connector *connector = nullptr;
  shoal_error *error = nullptr;
  shoal_table_list_result *tables = nullptr;
  shoal_table_properties_result *properties = nullptr;
  shoal_table_view table{};
  shoal_table_property_view property{};
  (void)table;
  (void)property;
  shoal_connector_free(&connector);
  shoal_table_list_free(&tables);
  shoal_table_properties_free(&properties);
  shoal_error_free(&error);
}
