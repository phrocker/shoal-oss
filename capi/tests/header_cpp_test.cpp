#include "shoal.h"

#include <cstdint>
#include <type_traits>

static_assert(std::is_same<shoal_status, std::int32_t>::value,
              "shoal_status must remain 32-bit");
static_assert(SHOAL_ABI_VERSION == 1u, "unexpected ABI version");

void header_compiles_as_cpp() {
  shoal_connector *connector = nullptr;
  shoal_error *error = nullptr;
  shoal_connector_free(&connector);
  shoal_error_free(&error);
}
