#include "bridge.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

int main(void) {
  static const uint8_t row[] = {'r', '\0', 'w'};
  static const uint8_t family[] = {'c', 'f'};
  static const uint8_t qualifier[] = {'c', 'q'};
  static const uint8_t visibility[] = {'A', '&', 'B'};
  static const uint8_t value[] = {'v', '\0', 'l'};

  shoal_scan_result *result = shoal_bridge_scan_result_alloc(2);
  assert(result != NULL);
  assert(shoal_bridge_scan_result_count(result) == 2);
  assert(shoal_bridge_scan_result_set(
      result, 0, row, sizeof(row), family, sizeof(family), qualifier,
      sizeof(qualifier), visibility, sizeof(visibility), 42, value,
      sizeof(value)));
  assert(shoal_bridge_scan_result_set(result, 1, NULL, 0, NULL, 0, NULL, 0,
                                      NULL, 0, -1, NULL, 0));

  shoal_key_value_view view;
  assert(shoal_bridge_scan_result_get(result, 0, &view));
  assert(view.row.length == sizeof(row));
  assert(memcmp(view.row.data, row, sizeof(row)) == 0);
  assert(view.column_family.length == sizeof(family));
  assert(memcmp(view.column_family.data, family, sizeof(family)) == 0);
  assert(view.column_qualifier.length == sizeof(qualifier));
  assert(memcmp(view.column_qualifier.data, qualifier, sizeof(qualifier)) == 0);
  assert(view.column_visibility.length == sizeof(visibility));
  assert(memcmp(view.column_visibility.data, visibility, sizeof(visibility)) ==
         0);
  assert(view.timestamp == 42);
  assert(view.value.length == sizeof(value));
  assert(memcmp(view.value.data, value, sizeof(value)) == 0);

  memset(&view, 0xff, sizeof(view));
  assert(shoal_bridge_scan_result_get(result, 1, &view));
  assert(view.row.data == NULL && view.row.length == 0);
  assert(view.value.data == NULL && view.value.length == 0);
  assert(view.timestamp == -1);
  assert(!shoal_bridge_scan_result_get(result, 2, &view));
  assert(!shoal_bridge_scan_result_set(result, 2, NULL, 0, NULL, 0, NULL, 0,
                                       NULL, 0, 0, NULL, 0));
  assert(!shoal_bridge_scan_result_set(result, 1, NULL, 1, NULL, 0, NULL, 0,
                                       NULL, 0, 0, NULL, 0));

  shoal_bridge_scan_result_free(result);
  shoal_bridge_scan_result_free(NULL);
  assert(shoal_bridge_scan_result_alloc(SIZE_MAX) == NULL);
  return 0;
}
