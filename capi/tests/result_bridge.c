#include "bridge.h"

#include <assert.h>
#include <stdint.h>
#include <string.h>

int main(void) {
  assert(shoal_bridge_string_alloc(NULL, 1) == NULL);
  char *string_copy = shoal_bridge_string_alloc("value", 5);
  assert(string_copy != NULL);
  assert(strcmp(string_copy, "value") == 0);
  shoal_bridge_string_free(string_copy);
  shoal_bridge_test_string_alloc_fail_after(1);
  string_copy = shoal_bridge_string_alloc("first", 5);
  assert(string_copy != NULL);
  shoal_bridge_string_free(string_copy);
  assert(shoal_bridge_string_alloc("second", 6) == NULL);
  shoal_bridge_test_string_alloc_reset();

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

  shoal_table_list_result *table_list = shoal_bridge_table_list_alloc(2);
  assert(table_list != NULL);
  assert(shoal_bridge_table_list_count(table_list) == 2);
  assert(shoal_bridge_table_list_set(table_list, 0, "analytics.orders", "2"));
  assert(shoal_bridge_table_list_set(table_list, 1, "events", "1"));
  shoal_table_view table_view;
  assert(shoal_bridge_table_list_get(table_list, 0, &table_view));
  assert(strcmp(table_view.name, "analytics.orders") == 0);
  assert(strcmp(table_view.id, "2") == 0);
  assert(shoal_bridge_table_list_get(table_list, 1, &table_view));
  assert(strcmp(table_view.name, "events") == 0);
  assert(strcmp(table_view.id, "1") == 0);
  assert(!shoal_bridge_table_list_get(table_list, 2, &table_view));
  assert(!shoal_bridge_table_list_set(table_list, 2, "missing", "3"));
  assert(!shoal_bridge_table_list_set(table_list, 0, NULL, "1"));
  shoal_bridge_table_list_free(table_list);
  shoal_bridge_table_list_free(NULL);
  assert(shoal_bridge_table_list_alloc(SIZE_MAX) == NULL);

  shoal_namespace_list_result *namespace_list =
      shoal_bridge_namespace_list_alloc(2);
  assert(namespace_list != NULL);
  assert(shoal_bridge_namespace_list_count(namespace_list) == 2);
  assert(shoal_bridge_namespace_list_set(namespace_list, 0, "", "+default"));
  assert(
      shoal_bridge_namespace_list_set(namespace_list, 1, "analytics", "12"));
  shoal_namespace_view namespace_view;
  assert(shoal_bridge_namespace_list_get(namespace_list, 0, &namespace_view));
  assert(strcmp(namespace_view.name, "") == 0);
  assert(strcmp(namespace_view.id, "+default") == 0);
  assert(shoal_bridge_namespace_list_get(namespace_list, 1, &namespace_view));
  assert(strcmp(namespace_view.name, "analytics") == 0);
  assert(strcmp(namespace_view.id, "12") == 0);
  assert(!shoal_bridge_namespace_list_get(namespace_list, 2, &namespace_view));
  assert(!shoal_bridge_namespace_list_set(namespace_list, 2, "missing", "3"));
  assert(!shoal_bridge_namespace_list_set(namespace_list, 0, NULL, "1"));
  shoal_bridge_namespace_list_free(namespace_list);
  shoal_bridge_namespace_list_free(NULL);
  assert(shoal_bridge_namespace_list_alloc(SIZE_MAX) == NULL);

  static const uint8_t prev_row[] = {'a', '\0'};
  static const uint8_t end_row[] = {'z'};
  shoal_write_failure *failure = shoal_bridge_write_failure_alloc(
      SHOAL_WRITE_FAILURE_AMBIGUOUS_COMMIT |
          SHOAL_WRITE_FAILURE_RETRY_EXHAUSTED,
      1, 1, 1, 1);
  assert(failure != NULL);
  assert(shoal_bridge_write_failure_set_failed_extent(
      failure, 0, "server:9997", "5", prev_row, sizeof(prev_row), 1, end_row,
      sizeof(end_row), 1, 3, 2));
  assert(shoal_bridge_write_failure_set_constraint(
      failure, 0, "server:9997", "Constraint", 7, "bad mutation", 4));
  assert(shoal_bridge_write_failure_set_authorization(
      failure, 0, "server:9997", "5", NULL, 0, 0, end_row, sizeof(end_row), 1,
      "PERMISSION_DENIED"));
  assert(shoal_bridge_write_failure_set_cleanup(
      failure, 0, "server:9997", "cancel failed"));

  assert(shoal_bridge_write_failure_flags(failure) ==
         (SHOAL_WRITE_FAILURE_AMBIGUOUS_COMMIT |
          SHOAL_WRITE_FAILURE_RETRY_EXHAUSTED));
  assert(shoal_bridge_write_failure_failed_extent_count(failure) == 1);
  shoal_failed_extent_view failed_extent;
  assert(shoal_bridge_write_failure_get_failed_extent(failure, 0,
                                                       &failed_extent));
  assert(strcmp(failed_extent.server, "server:9997") == 0);
  assert(strcmp(failed_extent.table_id, "5") == 0);
  assert(failed_extent.has_prev_row == 1);
  assert(failed_extent.prev_row.length == sizeof(prev_row));
  assert(memcmp(failed_extent.prev_row.data, prev_row, sizeof(prev_row)) == 0);
  assert(failed_extent.submitted == 3 && failed_extent.committed == 2);

  shoal_constraint_violation_view constraint;
  assert(shoal_bridge_write_failure_get_constraint(failure, 0, &constraint));
  assert(strcmp(constraint.constraint_class, "Constraint") == 0);
  assert(constraint.violation_code == 7);
  assert(constraint.violating_mutation_count == 4);

  shoal_authorization_failure_view authorization;
  assert(shoal_bridge_write_failure_get_authorization(failure, 0,
                                                       &authorization));
  assert(authorization.has_prev_row == 0);
  assert(authorization.prev_row.data == NULL);
  assert(strcmp(authorization.code, "PERMISSION_DENIED") == 0);

  shoal_cleanup_failure_view cleanup;
  assert(shoal_bridge_write_failure_get_cleanup(failure, 0, &cleanup));
  assert(strcmp(cleanup.message, "cancel failed") == 0);
  assert(!shoal_bridge_write_failure_get_cleanup(failure, 1, &cleanup));
  shoal_bridge_write_failure_free(failure);

  shoal_table_properties_result *properties =
      shoal_bridge_table_properties_alloc(2);
  assert(properties != NULL);
  assert(shoal_bridge_table_properties_count(properties) == 2);
  assert(shoal_bridge_table_properties_set(properties, 0, "table.empty", ""));
  assert(shoal_bridge_table_properties_set(properties, 1, "table.mode",
                                           "stream"));
  shoal_table_property_view property_view;
  assert(shoal_bridge_table_properties_get(properties, 0, &property_view));
  assert(strcmp(property_view.key, "table.empty") == 0);
  assert(strcmp(property_view.value, "") == 0);
  assert(shoal_bridge_table_properties_get(properties, 1, &property_view));
  assert(strcmp(property_view.key, "table.mode") == 0);
  assert(strcmp(property_view.value, "stream") == 0);
  assert(!shoal_bridge_table_properties_get(properties, 2, &property_view));
  assert(!shoal_bridge_table_properties_set(properties, 2, "missing", "x"));
  assert(!shoal_bridge_table_properties_set(properties, 0, NULL, "x"));
  shoal_bridge_table_properties_free(properties);
  shoal_bridge_table_properties_free(NULL);
  assert(shoal_bridge_table_properties_alloc(SIZE_MAX) == NULL);

  shoal_namespace_properties_result *namespace_properties =
      shoal_bridge_namespace_properties_alloc(2);
  assert(namespace_properties != NULL);
  assert(shoal_bridge_namespace_properties_count(namespace_properties) == 2);
  assert(shoal_bridge_namespace_properties_set(namespace_properties, 0,
                                               "table.empty", ""));
  assert(shoal_bridge_namespace_properties_set(namespace_properties, 1,
                                               "table.mode", "stream"));
  assert(shoal_bridge_namespace_properties_get(namespace_properties, 0,
                                               &property_view));
  assert(strcmp(property_view.key, "table.empty") == 0);
  assert(strcmp(property_view.value, "") == 0);
  assert(shoal_bridge_namespace_properties_get(namespace_properties, 1,
                                               &property_view));
  assert(strcmp(property_view.key, "table.mode") == 0);
  assert(strcmp(property_view.value, "stream") == 0);
  assert(!shoal_bridge_namespace_properties_get(namespace_properties, 2,
                                                &property_view));
  assert(!shoal_bridge_namespace_properties_set(namespace_properties, 2,
                                                "missing", "x"));
  assert(!shoal_bridge_namespace_properties_set(namespace_properties, 0, NULL,
                                                "x"));
  shoal_bridge_namespace_properties_free(namespace_properties);
  shoal_bridge_namespace_properties_free(NULL);
  assert(shoal_bridge_namespace_properties_alloc(SIZE_MAX) == NULL);
  return 0;
}
