#include <iostream>
#include <libdatahike.h>
#include <assert.h>

int main(int argc, char* argv[]) {
  graal_isolate_t *isolate = NULL;
  graal_isolatethread_t *thread = NULL;

  if (graal_create_isolate(NULL, &isolate, &thread) != 0) {
    fprintf(stderr, "Initialization error.\n");
    return 1;
  }

  const char *config_str = &argv[1][0];
  create_database((long)thread, config_str);
  assert(database_exists((long)thread, config_str) && "Database should exist.");
  char *json_str = &argv[2][0];
  char *tx_result = transact((long)thread, config_str, "json", json_str, "json");
  std::cout << "tx result: " << tx_result << std::endl;
  char *query_str = &argv[3][0];

  long num_inputs = 1;
  // char** input_formats = new char[num_inputs];
  const char *input_format = "db";
  const char *output_format = "edn";
  char *query_result = query((long)thread, query_str, num_inputs, &input_format, &config_str, output_format);
  std::cout << "query result: " << query_result << std::endl;
  std::string expected_q_result = "1";
  assert(expected_q_result.compare(query_result) == 0);
  libdatahike_free((long)thread, &query_result);
  return 0;
}
