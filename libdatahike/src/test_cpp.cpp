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

  char *config_str = &argv[1][0];
  create_database((long)thread, config_str);
  assert(database_exists((long)thread, config_str) && "Database should exist.");
  char *json_str = &argv[2][0];
  char *tx_result = transact_json((long)thread, config_str, json_str);
  // std::cout << "tx result: " << tx_result << std::endl;
  char *query_str = &argv[3][0];
  char *q_result = q_json((long)thread, query_str, config_str);
  //std::cout << "q result: " << q_result << std::endl;
  std::string expected_q_result = "1";
  assert(expected_q_result.compare(q_result) == 0);
  return 0;
}
