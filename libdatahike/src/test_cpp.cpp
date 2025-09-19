#include <iostream>
#include <libdatahike.h>
#include <assert.h>

void default_reader(char* edn) {
  std::cout << "result: " << edn << std::endl;
}

void database_exists_reader(char* database_exists_result_edn) {
  std::cout << "database exists result: " << database_exists_result_edn << std::endl;
  std::string true_str = "true";
  assert(true_str.compare(database_exists_result_edn) == 0);
}

void transact_reader(char* transact_result_edn) {
  std::cout << "transact result: " << transact_result_edn << std::endl;
}

void query_reader(char* query_result_edn) {
  std::cout << "query result: " << query_result_edn << std::endl;
  std::string expected_q_result = "#{[1 \"Alice\"]}";
  assert(expected_q_result.compare(query_result_edn) == 0);
}


int main(int argc, char* argv[]) {
  graal_isolate_t *isolate = NULL;
  graal_isolatethread_t *thread = NULL;

  if (graal_create_isolate(NULL, &isolate, &thread) != 0) {
    fprintf(stderr, "Initialization error.\n");
    return 1;
  }

  const char *config_str = &argv[1][0];

  void (*default_reader_pointer)(char*);
  default_reader_pointer = &default_reader;
  create_database((long)thread, config_str, "edn", (const void*)default_reader_pointer);

  void (*database_exists_reader_pointer)(char*);
  database_exists_reader_pointer = &database_exists_reader;
  database_exists((long)thread, config_str, "edn", (const void*)database_exists_reader_pointer);

  char *tx_str = &argv[2][0];
  void (*transact_reader_pointer)(char*);
  transact_reader_pointer = &transact_reader;
  transact((long)thread, config_str, "edn", tx_str, "edn", (const void*)transact_reader);

  char *query_str = &argv[3][0];
  long num_inputs = 1;
  const char *input_format = "db";
  const char *output_format = "edn";
  void (*query_reader_pointer)(char*);
  query_reader_pointer = &query_reader;
  query((long)thread, query_str, num_inputs, &input_format, &config_str,
        output_format, (const void*)query_reader_pointer);
  return 0;
}
