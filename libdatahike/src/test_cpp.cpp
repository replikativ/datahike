#include <iostream>
#include <libdatahike.h>
#include <assert.h>

const char* config_str = "{:store {:backend :file :path \"/tmp/libdatahike-test\" :id #uuid \"f11e0000-0000-0000-0000-000000000001\"} :schema-flexibility :write}";
const char* schema_str = "[{:db/ident :name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}]";
const char* tx_str = "[{:name \"Alice\"}]";
const char* query_str = "[:find ?e ?v :where [?e :name ?v]]";

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
  std::string expected_q_result = "#{[2 \"Alice\"]}";
  assert(expected_q_result.compare(query_result_edn) == 0);
}


int main(int argc, char* argv[]) {
  graal_isolate_t *isolate = NULL;
  graal_isolatethread_t *thread = NULL;

  if (graal_create_isolate(NULL, &isolate, &thread) != 0) {
    fprintf(stderr, "Initialization error.\n");
    return 1;
  }

  void (*default_reader_pointer)(char*);
  default_reader_pointer = &default_reader;
  create_database((long)thread, config_str, "edn", (const void*)default_reader_pointer);

  void (*database_exists_reader_pointer)(char*);
  database_exists_reader_pointer = &database_exists_reader;
  database_exists((long)thread, config_str, "edn", (const void*)database_exists_reader_pointer);

  void (*transact_reader_pointer)(char*);
  transact_reader_pointer = &transact_reader;
  transact((long)thread, config_str, "edn", schema_str, "edn", (const void*)transact_reader);
  transact((long)thread, config_str, "edn", tx_str, "edn", (const void*)transact_reader);

  long num_inputs = 1;
  const char *input_format = "db";
  const char *output_format = "edn";
  void (*query_reader_pointer)(char*);
  query_reader_pointer = &query_reader;
  q((long)thread, query_str, num_inputs, &input_format, &config_str,
        output_format, (const void*)query_reader_pointer);
  return 0;
}
