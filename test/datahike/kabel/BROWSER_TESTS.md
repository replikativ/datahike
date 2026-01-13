# Browser Integration Tests for Datahike Kabel

Browser-based integration tests for Datahike with KabelWriter, testing the full distributed transaction flow between a JVM server and browser client.

## Architecture

- **JVM Server** (`browser_test_server.clj`): Kabel WebSocket server on port 47296
- **Browser Client** (`browser_integration_test.cljs`): Browser tests using TieredStore (memory + IndexedDB) with KabelWriter
- **Store Sync**: konserve-sync replicates data bidirectionally

## Running Tests

### Prerequisites

The tests require a JVM test server to be running:

```bash
# Terminal 1: Start JVM test server
clj -M:test -e "(require 'datahike.kabel.browser-test-server) (datahike.kabel.browser-test-server/start-test-server!)"
```

### Option 1: Headless CI/CD (Recommended)

Automated headless testing with Karma and ChromeHeadless:

```bash
# Run once to install dependencies
npm install

# Run tests
bb test cljs-browser

# Or manually:
npx shadow-cljs compile :browser-ci
npx karma start --single-run
```

**CI Integration:**
- Uses Karma with ChromeHeadless browser
- Single-run mode with proper exit codes
- Suitable for CI/CD pipelines (CircleCI, GitHub Actions, etc.)
- Tests run without any manual intervention
- Auto-detects Chrome/Chromium binary (bb task only)
- Tests complete in ~1 second with clean exit

### Option 2: Manual (Interactive Development)

Interactive browser testing with live reload:

```bash
# Terminal 1: Start JVM test server
clj -M:test -e "(require 'datahike.kabel.browser-test-server) (datahike.kabel.browser-test-server/start-test-server!)"

# Terminal 2: Compile and watch browser tests
npx shadow-cljs watch browser-integration-test

# Browser: Open http://localhost:8022/
# Tests will run automatically in the browser
```

### Option 3: REPL-Driven

```clojure
;; Start REPL with test dependencies
clj -M:test

;; Start server
(require 'datahike.kabel.browser-test-server)
(datahike.kabel.browser-test-server/start-test-server!)

;; In another terminal, start shadow-cljs watch
;; npx shadow-cljs watch browser-integration-test

;; Stop server when done
(datahike.kabel.browser-test-server/stop-test-server!)
```

## CI/CD Integration

### CircleCI

**Already integrated!** The `browser-test` job runs automatically in the test pipeline.

See `.circleci/config.yml` - the job:
1. Starts test server in background (20 second startup wait)
2. Compiles ClojureScript with shadow-cljs
3. Runs Karma tests with ChromeHeadless
4. Auto-cleanup when job completes

Job runs in parallel with other tests and is required for deploy/release.

### GitHub Actions

Add to `.github/workflows/test.yml`:

```yaml
- name: Browser Integration Tests
  run: |
    # Start test server in background
    clj -M:test -e "(require 'datahike.kabel.browser-test-server) (datahike.kabel.browser-test-server/start-test-server!) (Thread/sleep Long/MAX_VALUE)" &
    SERVER_PID=$!
    
    # Wait for server to be ready  
    sleep 5
    
    # Run tests
    bb test cljs-browser
    
    # Cleanup
    kill $SERVER_PID
```

### Option 4: Automated (Legacy)

```bash
# Start server in background
clj -M:test -e "(require 'datahike.kabel.browser-test-server) (datahike.kabel.browser-test-server/start-test-server!) (Thread/sleep Long/MAX_VALUE)" &
SERVER_PID=$!

# Compile and run tests
npx shadow-cljs compile browser-integration-test

# Run with Karma or headless browser
# (Future: add Karma configuration)

# Stop server
kill $SERVER_PID
```

## Test Coverage

The browser tests verify:

1. **Dynamic Database Creation**: Create database remotely via KabelWriter
2. **Connection**: Connect with TieredStore (memory + IndexedDB) + KabelWriter
3. **Schema Transactions**: Add schema attributes
4. **Data Transactions**: Add entities via remote transactions
5. **Queries**: d/q, d/pull, d/entity APIs
6. **Persistence**: Disconnect, reconnect, verify data persists in IndexedDB
7. **Multiple Transactions**: Sequential transactions with query updates
8. **Cleanup**: Delete database (IndexedDB automatically cleaned up by `d/delete-database`)

## Troubleshooting

**Server not running:**
```
Error: Connection failed
→ Ensure JVM server is running on ws://localhost:47296
```

**Port already in use:**
```
Error: Address already in use
→ Stop other processes on port 47296 or change TEST_SERVER_PORT
```

**IndexedDB errors:**
```
Error: IndexedDB not available
→ Use a modern browser (Chrome, Firefox, Edge)
→ Check browser privacy settings (IndexedDB may be disabled)
```

**Build errors:**
```
Error: Cannot find module ...
→ Run: npm install
→ Ensure konserve, kabel, konserve-sync are linked or available
```

## Implementation Notes

**IndexedDB Cleanup**: `d/delete-database` handles complete cleanup including the IndexedDB backend. Do NOT call `idb/delete-idb` separately - this causes hangs in headless Chrome.

## Future Enhancements

- [ ] Multiple concurrent browser clients (Phase 2)
- [ ] Reconnection/resilience testing
- [ ] Page reload persistence
- [ ] Performance benchmarks
