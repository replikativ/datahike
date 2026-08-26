// Verify initial logging in a fresh process, before the package can be cached.
const mode = process.argv[2];

if (mode === 'default') {
  delete process.env.DATAHIKE_LOG_LEVEL;
} else if (mode === 'trace') {
  process.env.DATAHIKE_LOG_LEVEL = 'trace';
} else {
  throw new Error(`Expected test mode default or trace, got: ${mode}`);
}

const originalError = console.error.bind(console);
const capturedMessages = [];
const debugMessages = [];
for (const method of ['debug', 'info', 'warn', 'error']) {
  console[method] = (...args) => {
    capturedMessages.push([method, args]);
    if (method === 'debug') debugMessages.push(args);
  };
}

const d = require('./datahike.js.api.js');

async function exerciseDatabase() {
  const config = {
    store: { backend: ':memory', id: d.randomUuid() },
    'value-caps': ':default'
  };
  await d.createDatabase(config);
  const conn = await d.connect(config);
  d.release(conn);
  await d.deleteDatabase(config);
}

exerciseDatabase()
  .then(() => {
    if (mode === 'default' && capturedMessages.length !== 0) {
      throw new Error(`Default quickstart emitted ${capturedMessages.length} log messages`);
    }
    if (mode === 'trace' && debugMessages.length === 0) {
      throw new Error('DATAHIKE_LOG_LEVEL=trace did not enable trace/debug messages');
    }
    console.log(`  ✓ ${mode} initial log level (${debugMessages.length} trace/debug messages)`);
    process.exit(0);
  })
  .catch(error => {
    originalError(error);
    process.exit(1);
  });
