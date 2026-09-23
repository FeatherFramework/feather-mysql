'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Driver, ConnectionLease, normalize } = require('../bridge/driver');
const { parseConnectionString, connectionOptions, ConfigError } = require('../bridge/config');
const { publicError, isStatementError } = require('../bridge/errors');

function fixture(execute = async () => [[{ answer: 42 }], [{ name: 'answer' }]]) {
    const calls = { acquired: 0, released: 0, destroyed: 0, reset: 0, ended: 0, executions: [] };
    const connection = {
        execute: async (...args) => { calls.executions.push(args); return execute(...args); },
        threadId: 77,
        // The lease cleans a session with changeUser({}); calls.reset counts those.
        changeUser: async options => { calls.sessionOptions = options; calls.reset++; },
        release: () => calls.released++, destroy: () => calls.destroyed++,
    };
    const pool = {
        getConnection: async () => { calls.acquired++; return connection; },
        end: async () => { calls.ended++; },
    };
    return { driver: new Driver(pool, 1000), pool, calls, connection };
}
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

test('URI configuration decodes credentials and locks unsafe options', () => {
    const config = parseConnectionString('mysql://test:p%40ss%3Bword@localhost:3307/game?connectionLimit=4&ssl=true');
    assert.equal(config.password, 'p@ss;word');
    assert.equal(config.port, 3307);
    assert.equal(config.connectionLimit, 4);
    assert.equal(config.multipleStatements, false);
    assert.equal(config.waitForConnections, true, 'Requests queue instead of failing when the pool is busy');
    assert.equal(config.queueLimit, 512, 'The queue is bounded by default');
    assert.equal(config.ssl.rejectUnauthorized, true);
    assert.equal(config.supportBigNumbers, true);
    assert.equal(config.bigNumberStrings, false, 'Integers that fit are numbers; only oversized BIGINT values are strings');
    assert.equal(config.decimalNumbers, false, 'DECIMAL stays a string so money is never rounded');
    assert.equal(config.rowsAsArray, true);
});
test('queue length is configurable, bounded and never unbounded', () => {
    assert.equal(parseConnectionString('mysql://u@h/db?queueLimit=64').queueLimit, 64);
    for (const bad of ['0', '-1', 'x', '100001']) assert.throws(() => parseConnectionString(`mysql://u@h/db?queueLimit=${bad}`), ConfigError);
});
test('a one-off connection gets the same options minus pool-only keys', () => {
    const options = connectionOptions(parseConnectionString('mysql://u:p@h/db?connectionLimit=3'));
    for (const key of ['connectionLimit', 'queueLimit', 'waitForConnections', 'rowsAsArray']) assert.equal(key in options, false, key);
    assert.equal(options.user, 'u');
    assert.equal(options.multipleStatements, false);
    assert.deepEqual(options.flags, ['-LOCAL_FILES']);
});
test('configuration failures are ConfigError with messages that never contain credentials', () => {
    const secret = 'top-secret-pw';
    for (const text of [`mysql://u:${secret}@h:99999/db`, `mysql://u:${secret}@[oops/db`, `mysql://u:%E0%A4%A@h/db`,
        `host=h;user=u;password=${secret};database=d;nonsense=1`, `host=h;user=u;password=${secret};database=d;charset=bad;name`]) {
        assert.throws(() => parseConnectionString(text), error => {
            assert.ok(error instanceof ConfigError, text);
            assert.doesNotMatch(error.message, new RegExp(secret));
            return true;
        });
    }
});
test('semicolon configuration preserves equals and credential spaces', () => {
    const config = parseConnectionString('server=127.0.0.1;uid=test;pwd= a=b ;database=game;');
    assert.equal(config.password, ' a=b ');
    assert.equal(config.user, 'test');
});
test('invalid or unsupported connection options fail closed', () => {
    for (const text of ['', 'mysql://test@localhost', 'mysql://test@localhost/game?multipleStatements=true',
        'user=test;database=game;port=0', 'user=test;database=game;ssl=unsafe', 'user=test;database=game;connectionLimit=NaN']) {
        assert.throws(() => parseConnectionString(text));
    }
});
test('values remain separate from SQL and lease is released', async () => {
    const { driver, calls } = fixture();
    const sql = 'SELECT ? AS answer';
    const values = ["x'; DROP TABLE records; --", false, null];
    assert.equal((await driver.run(sql, values)).rows[0].answer, 42);
    await driver.drain();
    assert.deepEqual(calls.executions, [[sql, values]]);
    assert.equal(calls.released, 1);
    assert.equal(calls.reset, 1);
    assert.equal(calls.destroyed, 0);
});
test('execution errors discard the connection and do not retry', async () => {
    const { driver, calls } = fixture(async () => { throw Object.assign(new Error('secret SQL'), { code: 'ER_PARSE_ERROR' }); });
    await assert.rejects(driver.run('bad', []), { code: 'ER_PARSE_ERROR' });
    assert.equal(calls.destroyed, 1);
    assert.equal(calls.released, 0);
    assert.equal(calls.executions.length, 1);
});
test('cleanup restores the startup database and charset even if the caller mutates its settings', async () => {
    const { pool, calls } = fixture();
    const settings = { database: 'game', charset: 'utf8mb4_unicode_ci' };
    const driver = new Driver(pool, 1000, 10000, undefined, { sessionOptions: settings });
    settings.charset = 'latin1';
    await driver.run('SELECT 1', []);
    await driver.drain();
    assert.deepEqual(calls.sessionOptions, { database: 'game', charset: 'utf8mb4_unicode_ci' });
    const bare = fixture();
    await bare.driver.run('SELECT 1', []);
    await bare.driver.drain();
    assert.deepEqual(bare.calls.sessionOptions, {}, 'Without settings the driver still cleans the session');
    await driver.close();
});
test('a statement rejected by the server keeps the connection; unknown failures do not', async () => {
    const serverError = Object.assign(new Error('secret'), { code: 'ER_DUP_ENTRY', errno: 1062, sqlState: '23000' });
    const kept = fixture(async () => { throw serverError; });
    await assert.rejects(kept.driver.run('INSERT', []), { code: 'ER_DUP_ENTRY' });
    await kept.driver.drain();
    assert.deepEqual([kept.calls.destroyed, kept.calls.reset, kept.calls.released], [0, 1, 1]);
    const fatal = fixture(async () => { throw Object.assign(new Error('lost'), { code: 'PROTOCOL_CONNECTION_LOST', errno: 1, sqlState: '08S01', fatal: true }); });
    await assert.rejects(fatal.driver.run('SELECT 1', []), { code: 'PROTOCOL_CONNECTION_LOST' });
    await fatal.driver.drain();
    assert.deepEqual([fatal.calls.destroyed, fatal.calls.reset, fatal.calls.released], [1, 0, 0]);
    assert.equal(isStatementError(serverError), true);
    assert.equal(isStatementError({ code: 'ECONNRESET' }), false);
    assert.equal(isStatementError({ errno: 1, sqlState: 'bad' }), false);
});
test('the caller is answered before session cleanup finishes', async () => {
    let finishCleanup;
    const { driver, connection, calls } = fixture();
    connection.changeUser = () => new Promise(resolve => { finishCleanup = resolve; });
    assert.equal((await driver.run('SELECT ? AS answer', [1])).rows[0].answer, 42);
    assert.equal(calls.released, 0, 'Not lent again until the session is clean');
    finishCleanup();
    await driver.drain();
    assert.equal(calls.released, 1);
});
test('a timed-out statement is cancelled on the server with its connection ID', async () => {
    const killed = [];
    const { pool, connection } = fixture(() => new Promise(() => {}));
    const driver = new Driver(pool, 10, 1000, undefined, { killQuery: id => killed.push(id) });
    await assert.rejects(driver.run('UPDATE records SET n = 1', []), { code: 'QUERY_TIMEOUT' });
    assert.deepEqual(killed, [connection.threadId]);
});
test('a failing kill helper never changes the outcome', async () => {
    const { pool } = fixture(() => new Promise(() => {}));
    const driver = new Driver(pool, 10, 1000, undefined, { killQuery: () => { throw new Error('boom'); } });
    await assert.rejects(driver.run('UPDATE records SET n = 1', []), { code: 'QUERY_TIMEOUT' });
    const asynchronous = new Driver(pool, 10, 1000, undefined, { killQuery: () => Promise.reject(new Error('boom')) });
    await assert.rejects(asynchronous.run('UPDATE records SET n = 1', []), { code: 'QUERY_TIMEOUT' });
    await delay(1);
});
test('a full or absent wait queue is reported as POOL_EXHAUSTED and never executes', async () => {
    for (const message of ['Queue limit reached.', 'No connections available.']) {
        const { driver, pool, calls } = fixture();
        pool.getConnection = async () => { throw new Error(message); };
        await assert.rejects(driver.run('SELECT 1', []), { code: 'POOL_EXHAUSTED' });
        assert.equal(calls.executions.length, 0);
        assert.equal(driver.diagnostics().totals.poolExhausted, 1);
    }
});
test('acquisition failure does not execute', async () => {
    const { driver, pool, calls } = fixture();
    pool.getConnection = async () => { throw Object.assign(new Error(), { code: 'ECONNREFUSED' }); };
    await assert.rejects(driver.run('SELECT 1', []), { code: 'ECONNREFUSED' });
    assert.equal(calls.executions.length, 0);
});
test('query timeout destroys lease exactly once despite late result', async () => {
    let complete;
    const { driver, calls } = fixture(() => new Promise(resolve => { complete = resolve; }));
    driver.timeoutMs = 10;
    await assert.rejects(driver.run('UPDATE records SET n = ?', [2]), { code: 'QUERY_TIMEOUT' });
    complete([{ affectedRows: 1, insertId: 0 }, undefined]);
    await delay(1);
    assert.equal(calls.destroyed, 1);
    assert.equal(calls.released, 0);
    assert.equal(calls.executions.length, 1);
});
test('late acquisition after timeout is released without execution', async () => {
    const { driver, pool, connection, calls } = fixture();
    let acquire;
    pool.getConnection = () => new Promise(resolve => { acquire = resolve; });
    driver.timeoutMs = 10;
    await assert.rejects(driver.run('INSERT INTO records VALUES (?)', [1]), { code: 'QUERY_TIMEOUT' });
    acquire(connection);
    await delay(1);
    assert.equal(calls.released, 1);
    assert.equal(calls.executions.length, 0);
});
test('shutdown rejects in-flight and new requests, closes pool once', async () => {
    const { driver, calls } = fixture(() => new Promise(() => {}));
    const pending = driver.run('SELECT 1', []);
    const rejected = assert.rejects(pending, { code: 'RESOURCE_STOPPED' });
    await delay(1);
    await driver.close();
    await rejected;
    await driver.close();
    await assert.rejects(driver.run('SELECT 2', []), { code: 'RESOURCE_STOPPED' });
    assert.equal(calls.destroyed, 1);
    assert.equal(calls.ended, 1);
});
test('a retained lease executes on the same connection, closing is idempotent', async () => {
    const { connection, calls } = fixture();
    const lease = new ConnectionLease(connection);
    await lease.execute('SELECT 1', []);
    await lease.execute('SELECT 2', []);
    lease.release(); lease.release(); lease.destroy();
    assert.equal(calls.executions.length, 2);
    assert.equal(calls.released, 1);
    assert.throws(() => lease.execute('SELECT 3', []));
});
test('result normalization preserves order metadata, NULL, big integers and binary bytes', () => {
    const result = normalize([{ z: null, a: '9007199254740993', binary: Buffer.from([0, 255]) }],
        [{ name: 'z' }, { name: 'a' }, { name: 'binary' }]);
    assert.equal(result.firstColumn, 'z');
    assert.equal(result.first, null);
    assert.equal(result.rows[0].z, null);
    assert.equal(result.rows[0].a, '9007199254740993');
    assert.deepEqual(result.rows[0].binary, [0, 255]);
    assert.deepEqual(normalize([], []).rows, []);
    assert.equal(normalize([], []).first, null);
    assert.equal(normalize({ insertId: '9007199254740993', affectedRows: 0 }, undefined).header.insertId, '9007199254740993');
    assert.throws(() => normalize([[{ x: 1 }]], [[{ name: 'x' }]]), { code: 'RESULT_TYPE' });
});
test('positional rows keep duplicate column names apart so the first column is always the answer', () => {
    // SELECT a.id, b.id: an object row would keep only the second value.
    const result = normalize([[1, 2, 'x']], [{ name: 'id' }, { name: 'id' }, { name: 'label' }]);
    assert.equal(result.first, 1);
    assert.equal(result.firstColumn, 'id');
    assert.equal(result.rows[0].label, 'x');
    assert.equal(result.rows[0].id, 2, 'Object rows keep the last value, but value() no longer depends on them');
    const buffer = normalize([[Buffer.from([7, 8])]], [{ name: 'raw' }]);
    assert.deepEqual(buffer.first, [7, 8]);
    assert.deepEqual(buffer.rows[0].raw, [7, 8]);
    assert.equal(normalize([[0]], [{ name: 'zero' }]).first, 0, 'Zero is a value, not an absent row');
});
test('safe errors never expose SQL, credentials or driver messages', () => {
    const error = publicError({ code: 'ER_DUP_ENTRY', message: 'secret value', sql: 'secret SQL' });
    assert.equal(error.driverCode, 'ER_DUP_ENTRY');
    assert.doesNotMatch(JSON.stringify(error), /secret/);
    assert.match(publicError({ code: 'QUERY_TIMEOUT' }).message, /unknown/);
});
test('errors tell the caller what may be assumed about the database', () => {
    const rejected = { code: 'ER_DUP_ENTRY', errno: 1062, sqlState: '23000', message: 'secret', sqlMessage: "Duplicate entry 'secret'" };
    assert.deepEqual(publicError(rejected), { code: 'DATABASE_ERROR', message: 'Database rejected the statement',
        outcome: 'failed', sqlState: '23000', driverCode: 'ER_DUP_ENTRY' });
    const lost = publicError({ code: 'PROTOCOL_CONNECTION_LOST', fatal: true });
    assert.equal(lost.outcome, 'unknown');
    assert.match(lost.message, /may be unknown/);
    assert.equal(lost.sqlState, undefined);
    const expected = { POOL_EXHAUSTED: 'not_executed', QUERY_TIMEOUT: 'unknown', RESOURCE_STOPPED: 'unknown',
        TRANSACTION_TIMEOUT: 'rolled_back', TRANSACTION_CLOSED: 'not_executed', TRANSACTION_LIMIT: 'not_executed',
        RESULT_TYPE: 'executed', CONFIG_ERROR: 'not_executed', INVALID_CALLER: 'not_executed' };
    for (const [code, outcome] of Object.entries(expected)) assert.equal(publicError({ code }).outcome, outcome, code);
    assert.equal(publicError({ code: 'TRANSACTION_TIMEOUT', outcome: 'unknown' }).outcome, 'unknown', 'Timed out mid-COMMIT');
});
test('driver messages appear only when explicitly enabled, and are truncated', () => {
    const error = { code: 'ER_DUP_ENTRY', errno: 1062, sqlState: '23000', sqlMessage: 'x'.repeat(2000) };
    assert.equal(publicError(error).detail, undefined);
    assert.equal(publicError(error, { detail: true }).detail.length, 512);
    assert.equal(publicError({ code: 'ER_X', errno: 1, sqlState: '23000', sqlMessage: 42 }, { detail: true }).detail, undefined);
});

// A connection whose prepared path refuses the statement, like SHOW COLUMNS ... LIKE ? does.
function refusing({ refusal = { code: 'ER_PARSE_ERROR', errno: 1064, sqlState: '42000' }, mode = 'STRICT_TRANS_TABLES',
    charset, textRejects = false } = {}) {
    const calls = { prepared: [], text: [], destroyed: 0, released: 0 };
    const connection = {
        threadId: 9,
        execute: async (sql, parameters) => { calls.prepared.push([sql, parameters]); throw Object.assign(new Error('refused'), refusal); },
        query: async (sql, parameters) => {
            calls.text.push([sql, parameters]);
            if (sql === 'SELECT @@SESSION.sql_mode') return [[[mode]], [{ name: '@@SESSION.sql_mode' }]];
            if (textRejects) throw Object.assign(new Error('still bad'), { code: 'ER_PARSE_ERROR', errno: 1064, sqlState: '42000' });
            return [[['characters_id', 'int(11)']], [{ name: 'Field' }, { name: 'Type' }]];
        },
        changeUser: async () => {}, release: () => calls.released++, destroy: () => calls.destroyed++,
    };
    const pool = { getConnection: async () => connection, end: async () => {} };
    const driver = new Driver(pool, 1000, 10000, undefined, { sessionOptions: charset ? { charset } : {} });
    return { driver, calls };
}
const statementsRun = calls => calls.text.filter(([sql]) => sql !== 'SELECT @@SESSION.sql_mode');

test('a statement the server cannot prepare is retried once as text, with the same values', async () => {
    const { driver, calls } = refusing();
    const sql = 'SHOW COLUMNS FROM `inventory` LIKE ?;';
    const result = await driver.run(sql, ["it's a \\ test"]);
    assert.equal(result.first, 'characters_id');
    assert.deepEqual(calls.prepared, [[sql, ["it's a \\ test"]]], 'The prepared path was tried first');
    assert.deepEqual(statementsRun(calls), [[sql, ["it's a \\ test"]]], 'Then the same statement and values, once');
    assert.equal(driver.diagnostics().totals.textFallbacks, 1);
    await driver.drain();
    assert.deepEqual([calls.destroyed, calls.released], [0, 1], 'The connection is healthy and reused');
});

test('ER_UNSUPPORTED_PS is retried the same way', async () => {
    const { driver, calls } = refusing({ refusal: { code: 'ER_UNSUPPORTED_PS', errno: 1295, sqlState: 'HY000' } });
    assert.equal((await driver.run('LOCK TABLES t READ', [])).first, 'characters_id');
    assert.equal(statementsRun(calls).length, 1);
});

test('the text retry keeps every rule of the prepared path, otherwise the first error stands', async () => {
    const cases = {
        'an error that is not a prepare refusal': [{ refusal: { code: 'ER_DUP_ENTRY', errno: 1062, sqlState: '23000' } }, 'SELECT ?', [1]],
        'fewer values than placeholders': [{}, 'SHOW COLUMNS FROM t LIKE ? AND ?', [1]],
        'more values than placeholders': [{}, 'SHOW COLUMNS FROM t LIKE ?', [1, 2]],
        'identifier placeholders (??)': [{}, 'SHOW COLUMNS FROM ?? LIKE ?', ['t', 'x']],
        'a multibyte connection character set': [{ charset: 'gbk' }, 'SHOW COLUMNS FROM t LIKE ?', ['x']],
        'a server that does not honour backslash escapes': [{ mode: 'STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES' }, 'SHOW COLUMNS FROM t LIKE ?', ['x']],
    };
    for (const [name, [options, sql, parameters]] of Object.entries(cases)) {
        const { driver, calls } = refusing(options);
        await assert.rejects(driver.run(sql, parameters), error => error.errno === (options.refusal?.errno ?? 1064), name);
        assert.equal(statementsRun(calls).length, 0, `${name}: the statement was never sent as text`);
        assert.equal(driver.diagnostics().totals.textFallbacks, 0, name);
    }
});

test('genuinely bad SQL still fails after the retry, and does not cost the connection', async () => {
    const { driver, calls } = refusing({ textRejects: true });
    await assert.rejects(driver.run('SELEC ? FROM', [1]), { code: 'ER_PARSE_ERROR' });
    await driver.drain();
    assert.deepEqual([calls.destroyed, calls.released], [0, 1]);
    assert.equal(publicError({ code: 'ER_PARSE_ERROR', errno: 1064, sqlState: '42000' }).outcome, 'failed');
});

test('a retry that runs inside a transaction uses the transaction connection', async () => {
    const { driver, calls } = refusing();
    const tx = await driver.begin('owner');
    const result = await driver.transactionQuery('owner', tx.id, 'SHOW COLUMNS FROM t LIKE ?', ['x']);
    assert.equal(result.first, 'characters_id');
    const shown = statementsRun(calls).filter(([sql]) => sql.startsWith('SHOW'));
    assert.deepEqual(shown, [['SHOW COLUMNS FROM t LIKE ?', ['x']]], 'Between START TRANSACTION and COMMIT, on the same connection');
    assert.deepEqual(statementsRun(calls).map(([sql]) => sql), ['START TRANSACTION', 'SHOW COLUMNS FROM t LIKE ?']);
    await driver.finish('owner', tx.id, true);
    await driver.drain();
});
