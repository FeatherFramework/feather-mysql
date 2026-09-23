'use strict';
const { randomUUID } = require('node:crypto');
const { isStatementError } = require('./errors');

// Faults raised by the driver itself. `outcome` tells the bridge what the caller
// may assume about the database (see errors.js).
function fault(code, outcome) {
    return Object.assign(new Error(code), { code, outcome });
}

// ER_PARSE_ERROR and ER_UNSUPPORTED_PS. Both are raised while the server is preparing a statement,
// before anything runs, so trying again another way cannot apply a write twice.
const PREPARE_REFUSED = new Set([1064, 1295]);
// Client-side escaping backslashes quotes, which is not sound when the connection character set
// can swallow the backslash as part of a multibyte character.
const MULTIBYTE_CHARSETS = /^(gbk|gb2312|gb18030|big5|sjis|cp932|euckr|eucjpms|ujis)/i;

// How many placeholders the text protocol will substitute. Returns -1 when the statement uses `??`
// (identifier placeholders), which values must never be able to reach.
function placeholderCount(sql) {
    let count = 0;
    for (const run of sql.match(/\?+/g) ?? []) {
        if (run.length > 1) return -1;
        count++;
    }
    return count;
}

function firstValue(rows) {
    const row = rows?.[0];
    return Array.isArray(row) ? row[0] : Object.values(row ?? {})[0];
}

// One lease owns one physical connection until protocol cleanup is complete.
class ConnectionLease {
    constructor(connection, onClose = () => {}, sessionOptions = {}) {
        this.connection = connection; this.closed = false; this.onClose = onClose;
        this.sessionOptions = sessionOptions;
        this.onTextFallback = () => {};
    }
    // Values are bound by the server (prepared statement). A few statements cannot be prepared with
    // a placeholder, for example SHOW COLUMNS ... LIKE ?; those are retried once as text.
    execute(sql, parameters) {
        if (this.closed) throw new Error('Connection lease is closed');
        return this.connection.execute(sql, parameters).catch(error => this.retryAsText(sql, parameters, error));
    }
    // The retry keeps every rule the prepared path had: values only, the same placeholder count, and
    // only when client-side escaping is sound on this connection. Anything else keeps the first error.
    async retryAsText(sql, parameters, error) {
        if (!PREPARE_REFUSED.has(error?.errno) || this.closed || !Array.isArray(parameters)) throw error;
        if (MULTIBYTE_CHARSETS.test(this.sessionOptions.charset ?? '')) throw error;
        if (placeholderCount(sql) !== parameters.length) throw error;
        const [modes] = await this.connection.query('SELECT @@SESSION.sql_mode');
        if (/NO_BACKSLASH_ESCAPES/i.test(String(firstValue(modes) ?? ''))) throw error;
        this.onTextFallback();
        return this.connection.query(sql, parameters);
    }
    command(sql) {
        if (this.closed) throw new Error('Connection lease is closed');
        return this.connection.query(sql);
    }
    reset() {
        if (this.closed) throw new Error('Connection lease is closed');
        // COM_RESET_CONNECTION alone does not restore the selected database,
        // and MySQL resets charset variables to server defaults. mysql2's
        // changeUser reuses its credentials, restores the configured
        // database/charset and clears session state without replacing the
        // physical connection. The startup values are passed explicitly because
        // mysql2 session tracking can mutate its own charset configuration.
        return this.connection.changeUser(this.sessionOptions);
    }
    // Server-side connection ID, used to cancel a statement that is still running.
    threadId() {
        return this.connection.threadId ?? this.connection.connection?.threadId;
    }
    release() {
        if (this.closed) return;
        try { this.connection.release(); }
        catch (error) { this.destroy(); throw error; }
        this.closed = true;
        this.onClose();
    }
    destroy() {
        if (this.closed) return;
        try { this.connection.destroy(); }
        finally {
            // mysql2 3.24.4 destroy() half-closes its stream. Retire the transport
            // too, so cancellation does not leave a client socket waiting on SQL.
            try { this.connection.connection?.stream?.destroy(); }
            finally { this.closed = true; this.onClose(); }
        }
    }
}

function plain(value) {
    // Cfx msgpack does not carry Node Buffer prototypes to Lua.
    return Buffer.isBuffer(value) ? Array.from(value) : value;
}

// Rows arrive positionally (rowsAsArray) so duplicate column names cannot
// collapse and the first column is always known. Object rows are still accepted.
function normalize(rows, fields) {
    if (Array.isArray(rows)) {
        // CALL/multiple result sets are outside the result contract.
        if (fields?.some(Array.isArray)) throw Object.assign(new Error(), { code: 'RESULT_TYPE' });
        const names = (fields || []).map(field => field.name);
        const normalized = rows.map(row => {
            const result = Object.create(null);
            if (Array.isArray(row)) {
                for (let i = 0; i < names.length; i++) result[names[i]] = plain(row[i]);
            } else {
                for (const [key, value] of Object.entries(row)) result[key] = plain(value);
            }
            return result;
        });
        const head = rows[0];
        const first = head === undefined ? null : plain(Array.isArray(head) ? head[0] : head[names[0]]);
        return { kind: 'rows', rows: normalized, firstColumn: names[0], first };
    }
    return { kind: 'write', header: {
        insertId: rows.insertId, affectedRows: rows.affectedRows,
        warningStatus: rows.warningStatus,
    } };
}

class Driver {
    // options:
    //   maxTransactionsPerOwner  concurrent transactions one resource may hold
    //   cleanupTimeoutMs         deadline for session cleanup after an answer was sent
    //   killQuery(threadId)      best-effort cancellation of a statement on the server
    //   sessionOptions           { database, charset } restored by every session cleanup
    constructor(pool, timeoutMs, transactionTimeoutMs = 10000, onTransaction = () => {}, options = {}) {
        this.pool = pool;
        this.timeoutMs = timeoutMs;
        this.stopped = false;
        this.active = new Set();
        this.leases = new Set();
        this.cleanups = new Set();
        this.acquiring = 0;
        this.transactions = new Map();
        this.closedTransactions = new Map();
        this.transactionTimeoutMs = transactionTimeoutMs;
        this.onTransaction = onTransaction;
        this.maxTransactionsPerOwner = options.maxTransactionsPerOwner ?? Infinity;
        this.cleanupTimeoutMs = options.cleanupTimeoutMs ?? 5000;
        this.killQuery = options.killQuery ?? null;
        // Copied so a later change to the caller's object cannot alter cleanup.
        this.sessionOptions = { ...(options.sessionOptions ?? {}) };
        this.instance = randomUUID();
        this.sequence = 0;
        this.cleanupFailures = 0;
        this.resetFailures = 0;
        this.totals = { queries: 0, errors: 0, timeouts: 0, poolExhausted: 0, killRequests: 0, maxAcquireMs: 0, textFallbacks: 0 };
        this.database = { ready: false, code: null };
        this.readyTimer = null;
        this.wakeReady = null;
        this.closing = null;
    }
    // "Ready" means a probe has reached the database since start. Probing begins
    // only when the bridge asks for it, so a driver that is never probed (or is
    // built by a test) opens no connection.
    readiness() { return { ready: this.database.ready, code: this.database.code }; }
    async probe() {
        try {
            await this.run('SELECT 1', [], 'health');
            this.database.ready = true; this.database.code = null;
        } catch (error) {
            this.database.ready = false;
            this.database.code = typeof error?.code === 'string' ? error.code : 'DATABASE_ERROR';
        }
        return this.database.ready;
    }
    // Probe until the database answers: one second apart, doubling up to ten.
    // report(ready, code, attempt) is called on the first failure, every tenth
    // failure after that, and on success.
    async awaitDatabase(report = () => {}) {
        for (let attempt = 1; !this.stopped; attempt++) {
            if (await this.probe()) { report(true, null, attempt); return true; }
            if (attempt === 1 || attempt % 10 === 0) report(false, this.database.code, attempt);
            await new Promise(resolve => {
                this.wakeReady = resolve;
                this.readyTimer = setTimeout(resolve, Math.min(10000, 1000 * 2 ** Math.min(attempt - 1, 4)));
            });
        }
        return false;
    }
    async acquire() {
        this.acquiring++;
        const started = Date.now();
        try {
            const connection = await this.pool.getConnection();
            const waited = Date.now() - started;
            if (waited > this.totals.maxAcquireMs) this.totals.maxAcquireMs = waited;
            const lease = new ConnectionLease(connection, () => this.leases.delete(lease), this.sessionOptions);
            lease.onTextFallback = () => { this.totals.textFallbacks++; };
            this.leases.add(lease);
            return lease;
        } catch (error) {
            // The pinned mysql2 pool reports a full queue (or a full pool when it
            // does not queue) and a closed pool with uncoded errors.
            if (error?.message === 'No connections available.' || error?.message === 'Queue limit reached.') {
                error.code = 'POOL_EXHAUSTED';
                this.totals.poolExhausted++;
            } else if (error?.message === 'Pool is closed.') error.code = 'RESOURCE_STOPPED';
            throw error;
        } finally { this.acquiring--; }
    }
    // Destroy a lease. With kill, a statement that may still be executing is also
    // cancelled on the server: closing the client socket alone does not stop it.
    destroy(lease, { kill = false } = {}) {
        if (!lease || lease.closed) return;
        const threadId = kill ? lease.threadId() : undefined;
        try { lease.destroy(); }
        catch (_) { this.cleanupFailures++; }
        if (threadId && this.killQuery) {
            this.totals.killRequests++;
            try { Promise.resolve(this.killQuery(threadId)).catch(() => {}); } catch (_) { /* best effort */ }
        }
    }
    // Restore a healthy connection to the pool after the caller has been answered.
    // The connection is not lent again until the session is clean; failure or a
    // hung cleanup retires it. This can never change an answer already delivered.
    cleanup(lease) {
        const work = (async () => {
            let timer;
            try {
                await Promise.race([
                    lease.reset(),
                    new Promise((_, reject) => { timer = setTimeout(() => reject(fault('CLEANUP_TIMEOUT')), this.cleanupTimeoutMs); }),
                ]);
                lease.release();
            } catch (_) {
                this.resetFailures++;
                this.destroy(lease);
            } finally { clearTimeout(timer); }
        })().finally(() => this.cleanups.delete(work));
        this.cleanups.add(work);
        return work;
    }
    // Resolves once every connection returned after an answer has been cleaned.
    async drain() {
        while (this.cleanups.size) await Promise.allSettled([...this.cleanups]);
    }
    run(sql, parameters, owner = 'unknown') {
        if (this.stopped) return Promise.reject(fault('RESOURCE_STOPPED', 'unknown'));
        this.totals.queries++;
        return new Promise((resolve, reject) => {
            let lease, settled = false;
            const finish = (error, result) => {
                if (settled) return;
                settled = true;
                clearTimeout(timer);
                this.active.delete(operation);
                if (error) { this.totals.errors++; reject(error); } else resolve(result);
            };
            const cancel = code => {
                if (code === 'QUERY_TIMEOUT') this.totals.timeouts++;
                this.destroy(lease, { kill: true });
                finish(fault(code, 'unknown'));
            };
            const operation = { owner, cancel };
            const timer = setTimeout(() => cancel('QUERY_TIMEOUT'), this.timeoutMs);
            this.active.add(operation);
            (async () => {
                try {
                    lease = await this.acquire();
                    // Acquisition can complete after cancellation. Never execute then.
                    if (settled) {
                        if (this.stopped) this.destroy(lease); else lease.release();
                        return;
                    }
                    let value;
                    try {
                        const [rows, fields] = await lease.execute(sql, parameters);
                        value = normalize(rows, fields);
                    } catch (error) {
                        // A cancelled operation already destroyed its lease.
                        if (settled) return;
                        finish(error);
                        // The server answered with a statement error, or the result
                        // shape was unsupported: the session is intact once reset.
                        // Anything else (transport, protocol) leaves it uncertain.
                        if (isStatementError(error) || error.code === 'RESULT_TYPE') await this.cleanup(lease);
                        else this.destroy(lease);
                        return;
                    }
                    if (settled) return;
                    // Answer first: session cleanup must not delay the caller, nor
                    // turn an applied statement into a reported failure.
                    finish(null, value);
                    await this.cleanup(lease);
                } catch (error) {
                    finish(error);
                } finally {
                    if (lease && !lease.closed) this.destroy(lease);
                }
            })();
        });
    }
    logTransaction(tx, event, error) {
        try {
            this.onTransaction({ transactionId: tx.id, resource: tx.owner, event,
                durationMs: Date.now() - tx.started, queries: tx.queries, code: error?.code });
        } catch (_) { /* Diagnostics must not change database outcomes. */ }
    }
    retire(tx, error) {
        if (!tx.active) return;
        tx.active = false;
        clearTimeout(tx.timer);
        this.transactions.delete(tx.id);
        this.closedTransactions.set(tx.id, { owner: tx.owner, error });
        // Keep bounded tombstones for late timeout/error replies, never connections.
        if (this.closedTransactions.size > 1024) this.closedTransactions.delete(this.closedTransactions.keys().next().value);
    }
    abort(tx, error) {
        if (!tx.active) return;
        tx.failure = error;
        if (error.code === 'TRANSACTION_TIMEOUT') this.totals.timeouts++;
        // A statement still running on the server is cancelled with the connection.
        this.destroy(tx.lease, { kill: tx.busy });
        this.retire(tx, error);
        tx.rejectCancellation(error);
        this.logTransaction(tx, 'ABORT', error);
    }
    // The deadline fires from a timer, so the outcome is decided when it fires:
    // only a COMMIT that was already sent can have an unknown result.
    armDeadline(tx) {
        clearTimeout(tx.timer);
        tx.timer = setTimeout(() => this.abort(tx,
            fault('TRANSACTION_TIMEOUT', tx.phase === 'commit' ? 'unknown' : 'rolled_back')), this.transactionTimeoutMs);
    }
    transaction(owner, id) {
        const tx = this.transactions.get(id);
        const closed = this.closedTransactions.get(id);
        if ((tx || closed) && (tx || closed).owner !== owner) throw fault('TRANSACTION_OWNER', 'not_executed');
        if (!tx) throw closed?.error || fault('TRANSACTION_CLOSED', 'not_executed');
        return tx;
    }
    ownedBy(owner) {
        let count = 0;
        for (const tx of this.transactions.values()) if (tx.owner === owner) count++;
        return count;
    }
    race(tx, work) { return Promise.race([work, tx.cancellation]); }
    async begin(owner) {
        if (this.stopped) throw fault('RESOURCE_STOPPED', 'unknown');
        // One resource holding every connection would starve all the others.
        if (this.ownedBy(owner) >= this.maxTransactionsPerOwner) throw fault('TRANSACTION_LIMIT', 'not_executed');
        const tx = { id: `${this.instance}:${++this.sequence}`, owner, active: true,
            started: Date.now(), phase: 'acquiring', busy: false, queries: 0 };
        tx.cancellation = new Promise((_, reject) => { tx.rejectCancellation = reject; });
        tx.cancellation.catch(() => {});
        this.armDeadline(tx);
        this.transactions.set(tx.id, tx);
        try {
            tx.lease = await this.race(tx, this.acquire().then(lease => {
                if (!tx.active) {
                    if (this.stopped) this.destroy(lease); else lease.release();
                    throw tx.failure || fault('TRANSACTION_CLOSED', 'not_executed');
                }
                tx.lease = lease;
                return lease;
            }));
            tx.phase = 'begin';
            await this.race(tx, tx.lease.command('START TRANSACTION'));
            tx.phase = 'active';
            this.logTransaction(tx, 'BEGIN');
            return { id: tx.id };
        } catch (error) { this.abort(tx, error); throw error; }
    }
    async transactionQuery(owner, id, sql, parameters) {
        const tx = this.transaction(owner, id);
        if (tx.busy || tx.phase !== 'active') {
            const error = fault('TRANSACTION_BUSY', 'not_executed'); this.abort(tx, error); throw error;
        }
        tx.busy = true;
        tx.phase = 'query';
        try {
            const [rows, fields] = await this.race(tx, tx.lease.execute(sql, parameters));
            const value = normalize(rows, fields);
            tx.queries++;
            this.logTransaction(tx, 'QUERY');
            return value;
        } catch (error) {
            tx.busy = false;
            if (tx.active) return this.complete(tx, false, error);
            throw tx.failure || error;
        } finally {
            if (tx.active && tx.phase === 'query') { tx.busy = false; tx.phase = 'active'; }
        }
    }
    // COMMIT or ROLLBACK, then hand the connection to background cleanup. The
    // work deadline stops here: finishing gets a fresh window so a transaction
    // that used its whole budget is not cut off mid-COMMIT for no reason.
    async complete(tx, commit, originalError) {
        tx.busy = true;
        tx.phase = commit ? 'commit' : 'rollback';
        this.armDeadline(tx);
        try {
            await this.race(tx, tx.lease.command(commit ? 'COMMIT' : 'ROLLBACK'));
        } catch (error) {
            this.abort(tx, originalError || error);
            throw originalError || error;
        }
        // The statement is acknowledged, so the outcome is decided. Session
        // cleanup can no longer change what the caller is told.
        const lease = tx.lease;
        this.retire(tx, originalError);
        this.logTransaction(tx, commit ? 'COMMIT' : 'ROLLBACK', originalError);
        this.cleanup(lease);
        if (originalError) throw originalError;
        return commit;
    }
    async finish(owner, id, commit) {
        const tx = this.transaction(owner, id);
        if (tx.busy || tx.phase !== 'active') {
            const error = fault('TRANSACTION_BUSY', 'not_executed'); this.abort(tx, error); throw error;
        }
        return this.complete(tx, commit === true);
    }
    // A fixed list of statements as one transaction: commit when every statement
    // succeeded, otherwise roll back and raise the first failure.
    async batch(owner, statements) {
        const tx = await this.begin(owner);
        try {
            for (const statement of statements) await this.transactionQuery(owner, tx.id, statement.sql, statement.parameters);
        } catch (error) {
            // A failed statement already rolled back; anything else must not leave the transaction open.
            if (this.transactions.has(tx.id)) await this.finish(owner, tx.id, false).catch(() => {});
            throw error;
        }
        return this.finish(owner, tx.id, true);
    }
    abortOwner(owner) {
        for (const operation of [...this.active]) if (operation.owner === owner) operation.cancel('RESOURCE_STOPPED');
        for (const tx of [...this.transactions.values()]) if (tx.owner === owner) this.abort(tx, fault('RESOURCE_STOPPED', 'unknown'));
    }
    diagnostics() {
        const pool = this.pool.pool;
        return { stopped: this.stopped, activeRequests: this.active.size, acquiring: this.acquiring,
            checkedOut: this.leases.size, cleaning: this.cleanups.size, transactions: this.transactions.size,
            cleanupFailures: this.cleanupFailures, resetFailures: this.resetFailures, totals: { ...this.totals },
            ready: this.database.ready, readyCode: this.database.code,
            poolConnections: pool?._allConnections?.length ?? null,
            poolFree: pool?._freeConnections?.length ?? null, poolQueued: pool?._connectionQueue?.length ?? null,
            transactionDetails: [...this.transactions.values()].map(tx => ({ id: tx.id, resource: tx.owner,
                phase: tx.phase, durationMs: Date.now() - tx.started, queries: tx.queries })) };
    }
    close() {
        if (this.closing) return this.closing;
        this.stopped = true;
        clearTimeout(this.readyTimer);
        this.wakeReady?.();
        for (const operation of [...this.active]) operation.cancel('RESOURCE_STOPPED');
        for (const tx of [...this.transactions.values()]) this.abort(tx, fault('RESOURCE_STOPPED', 'unknown'));
        // Connections still finishing session cleanup are retired with the pool.
        for (const lease of [...this.leases]) this.destroy(lease);
        this.closing = Promise.resolve().then(() => this.pool.end());
        return this.closing;
    }
}

module.exports = { Driver, ConnectionLease, normalize };
