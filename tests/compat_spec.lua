-- Run from the resource directory: lua5.4 tests/compat_spec.lua
local passed = 0
local function check(name, test)
    local ok, err = pcall(test)
    if not ok then
        if type(err) == 'table' then err = tostring(err.code) .. ': ' .. tostring(err.message) end
        error(name .. ': ' .. tostring(err), 0)
    end
    passed = passed + 1
    print('PASS ' .. name)
end

-- useNative: load the real lib/DB.lua into the consumer (so the adapter is exercised through the
-- native library and a mocked provider export). Otherwise DB is a recording double.
local function runtime(useNative)
    local r = { calls = {}, threads = {}, handlers = {}, messages = {}, loads = 0, state = 'started', resolutions = 0,
        transactions = 0, ready = true, awaited = 0, result = {} }
    local env = setmetatable({}, { __index = _G })
    r.env = env
    env.IsDuplicityVersion = function() return true end
    env.GetCurrentResourceName = function() return 'compat-consumer' end
    env.GetResourceState = function() return r.state end
    env.AddEventHandler = function(name, callback) r.handlers[name] = callback end
    env.print = function(message) r.messages[#r.messages + 1] = message end
    env.LoadResourceFile = function(resource, path)
        assert(resource == 'feather-mysql' and (path == 'lib/DB.lua' or path == 'lib/Named.lua'))
        if path == 'lib/DB.lua' then r.loads = r.loads + 1 end
        local file = assert(io.open(path))
        local source = file:read('*a')
        file:close()
        return source
    end
    env.GetConvar = function(_, default) return default end
    env.GetGameTimer = function() return 0 end
    env.Wait = function() coroutine.yield() end
    env.CreateThread = function(callback) r.threads[#r.threads + 1] = coroutine.create(callback) end
    env.promise = { new = function()
        return { resolve = function(self, value)
            r.resolutions = r.resolutions + 1
            self.done, self.value = true, value
        end }
    end }
    env.Citizen = { Await = function(deferred)
        while not deferred.done do coroutine.yield() end
        return deferred.value
    end }
    env.exports = { ['feather-mysql'] = {
        ExecuteV1 = function(_, method, sql, parameters, callback)
            r.calls[#r.calls + 1] = { method = method, sql = sql, parameters = parameters }
            if r.delayed then r.held = callback else callback(r.envelope) end
        end,
        ReadyV1 = function(_, callback) callback({ ok = true, value = { ready = r.ready } }) end,
    } }
    if not useNative then
        local function record(method, sql, ...)
            r.calls[#r.calls + 1] = { method = method, sql = sql, parameters = table.pack(...) }
            if r.failure then error(r.failure, 0) end
            if type(r.result) == 'function' then return r.result(method, sql) end
            return r.result
        end
        -- Mirrors DB.transaction: a tx.* failure poisons it, and only a callback result of true commits.
        local function transaction(callback)
            r.transactions = r.transactions + 1
            local poisoned
            local tx = {}
            for _, method in ipairs({ 'query', 'one', 'value', 'insert', 'exec', 'raw' }) do
                tx[method] = function(sql, ...)
                    if r.txFailure and not poisoned then poisoned = r.txFailure; error(poisoned, 0) end
                    if poisoned then error(poisoned, 0) end
                    return record('tx.' .. method, sql, ...)
                end
            end
            local ok, result = pcall(callback, tx)
            if not ok then error(result, 0) end
            if poisoned then error(poisoned, 0) end
            return result == true
        end
        env.DB = {
            query = function(sql, ...) return record('query', sql, ...) end,
            one = function(sql, ...) return record('one', sql, ...) end,
            value = function(sql, ...) return record('value', sql, ...) end,
            insert = function(sql, ...) return record('insert', sql, ...) end,
            exec = function(sql, ...) return record('exec', sql, ...) end,
            raw = function(sql, ...) return record('raw', sql, ...) end,
            transaction = transaction,
            isReady = function() return r.ready end,
            awaitReady = function() r.awaited = r.awaited + 1; return true end,
        }
    end
    function r.load() assert(loadfile('lib/MySQL.lua', 't', env))() end
    function r.pump()
        local scheduled = r.threads
        r.threads = {}
        for _, thread in ipairs(scheduled) do
            local ok, err = coroutine.resume(thread)
            assert(ok, tostring(err))
            if coroutine.status(thread) ~= 'dead' then r.threads[#r.threads + 1] = thread end
        end
    end
    function r.await(callback)
        local thread = coroutine.create(callback)
        local ok, err = coroutine.resume(thread)
        if not ok then error(err, 0) end
        assert(coroutine.status(thread) == 'dead', 'Unexpected suspended test')
    end
    return r
end

-- Errors leave the adapter as text ("[feather-mysql] CODE: reason (details)" and a stack trace), because
-- the Cfx runtime prints an uncaught table error as only "error object is not a string".
local function asError(err)
    assert(type(err) == 'string', 'Adapter errors must be text, got ' .. type(err))
    local code, message = err:match('^%[feather%-mysql%] ([%u_]+): ([^\n]*)')
    assert(code, err)
    assert(err:find('stack traceback', 1, true), 'The text carries the caller\'s stack trace')
    return { code = code, message = message, text = err }
end
local function failing(fn)
    local ok, err = pcall(fn)
    assert(not ok, 'expected a failure')
    return asError(err)
end

check('import alone loads the native API in the consumer environment', function()
    local r = runtime(true)
    r.load()
    assert(r.loads == 1 and type(r.env.DB.query) == 'function' and r.handlers.onResourceStop)
    r.envelope = { ok = true, value = { { id = 7 } } }
    r.await(function() assert(r.env.MySQL.query.await('SELECT ?', { 7 })[1].id == 7) end)
    assert(r.calls[1].method == 'raw' and r.calls[1].parameters[1] == 7)
end)

check('an existing DB table and its functions are reused without replacement', function()
    local r = runtime(false)
    local db, query = r.env.DB, r.env.DB.query
    r.load()
    assert(r.loads == 0 and r.env.DB == db and r.env.DB.query == query)
    assert(next(r.handlers) == nil)
    local ok, err = pcall(r.load)
    assert(not ok and tostring(err):find('MySQL is already defined', 1, true))
    assert(r.env.DB == db)
end)

check('the five basic await methods delegate once to their native functions, query through raw', function()
    local r = runtime(false)
    r.load()
    local mappings = { query = 'raw', single = 'one', scalar = 'value', insert = 'insert', update = 'exec' }
    local parameters = { "O'Brien ?; --", false, 0, '', '9007199254740993' }
    for method, native in pairs(mappings) do
        r.result = { marker = native }
        local before = #r.calls
        assert(r.env.MySQL[method].await('SELECT ?, ?, ?, ?, ?', parameters) == r.result)
        local call = r.calls[#r.calls]
        assert(#r.calls == before + 1 and call.method == native)
        assert(call.sql == 'SELECT ?, ?, ?, ?, ?' and call.parameters.n == 5)
        for i = 1, 5 do assert(call.parameters[i] == parameters[i]) end
    end
end)

check('all callback methods run asynchronously and return results once', function()
    local r = runtime(false)
    r.load()
    local mappings = { query = 'raw', single = 'one', scalar = 'value', insert = 'insert', update = 'exec', prepare = 'raw' }
    for method, native in pairs(mappings) do
        local count, before = 0, #r.calls
        r.result = native
        r.env.MySQL[method]('SELECT ?', { 42 }, function(value, err)
            count = count + 1
            assert(value == native and err == nil)
        end)
        assert(count == 0 and #r.calls == before)
        r.pump()
        assert(count == 1 and #r.calls == before + 1 and r.calls[#r.calls].method == native)
    end
    assert(#r.messages == 0)
end)

check('query returns rows for a read and a write header for a write, as oxmysql did', function()
    local r = runtime(false)
    r.load()
    r.result = { { id = 1 }, { id = 2 } }
    assert(#r.env.MySQL.query.await('SELECT id FROM t') == 2)
    r.result = { affectedRows = 3, insertId = 9, warningStatus = 0 }
    local header = r.env.MySQL.query.await('UPDATE t SET n = ?', { 1 })
    assert(header.affectedRows == 3 and header.insertId == 9)
    assert(r.calls[2].method == 'raw', 'Both go through the same native call; the provider decides the shape')
end)

check('omitted, nil and empty parameter tables produce zero arguments', function()
    local r = runtime(false)
    r.load()
    r.env.MySQL.query.await('SELECT 1')
    r.env.MySQL.query.await('SELECT 1', nil)
    r.env.MySQL.query.await('SELECT 1', {})
    local completed = 0
    local function done(_, err) assert(err == nil); completed = completed + 1 end
    r.env.MySQL.query('SELECT 1', done)
    r.env.MySQL.query('SELECT 1', nil, done)
    r.env.MySQL.query('SELECT 1', {}, done)
    r.pump()
    assert(#r.calls == 6 and completed == 3)
    for _, call in ipairs(r.calls) do assert(call.parameters.n == 0) end
end)

check('nil, false, zero and empty string results are not confused with errors', function()
    local r = runtime(false)
    r.load()
    local function verify(value)
        r.result = value
        assert(r.env.MySQL.scalar.await('SELECT 1') == value)
        local called = false
        r.env.MySQL.scalar('SELECT 1', function(result, err)
            assert(result == value and err == nil)
            called = true
        end)
        r.pump()
        assert(called)
    end
    verify(nil); verify(false); verify(0); verify('')
end)

check('callback parameters are snapshotted before the thread starts', function()
    local r = runtime(false)
    r.load()
    local parameters = { 'original', false }
    r.env.MySQL.query('SELECT ?, ?', parameters, function() end)
    parameters[1], parameters[2], parameters[3] = 'changed', true, 3
    r.pump()
    assert(r.calls[1].parameters.n == 2)
    assert(r.calls[1].parameters[1] == 'original' and r.calls[1].parameters[2] == false)
end)

check('invalid parameter tables fail without dispatch in both forms', function()
    local r = runtime(false)
    r.load()
    for _, parameters in ipairs({ 7, false, 'value', { id = 1 }, { [2] = 1 }, { [0] = 1 }, { [1.5] = 1 }, { 1, named = 2 } }) do
        local ok, err = pcall(r.env.MySQL.query.await, 'SELECT ?', parameters)
        assert(not ok and asError(err).code == 'INVALID_ARGUMENT')
        local count, failure = 0
        r.env.MySQL.query('SELECT ?', parameters, function(value, callbackError)
            assert(value == nil)
            count, failure = count + 1, callbackError
        end)
        r.pump()
        assert(count == 1 and failure.code == 'INVALID_ARGUMENT')
    end
    assert(#r.calls == 0)
end)

check('database errors preserve their structured identity, and a raising callback is shown, not retried', function()
    local r = runtime(false)
    r.load()
    r.failure = { code = 'DATABASE_ERROR', message = 'safe', driverCode = 'ER_PARSE_ERROR', resource = 'compat-consumer', queryId = 9 }
    local ok, err = pcall(r.env.MySQL.query.await, 'broken')
    assert(not ok and asError(err).code == 'DATABASE_ERROR')
    assert(err:find('driverCode=ER_PARSE_ERROR', 1, true), 'The database error code reaches the text: ' .. err)
    assert(err:find('compat_spec.lua', 1, true), 'The stack trace names the caller, which Cfx does not for a resumed coroutine')
    local count = 0
    r.env.MySQL.query('broken', function(value, failure)
        count = count + 1
        assert(value == nil and failure == r.failure)
        error('consumer callback failed')
    end)
    r.pump()
    assert(count == 1 and #r.calls == 2 and #r.messages == 1)
    -- The SQL is not retried, but the consumer's own bug must stay visible.
    assert(r.messages[1]:find('consumer callback failed', 1, true), r.messages[1])
    assert(r.messages[1]:find('feather-mysql', 1, true))
end)

check('callable Cfx callback tables are supported and missing callbacks fail clearly', function()
    local r = runtime(false)
    r.load()
    r.result = 5
    local received
    local callback = setmetatable({}, { __call = function(_, value, err)
        assert(not err)
        received = value
    end })
    r.env.MySQL.scalar('SELECT 5', callback)
    r.pump()
    assert(received == 5)
    local ok, err = pcall(function() r.env.MySQL.query('SELECT 1') end)
    assert(not ok and asError(err).code == 'INVALID_CALLBACK')
    ok, err = pcall(function() r.env.MySQL.query('SELECT 1', {}, 'invalid') end)
    assert(not ok and asError(err).code == 'INVALID_CALLBACK' and #r.calls == 1)
end)

check('prepare runs once for one parameter list and once per list, atomically, for a list of lists', function()
    local r = runtime(false)
    r.load()
    r.result = function(_, sql) return { affectedRows = 1, insertId = 0, sql = sql } end
    local single = r.env.MySQL.prepare.await('INSERT IGNORE INTO t (a, b) VALUES (?, ?)', { 1, 'x' })
    assert(single.affectedRows == 1 and #r.calls == 1 and r.calls[1].method == 'raw' and r.transactions == 0)
    local before = #r.calls
    local rows = { { 'owner', 1, 2, 3 }, { 'owner', 4, 5, 6 }, { 'owner', 7, 8, 9 } }
    local results = r.env.MySQL.prepare.await('INSERT IGNORE INTO t (o, x, y, z) VALUES (?, ?, ?, ?)', rows)
    assert(r.transactions == 1 and #r.calls == before + 3 and #results == 3)
    for i = 1, 3 do
        local call = r.calls[before + i]
        assert(call.method == 'tx.raw' and call.parameters.n == 4 and call.parameters[2] == rows[i][2])
    end
    r.env.MySQL.prepare('INSERT IGNORE INTO t (o, x, y, z) VALUES (?, ?, ?, ?)', rows, function(value, err) assert(#value == 3 and err == nil) end)
    r.pump()
    assert(r.transactions == 2)
    r.txFailure = { code = 'DATABASE_ERROR', message = 'Database rejected the statement' }
    local err = failing(function() r.env.MySQL.prepare.await('INSERT INTO t VALUES (?)', { { 1 }, { 2 } }) end)
    assert(err.code == 'DATABASE_ERROR' and err.message == 'Database rejected the statement', 'A failed batch raises like every other await call')
    r.txFailure = nil
    local count = #r.calls
    err = failing(function() r.env.MySQL.prepare.await('INSERT INTO t VALUES (?)', { { 1 }, { named = 2 } }) end)
    assert(err.code == 'INVALID_ARGUMENT', 'A malformed parameter list is rejected')
    assert(#r.calls == count + 1, 'Only the statement before the malformed one ran, and its transaction is failed')
end)

check('transaction accepts the three query forms and returns true only when committed', function()
    local r = runtime(false)
    r.load()
    local committed = r.env.MySQL.transaction.await({
        'UPDATE a SET n = 1',
        { query = 'INSERT INTO b VALUES (?, ?)', values = { "O'Brien", false } },
        { 'DELETE FROM c WHERE id = ?', { 7 } },
    })
    assert(committed == true and r.transactions == 1 and #r.calls == 3)
    assert(r.calls[1].method == 'tx.raw' and r.calls[1].sql == 'UPDATE a SET n = 1' and r.calls[1].parameters.n == 0)
    assert(r.calls[2].parameters[1] == "O'Brien" and r.calls[2].parameters[2] == false)
    assert(r.calls[3].parameters[1] == 7)
    local viaCallback
    r.env.MySQL.transaction({ 'UPDATE a SET n = 2' }, function(result) viaCallback = result end)
    assert(viaCallback == nil, 'The callback form is asynchronous')
    r.pump()
    assert(viaCallback == true)
    r.env.MySQL.transaction({ 'UPDATE a SET n = 3' })
    r.pump()
    assert(r.transactions == 3, 'The callback is optional')
end)

check('transaction returns false, and says why, when a statement fails; invalid input never starts one', function()
    local r = runtime(false)
    r.load()
    r.txFailure = { code = 'DATABASE_ERROR', message = 'Database rejected the statement', driverCode = 'ER_DUP_ENTRY' }
    assert(r.env.MySQL.transaction.await({ 'INSERT INTO a VALUES (1)', 'INSERT INTO a VALUES (1)' }) == false)
    assert(#r.messages == 1 and r.messages[1]:find('MySQL.transaction failed', 1, true) and r.messages[1]:find('DATABASE_ERROR', 1, true))
    r.txFailure = nil
    local before = r.transactions
    for _, invalid in ipairs({ 'text', {}, { 5 }, { { values = { 1 } } }, { 'ok', { query = 7 } }, { { 'SELECT ?', { 1, nil, 3 } } } }) do
        local err = failing(function() r.env.MySQL.transaction.await(invalid) end)
        assert(err.code == 'INVALID_ARGUMENT', tostring(err.code))
    end
    assert(r.transactions == before, 'No transaction was started for any of them')
    local err = failing(function() r.env.MySQL.transaction({ 'SELECT 1' }, 'not a function') end)
    assert(err.code == 'INVALID_CALLBACK')
    local delivered
    r.env.MySQL.transaction({ 'UPDATE a SET n = 1' }, function(result) delivered = result; error('callback boom') end)
    r.messages = {}
    r.pump()
    assert(delivered == true and #r.messages == 1 and r.messages[1]:find('callback boom', 1, true))
end)

check('startTransaction commits unless the callback returns false, as oxmysql did', function()
    local r = runtime(false)
    r.load()
    assert(r.env.MySQL.startTransaction(function() end) == true, 'Returning nothing commits')
    assert(r.env.MySQL.startTransaction(function() return true end) == true)
    assert(r.env.MySQL.startTransaction(function() return 'anything else' end) == true)
    assert(r.env.MySQL.startTransaction(function() return false end) == false, 'Only an explicit false rolls back')
    assert(r.transactions == 4 and #r.messages == 0)
end)

check('startTransaction gives the callback a query function returning rows or a write header', function()
    local r = runtime(false)
    r.load()
    r.result = function(_, sql)
        if sql:find('SELECT', 1, true) then return { { request_fingerprint = 'abc' } } end
        return { affectedRows = 1, insertId = 0 }
    end
    local seen = {}
    local committed = r.env.MySQL.startTransaction(function(query)
        seen.write = query('INSERT IGNORE INTO receipts (r, f) VALUES (?, ?)', { 'resource', 'req-1' })
        seen.rows = query('SELECT request_fingerprint FROM receipts WHERE r = ? FOR UPDATE', { 'resource' })
        seen.none = query('SELECT 1')
        return true
    end)
    assert(committed == true and seen.write.affectedRows == 1 and seen.rows[1].request_fingerprint == 'abc')
    assert(#r.calls == 3 and r.calls[1].parameters[1] == 'resource' and r.calls[1].parameters[2] == 'req-1')
    assert(r.calls[3].parameters.n == 0 and r.calls[1].method == 'tx.raw')
end)

check('startTransaction returns false when anything inside fails, even if the callback swallows the error', function()
    local r = runtime(false)
    r.load()
    local failure = { code = 'DATABASE_ERROR', message = 'Database rejected the statement' }
    assert(r.env.MySQL.startTransaction(function() error('callback bug') end) == false)
    assert(r.messages[#r.messages]:find('callback bug', 1, true))
    r.txFailure = failure
    local swallowed = r.env.MySQL.startTransaction(function(query)
        local ok = pcall(query, 'INSERT INTO a VALUES (1)')
        assert(not ok)
        return true   -- the caller claims success, but the transaction is poisoned
    end)
    assert(swallowed == false, 'A failed statement can never commit')
    assert(r.messages[#r.messages]:find('DATABASE_ERROR', 1, true))
    r.txFailure = nil
    assert(r.env.MySQL.startTransaction(function(query) query('SELECT ?', { 1, nil, 3 }); return true end) == false,
        'A malformed parameter list fails the transaction')
    local err = failing(function() r.env.MySQL.startTransaction('not a function') end)
    assert(err.code == 'INVALID_CALLBACK')
    err = failing(function() r.env.MySQL.startTransaction() end)
    assert(err.code == 'INVALID_CALLBACK')
end)

check('ready runs its callback in a thread once the provider is reachable', function()
    local r = runtime(false)
    r.load()
    local calls = 0
    r.env.MySQL.ready(function() calls = calls + 1 end)
    assert(calls == 0, 'It never runs inline')
    r.pump()
    assert(calls == 1 and r.awaited == 1)
    r.env.MySQL.ready(function() error('startup bug') end)
    r.messages = {}
    r.pump()
    assert(#r.messages == 1 and r.messages[1]:find('startup bug', 1, true))
    local err = failing(function() r.env.MySQL.ready('not a function') end)
    assert(err.code == 'INVALID_CALLBACK')
end)

check('isReady and awaitConnection delegate to the native readiness', function()
    local r = runtime(false)
    r.load()
    assert(r.env.MySQL.isReady() == false, 'Outside a coroutine it cannot ask, so it says false')
    r.await(function()
        r.ready = true
        assert(r.env.MySQL.isReady() == true)
        r.ready = false
        assert(r.env.MySQL.isReady() == false)
        assert(r.env.MySQL.awaitConnection() == true)
    end)
    assert(r.awaited == 1)
end)

check('unsupported APIs raise errors on access and are not implemented as stubs', function()
    local r = runtime(false)
    r.load()
    for _, method in ipairs({ 'rawExecute', 'Async', 'Sync', 'execute', 'fetch', 'unknown' }) do
        assert(rawget(r.env.MySQL, method) == nil)
        local ok, err = pcall(function() return r.env.MySQL[method] end)
        err = ok and err or asError(err)
        assert(not ok and err.code == 'UNSUPPORTED_API' and err.message:find(method, 1, true))
    end
    for _, method in ipairs({ 'query', 'single', 'scalar', 'insert', 'update', 'prepare', 'transaction', 'startTransaction', 'ready', 'isReady', 'awaitConnection' }) do
        assert(rawget(r.env.MySQL, method) ~= nil, method .. ' must exist')
    end
    assert(#r.calls == 0 and #r.threads == 0)
end)

check('native coroutine checks, errors and unavailable state remain effective', function()
    local r = runtime(true)
    r.load()
    local ok, err = pcall(r.env.MySQL.query.await, 'SELECT 1')
    assert(not ok and asError(err).code == 'INVALID_CONTEXT' and #r.calls == 0)
    r.state = 'stopped'
    local failure
    r.env.MySQL.query('SELECT 1', function(_, callbackError) failure = callbackError end)
    r.pump()
    assert(failure.code == 'UNAVAILABLE' and #r.calls == 0)
    r.state = 'started'
    r.envelope = { ok = false, error = { code = 'RESULT_TYPE', message = 'statement may already have executed' } }
    r.await(function()
        local succeeded, resultError = pcall(r.env.MySQL.query.await, 'CREATE TABLE x (n INT)')
        assert(not succeeded and asError(resultError).code == 'RESULT_TYPE' and resultError:find('may already have executed', 1, true))
    end)
    assert(#r.calls == 1 and r.calls[1].method == 'raw')
end)

check('pending compatibility calls retain native stop and late-response protection', function()
    local r = runtime(true)
    r.load()
    r.delayed = true
    local count, failure = 0
    r.env.MySQL.query('SELECT 1', function(_, err) count, failure = count + 1, err end)
    r.pump()
    -- The waiting request plus the watchdog sweeper that guards outstanding requests.
    assert(count == 0 and #r.threads == 2)
    r.handlers.onResourceStop('feather-mysql')
    r.pump()
    r.pump()
    assert(count == 1 and failure.code == 'RESOURCE_STOPPED' and #r.threads == 0, 'The sweeper ends once nothing is outstanding')
    r.held({ ok = true, value = {} })
    assert(count == 1 and r.resolutions == 1)
end)

check('through the native library, ready keeps waiting until the provider reports the database reachable', function()
    local r = runtime(true)
    r.load()
    r.ready = false
    local ran = 0
    r.env.MySQL.ready(function() ran = ran + 1 end)
    r.pump(); r.pump(); r.pump()
    assert(ran == 0 and #r.threads >= 1, 'The ready thread keeps polling while the database is not reachable')
    r.ready = true
    r.pump(); r.pump()
    assert(ran == 1, 'It runs exactly once, as soon as the database is reachable')
    r.pump(); r.pump()
    assert(ran == 1 and #r.threads == 0, 'And nothing keeps polling afterwards')
end)

check('named placeholders are rewritten to positional ones for every adapter call', function()
    local r = runtime(false)
    r.load()
    r.result = {}
    -- await and callback forms
    r.env.MySQL.query.await('DELETE FROM bcchousing WHERE houseid = @houseid', { houseid = 5 })
    local call = r.calls[#r.calls]
    assert(call.method == 'raw' and call.sql == 'DELETE FROM bcchousing WHERE houseid = ?' and call.parameters.n == 1 and call.parameters[1] == 5)
    r.env.MySQL.update.await('UPDATE t SET a = @a, b = :b WHERE id = @a', { ['@a'] = 1, [':b'] = false })
    call = r.calls[#r.calls]
    assert(call.method == 'exec' and call.sql == 'UPDATE t SET a = ?, b = ? WHERE id = ?')
    assert(call.parameters.n == 3 and call.parameters[1] == 1 and call.parameters[2] == false and call.parameters[3] == 1)
    local delivered
    r.env.MySQL.scalar('SELECT COUNT(*) FROM t WHERE x = @x', { x = 'v' }, function(value, err) delivered = { value, err } end)
    r.pump()
    call = r.calls[#r.calls]
    assert(delivered and delivered[2] == nil and call.method == 'value' and call.sql == 'SELECT COUNT(*) FROM t WHERE x = ?' and call.parameters[1] == 'v')
    -- prepare: one named set, and a list of named sets
    r.env.MySQL.prepare.await('INSERT INTO t (a) VALUES (@a)', { a = 9 })
    call = r.calls[#r.calls]
    assert(call.sql == 'INSERT INTO t (a) VALUES (?)' and call.parameters[1] == 9)
    local before = #r.calls
    r.env.MySQL.prepare.await('INSERT INTO t (a) VALUES (@a)', { { a = 1 }, { a = 2 } })
    assert(#r.calls == before + 2 and r.calls[before + 1].sql == 'INSERT INTO t (a) VALUES (?)' and r.calls[before + 2].parameters[1] == 2)
    -- transaction list and startTransaction's query()
    assert(r.env.MySQL.transaction.await({
        { query = 'UPDATE t SET n = @n WHERE id = @id', values = { id = 2, n = 1 } },
        { 'DELETE FROM t WHERE id = :id', { id = 3 } },
    }) == true)
    local last = r.calls[#r.calls]
    assert(r.calls[#r.calls - 1].sql == 'UPDATE t SET n = ? WHERE id = ?' and r.calls[#r.calls - 1].parameters[1] == 1 and r.calls[#r.calls - 1].parameters[2] == 2)
    assert(last.sql == 'DELETE FROM t WHERE id = ?' and last.parameters[1] == 3)
    r.env.MySQL.startTransaction(function(query)
        query('SELECT * FROM t WHERE id = @id', { id = 8 })
        return true
    end)
    last = r.calls[#r.calls]
    assert(last.method == 'tx.raw' and last.sql == 'SELECT * FROM t WHERE id = ?' and last.parameters[1] == 8)
end)

check('named parameters that do not match the statement are refused before anything is sent', function()
    local r = runtime(false)
    r.load()
    local before = #r.calls
    local err = failing(function() r.env.MySQL.query.await('DELETE FROM t WHERE id = @housid', { houseid = 5 }) end)
    assert(err.code == 'INVALID_ARGUMENT' and err.message:find('houseid', 1, true), tostring(err.message))
    err = failing(function() r.env.MySQL.query.await('SELECT ?', { id = 1 }) end)
    assert(err.code == 'INVALID_ARGUMENT')
    local failure
    r.env.MySQL.query('DELETE FROM t WHERE id = @housid', { houseid = 5 }, function(_, callbackError) failure = callbackError end)
    r.pump()
    assert(failure and failure.code == 'INVALID_ARGUMENT')
    assert(r.env.MySQL.transaction.await ~= nil)
    err = failing(function() r.env.MySQL.transaction.await({ { query = 'UPDATE t SET a = @b', values = { c = 1 } } }) end)
    assert(err.code == 'INVALID_ARGUMENT' and r.transactions == 0)
    assert(#r.calls == before, 'Nothing was dispatched for any of them')
end)

print(('Compatibility tests: %d passed'):format(passed))
