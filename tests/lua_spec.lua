-- Run from the resource directory: lua5.4 tests/lua_spec.lua
local tests = 0
local function check(name, fn)
    local thread = coroutine.create(fn)
    local ok, err = coroutine.resume(thread)
    assert(ok, name .. ': ' .. tostring(err))
    assert(coroutine.status(thread) == 'dead', name .. ': unexpected pending request')
    tests = tests + 1
    print('PASS ' .. name)
end

local registered, handlers, logs = {}, {}, {}
local state, caller, current = 'started', 'example-consumer', 'feather-mysql'
local response, held, delayed, bridgeCalls = nil, nil, false, 0
local lastRequest, failExport, resolutions = nil, false, 0
local selfCheckAnswer, lastBatch
local readyAnswer = function() return { ok = true, value = { ready = true } } end
local batchAnswer = function() return { ok = true, value = true } end
function GetConvar(_, default) return default end
function GetInvokingResource() return caller end
function GetCurrentResourceName() return current end
-- The provider reads its own lib/Named.lua at load.
function LoadResourceFile(_, path)
    local file = assert(io.open(path, 'rb'))
    local text = file:read('*a')
    file:close()
    return text
end
local states, provides = {}, {}
function GetResourceState(name) return states[name] or state end
function GetNumResourceMetadata(_, key) return key == 'provide' and #provides or 0 end
function GetResourceMetadata(_, key, index) return key == 'provide' and provides[index + 1] or nil end
local now = 123
function GetGameTimer() return now end
function IsDuplicityVersion() return true end
function AddEventHandler(name, fn) handlers[name] = fn end
-- Minimal Cfx scheduler: threads only run when a test pumps them.
local threads, timers = {}, {}
function CreateThread(fn) threads[#threads + 1] = coroutine.create(fn) end
function Wait() coroutine.yield() end
function SetTimeout(_, fn) timers[#timers + 1] = fn end
local function pump()
    local running = threads
    threads = {}
    for _, thread in ipairs(running) do
        local ok, err = coroutine.resume(thread)
        assert(ok, tostring(err))
        if coroutine.status(thread) ~= 'dead' then threads[#threads + 1] = thread end
    end
end
local function capturePrint(fn)
    local lines, real = {}, print
    print = function(...)
        local parts = {}
        for i = 1, select('#', ...) do parts[i] = tostring((select(i, ...))) end
        lines[#lines + 1] = table.concat(parts, ' ')
    end
    local ok, err = pcall(fn)
    print = real
    assert(ok, err)
    return lines
end
json = { encode = function(entry) logs[#logs + 1] = entry; return 'encoded' end }
exports = setmetatable({}, {
    __call = function(_, name, fn) registered[name] = fn end,
    __index = function()
        return setmetatable({}, { __index = function(_, name)
            return function(_, ...)
                if name == 'DriverSelfCheckV1' then
                    local callback = ...
                    if selfCheckAnswer then callback(selfCheckAnswer()) end
                elseif name == 'DriverReadyV1' then
                    local callback = ...
                    callback(readyAnswer())
                elseif name == 'DriverTransactionBatchV1' then
                    local owner, requests, callback = ...
                    lastBatch = { owner = owner, requests = requests }
                    callback(batchAnswer())
                elseif name == 'DriverExecuteV1' then
                    bridgeCalls = bridgeCalls + 1
                    local request, callback = ...
                    lastRequest = request
                    assert(type(request.sql) == 'string')
                    if delayed then held = callback else callback(response) end
                else
                    if failExport then error('Export unavailable') end
                    -- The callback is always the last argument. Match the callable-table
                    -- representation used by Cfx msgpack.
                    local args = table.pack(...)
                    local callback = args[args.n]
                    args[args.n] = setmetatable({ __cfx_functionReference = 'fixture' }, {
                        __call = function(_, ...) return callback(...) end,
                    })
                    return registered[name](table.unpack(args, 1, args.n))
                end
            end
        end })
    end,
})
promise = { new = function()
    return { resolve = function(self, value)
        resolutions = resolutions + 1
        self.value = value
        self.done = true
    end }
end }
Citizen = { Await = function(deferred)
    while not deferred.done do coroutine.yield() end
    return deferred.value
end }

for _, name in ipairs({ 'config', 'errors', 'validation', 'results', 'logger', 'main', 'compat' }) do
    dofile('server/' .. name .. '.lua')
end
dofile('lib/DB.lua')

-- The driver reports the first column by position as `first`.
local function rows(value) response = { ok = true, value = { kind = 'rows', rows = value, firstColumn = 'z', first = value[1] and value[1].z } } end
local function write(id, affected) response = { ok = true, value = { kind = 'write', header = { insertId = id, affectedRows = affected } } } end

check('five plain DB functions return the specified shapes', function()
    local methods = { 'query', 'one', 'value', 'insert', 'exec' }
    for _, method in ipairs(methods) do assert(type(DB[method]) == 'function') end
    local count = 0
    for _ in pairs(DB) do count = count + 1 end
    assert(count == 9 and type(DB.transaction) == 'function' and type(DB.raw) == 'function' and MySQL == nil)
    rows({ { z = 42, a = 1 }, { z = 43 } })
    assert(#DB.query('SELECT ? AS z', 42) == 2)
    assert(DB.one('SELECT ? AS z', 42).z == 42)
    assert(DB.value('SELECT ? AS z', 42) == 42)
    write(13, 2)
    assert(DB.insert('INSERT INTO x (n) VALUES (?)', 42) == 13)
    assert(DB.exec('UPDATE x SET n = ?', 42) == 2)
    write('9007199254740993', 1)
    assert(DB.insert('INSERT INTO x (n) VALUES (?)', 42) == '9007199254740993')
end)
check('varargs preserve order and values without SQL interpolation', function()
    rows({})
    local sql, identifier = 'SELECT ?, ?, ?, ?, ?', "O'Brien ?; DROP TABLE imaginary; --"
    DB.query(sql, identifier, 'character-uuid', false, 0, '')
    assert(lastRequest.sql == sql and lastRequest.count == 5)
    local expected = { identifier, 'character-uuid', false, 0, '' }
    for i = 1, 5 do assert(lastRequest.parameters[i].value == expected[i]) end
    DB.query('SELECT 1')
    assert(lastRequest.count == 0 and next(lastRequest.parameters) == nil)
end)
check('nil arguments bind NULL at the first, middle and final positions', function()
    rows({})
    DB.query('SELECT ?, ?, ?, ?, ?', nil, false, nil, 'value', nil)
    assert(lastRequest.count == 5)
    for _, i in ipairs({ 1, 3, 5 }) do assert(lastRequest.parameters[i].isNull) end
    assert(lastRequest.parameters[2].value == false and lastRequest.parameters[4].value == 'value')
    DB.query('SELECT ?', nil)
    assert(lastRequest.count == 1 and lastRequest.parameters[1].isNull)
    DB.query('SELECT ?, ?', nil, nil)
    assert(lastRequest.count == 2 and lastRequest.parameters[1].isNull and lastRequest.parameters[2].isNull)
end)
check('empty results and NULL/false/zero/empty-string values remain distinct', function()
    rows({})
    assert(#DB.query('SELECT 1') == 0)
    assert(DB.one('SELECT 1') == nil)
    assert(DB.value('SELECT 1') == nil)
    rows({ { a = 99 } }); assert(DB.value('SELECT NULL AS z, 99 AS a') == nil)
    rows({ { z = false, a = 99 } }); assert(DB.value('SELECT false') == false)
    rows({ { z = 0, a = 99 } }); assert(DB.value('SELECT 0') == 0)
    rows({ { z = '', a = 99 } }); assert(DB.value("SELECT ''") == '')
    write(0, 0)
    assert(DB.exec('CREATE TABLE x (n INT)') == 0)
    assert(DB.insert('INSERT INTO x (n) VALUES (?)', 42) == 0)
end)
check('tables, callbacks and unsafe numbers are rejected without driver calls', function()
    for _, parameter in ipairs({ { 42 }, { named = 1 }, 0/0, math.huge, 9007199254740992, {}, function() end }) do
        local before = bridgeCalls
        local ok, err = pcall(DB.query, 'SELECT ?', parameter)
        assert(not ok and err.code == 'INVALID_ARGUMENT' and bridgeCalls == before)
    end
end)
check('invalid SQL arguments and old internal method names are rejected', function()
    for _, sql in ipairs({ '', '   ', false, {} }) do
        local before = bridgeCalls
        local ok, err = pcall(DB.query, sql)
        assert(not ok and err.code == 'INVALID_ARGUMENT' and bridgeCalls == before)
    end
    local ok, err = pcall(DB.query, nil)
    assert(not ok and err.code == 'INVALID_ARGUMENT')
    for _, method in ipairs({ 'single', 'scalar', 'update' }) do
        assert(FeatherMySQL.validate(method, 'SELECT 1') == nil)
    end
end)
check('errors retain safe details, caller and public method name', function()
    response = { ok = false, error = { code = 'DATABASE_ERROR', message = 'safe', driverCode = 'ER_PARSE_ERROR' } }
    for _, method in ipairs({ 'query', 'one', 'value', 'insert', 'exec' }) do
        local ok, err = pcall(DB[method], 'broken')
        assert(not ok and err.code == 'DATABASE_ERROR' and err.message == 'safe')
        assert(err.resource == 'example-consumer' and err.method == method and err.queryId)
        assert(err.driverCode == 'ER_PARSE_ERROR')
    end
end)
check('query rejects write headers and write methods reject rows', function()
    write(5, 1)
    for _, method in ipairs({ 'query', 'one', 'value' }) do
        local ok, err = pcall(DB[method], 'UPDATE x SET n = 1')
        assert(not ok and err.code == 'RESULT_TYPE')
        assert(err.message:find('already have executed', 1, true))
    end
    rows({ { z = 1 } })
    for _, method in ipairs({ 'insert', 'exec' }) do
        local ok, err = pcall(DB[method], 'SELECT 1')
        assert(not ok and err.code == 'RESULT_TYPE')
    end
end)
-- Deliberately call outside a coroutine: reject before any database work.
local beforeContext = bridgeCalls
local contextOk, contextError = pcall(DB.query, 'SELECT 1')
-- Also from the main thread: readiness needs to wait, so it needs a coroutine too.
local readyOk, readyError = pcall(DB.isReady)
local awaitOk, awaitError = pcall(DB.awaitReady, 10)
check('readiness helpers need a coroutine to wait in', function()
    assert(not readyOk and readyError.code == 'INVALID_CONTEXT')
    assert(not awaitOk and awaitError.code == 'INVALID_CONTEXT')
end)
check('nonyieldable callers fail before dispatch', function()
    assert(not contextOk and contextError.code == 'INVALID_CONTEXT' and bridgeCalls == beforeContext)
end)
check('unavailable resources and exports fail without driver calls', function()
    local before = bridgeCalls
    state = 'stopped'
    local ok, err = pcall(DB.query, 'SELECT 1')
    assert(not ok and err.code == 'UNAVAILABLE' and bridgeCalls == before)
    state, failExport = 'started', true
    ok, err = pcall(DB.query, 'SELECT 1')
    assert(not ok and err.code == 'UNAVAILABLE' and bridgeCalls == before)
    failExport = false
end)
check('DB calls yield, resume with results and retain attribution in logs', function()
    delayed = true
    FeatherMySQL.config.logQueries = true
    local result
    local co = coroutine.create(function() result = DB.value('SELECT ? AS z', 7) end)
    assert(coroutine.resume(co))
    assert(coroutine.status(co) == 'suspended' and result == nil)
    caller = 'another-consumer'
    held({ ok = true, value = { kind = 'rows', rows = { { z = 7 } }, firstColumn = 'z', first = 7 } })
    assert(coroutine.resume(co))
    assert(result == 7 and coroutine.status(co) == 'dead')
    assert(logs[#logs].resource == 'example-consumer' and logs[#logs].method == 'value')
    caller, delayed = 'example-consumer', false
    FeatherMySQL.config.logQueries = false
end)
check('provider stop settles once, ignores late results and permits calls after restart', function()
    delayed = true
    local ok, err
    local co = coroutine.create(function() ok, err = pcall(DB.query, 'SELECT 1') end)
    assert(coroutine.resume(co))
    handlers.onResourceStop('unrelated-resource')
    assert(coroutine.status(co) == 'suspended')
    state = 'stopped'
    handlers.onResourceStop('feather-mysql')
    local afterStop = resolutions
    assert(coroutine.resume(co))
    assert(not ok and err.code == 'RESOURCE_STOPPED')
    held({ ok = true, value = { kind = 'rows', rows = {} } })
    assert(resolutions == afterStop and coroutine.status(co) == 'dead')
    delayed, state = false, 'started'
    rows({ { z = 9 } })
    assert(DB.value('SELECT 9') == 9)
end)
check('slow and query logging omit values and SQL unless explicitly enabled', function()
    logs = {}
    FeatherMySQL.config.logQueries = false
    FeatherMySQL.config.logSql = false
    FeatherMySQL.config.slowMs = 200
    local context = { resource = 'consumer', method = 'query', id = 1, sql = 'SELECT secret' }
    FeatherMySQL.log(context, 199)
    assert(#logs == 0)
    FeatherMySQL.log(context, 200)
    assert(#logs == 1 and logs[1].slow and logs[1].sql == nil)
    FeatherMySQL.config.logQueries = true
    FeatherMySQL.log(context, 1)
    assert(#logs == 2 and logs[2].sql == nil)
    FeatherMySQL.config.logSql = true
    FeatherMySQL.log(context, 1)
    assert(logs[3].sql == context.sql)
end)
check('value reads the first column by position even when names repeat', function()
    -- SELECT a.id, b.id: the row object keeps one `id`, but the driver reports the first value.
    response = { ok = true, value = { kind = 'rows', rows = { { id = 2 } }, firstColumn = 'id', first = 1 } }
    assert(DB.value('SELECT a.id, b.id FROM a JOIN b') == 1)
    response = { ok = true, value = { kind = 'rows', rows = { { z = 5 } }, firstColumn = 'z', first = 0 } }
    assert(DB.value('SELECT 0') == 0)
end)
check('integers must fit in 2^53 on both sides; floats only need to be finite', function()
    local accepted = { 9007199254740991, -9007199254740991, 0.1, 1e20, -1e300, 0, -0.0 }
    local rejected = { math.mininteger, math.maxinteger, 9007199254740992, -9007199254740992, 0 / 0, math.huge, -math.huge }
    rows({})
    for _, value in ipairs(accepted) do
        local before = bridgeCalls
        DB.query('SELECT ?', value)
        assert(bridgeCalls == before + 1, tostring(value))
    end
    for _, value in ipairs(rejected) do
        local before = bridgeCalls
        local ok, err = pcall(DB.query, 'SELECT ?', value)
        assert(not ok and err.code == 'INVALID_ARGUMENT' and bridgeCalls == before, tostring(value))
    end
end)
check('errors say what may be assumed about the database', function()
    local expected = { INVALID_ARGUMENT = 'not_executed', RESULT_TYPE = 'executed', UNAVAILABLE = 'not_executed' }
    local _, invalid = pcall(DB.query, '')
    assert(invalid.outcome == expected.INVALID_ARGUMENT)
    write(1, 1)
    local _, mismatch = pcall(DB.query, 'UPDATE x SET n = 1')
    assert(mismatch.code == 'RESULT_TYPE' and mismatch.outcome == expected.RESULT_TYPE)
    state = 'stopped'
    local _, unavailable = pcall(DB.query, 'SELECT 1')
    state = 'started'
    assert(unavailable.code == 'UNAVAILABLE' and unavailable.outcome == expected.UNAVAILABLE)
    response = { ok = false, error = { code = 'DATABASE_ERROR', message = 'Database rejected the statement',
        driverCode = 'ER_DUP_ENTRY', sqlState = '23000', outcome = 'failed', detail = 'Duplicate entry' } }
    local _, rejected = pcall(DB.exec, 'INSERT INTO x VALUES (1)')
    assert(rejected.outcome == 'failed' and rejected.sqlState == '23000' and rejected.detail == 'Duplicate entry')
    assert(rejected.driverCode == 'ER_DUP_ENTRY' and rejected.resource == 'example-consumer')
    response = { ok = false, error = { code = 'DATABASE_ERROR', message = 'x', outcome = 42, sqlState = {}, detail = false } }
    local _, sanitized = pcall(DB.exec, 'INSERT INTO x VALUES (1)')
    assert(sanitized.outcome == nil and sanitized.sqlState == nil and sanitized.detail == nil, 'Untyped fields are dropped')
end)
check('raised errors print readably and record where they were raised', function()
    write(0, 0)
    response = { ok = false, error = { code = 'DATABASE_ERROR', message = 'Database rejected the statement',
        driverCode = 'ER_DUP_ENTRY', sqlState = '23000', outcome = 'failed', resource = 'example-consumer', method = 'exec', queryId = 9 } }
    local ok, err = pcall(DB.exec, 'INSERT INTO x VALUES (1)')
    assert(not ok and type(err) == 'table' and err.code == 'DATABASE_ERROR', 'Callers can still branch on err.code')
    local text = tostring(err)
    assert(text:find('DATABASE_ERROR', 1, true) and text:find('Database rejected the statement', 1, true))
    assert(text:find('ER_DUP_ENTRY', 1, true) and text:find('23000', 1, true) and text:find('outcome=failed', 1, true))
    assert(not text:find('table: 0x', 1, true))
    assert(type(err.traceback) == 'string' and err.traceback:find('stack traceback', 1, true), 'The call site is recorded')
    assert(text:find(err.traceback, 1, true))
end)
check('a reply that never arrives is turned into an error by the watchdog, and late replies are ignored', function()
    delayed = true
    local ok, err
    local co = coroutine.create(function() ok, err = pcall(DB.value, 'SELECT 1') end)
    assert(coroutine.resume(co) and coroutine.status(co) == 'suspended')
    pump()
    assert(#threads == 1, 'One sweeper watches outstanding requests')
    now = now + 34999; pump()
    assert(coroutine.status(co) == 'suspended', 'Not before the longest provider deadline plus its margin')
    now = now + 2; pump()
    assert(coroutine.status(co) == 'suspended' or coroutine.status(co) == 'dead')
    assert(coroutine.resume(co))
    assert(not ok and err.code == 'WATCHDOG_TIMEOUT' and err.outcome == 'unknown' and err.resource == GetCurrentResourceName())
    local settled = resolutions
    held({ ok = true, value = { kind = 'rows', rows = {}, first = 1 } })
    assert(resolutions == settled, 'A late reply cannot settle the request twice')
    pump()
    assert(#threads == 0, 'The sweeper ends when nothing is outstanding')
    delayed = false
    rows({ { z = 3 } })
    assert(DB.value('SELECT 3') == 3, 'Later calls still work')
    pump()
end)
check('error logging is rate limited during an outage and reports what was suppressed', function()
    logs = {}
    FeatherMySQL.config.logQueries, FeatherMySQL.config.slowMs, FeatherMySQL.config.errorLogLimit = false, 0, 3
    local context = { resource = 'consumer', method = 'query', id = 1 }
    local lines = capturePrint(function()
        now = 10000
        for _ = 1, 10 do FeatherMySQL.log(context, 1, { code = 'DATABASE_ERROR', outcome = 'unknown' }) end
        assert(#logs == 3, 'Only three lines in the first second')
        now = 11000
        FeatherMySQL.log(context, 1, { code = 'DATABASE_ERROR', outcome = 'unknown', sqlState = '08S01' })
    end)
    assert(#logs == 4 and logs[4].sqlState == '08S01' and logs[4].outcome == 'unknown')
    local summary = false
    for _, line in ipairs(lines) do if line:find('7 error log lines suppressed', 1, true) then summary = true end end
    assert(summary, 'The next window says how many lines were dropped')
    FeatherMySQL.config.errorLogLimit = 0
    logs = {}
    capturePrint(function() for _ = 1, 30 do FeatherMySQL.log(context, 1, { code = 'X' }) end end)
    assert(#logs == 30, 'A limit of 0 disables the cap')
    FeatherMySQL.config.errorLogLimit = 20
    FeatherMySQL.config.slowMs = 200
end)
check('the driver detail reaches the log only when it was supplied', function()
    logs = {}
    FeatherMySQL.config.errorLogLimit = 0
    local context = { resource = 'consumer', method = 'query', id = 1 }
    capturePrint(function()
        FeatherMySQL.log(context, 1, { code = 'DATABASE_ERROR' })
        FeatherMySQL.log(context, 1, { code = 'DATABASE_ERROR', detail = 'operator opted in' })
    end)
    assert(logs[1].detail == nil and logs[2].detail == 'operator opted in')
    FeatherMySQL.config.errorLogLimit = 20
end)
local sample = function()
    return { ok = true, value = { kind = 'rows', firstColumn = 'text', first = 'h\u{e9}llo \u{1F600} \u{65e5}\u{672c}\u{8a9e}',
        rows = { { text = 'h\u{e9}llo \u{1F600} \u{65e5}\u{672c}\u{8a9e}', number = 42, bytes = { 0, 255 }, big = '9007199254740993', flag = false } } } }
end
check('the startup self-check reports success only when every value shape round-trips', function()
    selfCheckAnswer = sample
    local lines = capturePrint(function() handlers.onResourceStart('feather-mysql') end)
    assert(#lines == 1 and lines[1]:find('Bridge self-check OK', 1, true), lines[1])
    assert(#timers >= 1, 'A no-response watchdog is armed')
    lines = capturePrint(function() handlers.onResourceStart('another-resource') end)
    assert(#lines == 0, 'Other resources starting is not our business')
    local broken = {
        function() local a = sample(); a.value.rows[1].text = 'h?llo'; return a end,
        function() local a = sample(); a.value.rows[1].bytes = { 0, 254 }; return a end,
        function() local a = sample(); a.value.rows[1].flag = 0; return a end,
        function() local a = sample(); a.value.rows[1].nothing = 'not nil'; return a end,
        function() return { ok = false, error = { code = 'INVALID_CALLER', message = 'Only this resource may call the internal database bridge' } } end,
        function() return 'garbage' end,
    }
    for index, variant in ipairs(broken) do
        selfCheckAnswer = variant
        lines = capturePrint(function() handlers.onResourceStart('feather-mysql') end)
        assert(#lines == 1 and lines[1]:find('Bridge self-check FAILED', 1, true), index .. ': ' .. tostring(lines[1]))
    end
    selfCheckAnswer = function() return { ok = false, error = { code = 'INVALID_CALLER', message = 'Only this resource may call the internal database bridge' } } end
    lines = capturePrint(function() handlers.onResourceStart('feather-mysql') end)
    assert(lines[1]:find('INVALID_CALLER', 1, true), 'The refusal reason is shown')
end)
check('a self-check that gets no answer says so after five seconds', function()
    selfCheckAnswer = nil
    timers = {}
    local lines = capturePrint(function()
        handlers.onResourceStart('feather-mysql')
        for _, fn in ipairs(timers) do fn() end
    end)
    assert(#lines == 1 and lines[1]:find('no response within 5 seconds', 1, true), tostring(lines[1]))
    failExport = true
    lines = capturePrint(function() handlers.onResourceStart('feather-mysql') end)
    failExport = false
    assert(#lines == 0 or lines[1] ~= nil)
end)
check('raw returns rows for row results and a write header for everything else', function()
    rows({ { z = 1 }, { z = 2 } })
    local result = DB.raw('SELECT z FROM t')
    assert(#result == 2 and result[1].z == 1)
    rows({})
    assert(#DB.raw('SELECT 1 WHERE 0') == 0, 'An empty result is an empty array, not a header')
    response = { ok = true, value = { kind = 'write', header = { insertId = 7, affectedRows = 3, warningStatus = 1, extra = 'dropped' } } }
    local header = DB.raw('UPDATE t SET n = ?', 1)
    assert(header.affectedRows == 3 and header.insertId == 7 and header.warningStatus == 1 and header.extra == nil)
    assert(FeatherMySQL.validate('raw', 'SELECT 1') ~= nil and FeatherMySQL.validate('other', 'SELECT 1') == nil)
    assert(lastRequest.parameters[1].value == 1)
end)
check('readiness is a plain boolean and never an error while the provider is down', function()
    readyAnswer = function() return { ok = true, value = { ready = true } } end
    assert(DB.isReady() == true)
    readyAnswer = function() return { ok = true, value = { ready = false, code = 'ECONNREFUSED' } } end
    assert(DB.isReady() == false)
    readyAnswer = function() return { ok = false, error = { code = 'CONFIG_ERROR', message = 'Invalid database configuration' } } end
    assert(DB.isReady() == false)
    state = 'stopped'
    assert(DB.isReady() == false)
    state = 'started'
    readyAnswer = function() return { ok = true, value = { ready = true } } end
    local ok, err = pcall(DB.isReady)
    assert(ok and err == true)
end)
check('awaitReady waits, then returns true; or false when the timeout passes first', function()
    readyAnswer = function() return { ok = true, value = { ready = false } } end
    local result
    local co = coroutine.create(function() result = DB.awaitReady(1000) end)
    assert(coroutine.resume(co) and coroutine.status(co) == 'suspended')
    readyAnswer = function() return { ok = true, value = { ready = true } } end
    assert(coroutine.resume(co))
    assert(result == true and coroutine.status(co) == 'dead')
    readyAnswer = function() return { ok = true, value = { ready = false } } end
    local timedOut
    co = coroutine.create(function() timedOut = DB.awaitReady(1000) end)
    assert(coroutine.resume(co) and coroutine.status(co) == 'suspended')
    now = now + 1500
    assert(coroutine.resume(co))
    assert(timedOut == false and coroutine.status(co) == 'dead')
    readyAnswer = function() return { ok = true, value = { ready = true } } end
end)
check('oxmysql-named exports return results to a callback or to the waiting caller', function()
    caller = 'compat-caller'
    local names = { query = 'raw', single = 'one', scalar = 'value', insert = 'insert', update = 'exec' }
    for export in pairs(names) do assert(type(registered[export]) == 'function', export) end
    rows({ { z = 5 } })
    local got
    registered.query('SELECT z FROM t WHERE id = ?', { 9 }, function(result) got = result end)
    assert(got and got[1].z == 5 and lastRequest.resource == 'compat-caller' and lastRequest.parameters[1].value == 9)
    got = nil
    assert(registered.single('SELECT z FROM t', {}).z == 5, 'Without a callback the caller waits and gets the result')
    assert(registered.scalar('SELECT z FROM t') == 5)
    write(42, 1)
    assert(registered.insert('INSERT INTO t VALUES (?)', { 1 }) == 42)
    assert(registered.update('UPDATE t SET n = 1') == 1)
    registered.update('UPDATE t SET n = 1', nil, function(affected) got = affected end)
    assert(got == 1)
    write(0, 0)
    local header = registered.query('UPDATE t SET n = 1')
    assert(header.affectedRows == 0 and header.insertId == 0, 'A write through query returns a header')
    caller = 'example-consumer'
end)
-- Keep only the lines a compat export printed (the provider's own error log is captured too).
local function lines_with(lines, text)
    local kept = {}
    for _, line in ipairs(lines) do if line:find(text, 1, true) then kept[#kept + 1] = line end end
    return kept
end
check('oxmysql-named exports log a failure and answer nil instead of raising', function()
    caller = 'compat-caller'
    response = { ok = false, error = { code = 'DATABASE_ERROR', message = 'Database rejected the statement', driverCode = 'ER_DUP_ENTRY' } }
    local got, called = 'unset', false
    local lines = capturePrint(function()
        assert(registered.query('INSERT INTO t VALUES (1)') == nil)
        registered.update('INSERT INTO t VALUES (1)', {}, function(result) got, called = result, true end)
        local ok, err = pcall(registered.query, 'SELECT ?', { {} })
        assert(ok and err == nil, 'Invalid parameters do not raise either')
    end)
    assert(called and got == nil)
    local reports = lines_with(lines, 'failed for compat-caller')
    assert(#reports == 3, #reports)
    assert(reports[1]:find('query failed for compat-caller', 1, true) and reports[1]:find('DATABASE_ERROR', 1, true), reports[1])
    assert(reports[2]:find('update failed for compat-caller', 1, true))
    assert(reports[3]:find('INVALID_ARGUMENT', 1, true), reports[3])
    caller = 'example-consumer'
end)
check('the transaction export accepts the three query forms and reports true only when committed', function()
    caller = 'compat-caller'
    batchAnswer = function() return { ok = true, value = true } end
    local committed = registered.transaction({
        'UPDATE a SET n = 1',
        { query = 'INSERT INTO b VALUES (?, ?)', values = { "O'Brien", false } },
        { 'DELETE FROM c WHERE id = ?', { 7 } },
    })
    assert(committed == true and lastBatch.owner == 'compat-caller' and #lastBatch.requests == 3)
    assert(lastBatch.requests[1].sql == 'UPDATE a SET n = 1' and lastBatch.requests[1].count == 0)
    assert(lastBatch.requests[2].count == 2 and lastBatch.requests[2].parameters[1].value == "O'Brien" and lastBatch.requests[2].parameters[2].value == false)
    assert(lastBatch.requests[3].parameters[1].value == 7)
    local viaCallback
    registered.transaction({ 'UPDATE a SET n = 2' }, function(result) viaCallback = result end)
    assert(viaCallback == true)
    batchAnswer = function() return { ok = false, error = { code = 'DATABASE_ERROR', message = 'Database rejected the statement' } } end
    local before = lastBatch
    local lines = capturePrint(function() assert(registered.transaction({ 'UPDATE a SET n = 3' }) == false) end)
    assert(#lines_with(lines, 'transaction failed for compat-caller') == 1)
    batchAnswer = function() return { ok = true, value = true } end
    lastBatch = nil
    capturePrint(function()
        assert(registered.transaction('not a list') == false)
        assert(registered.transaction({}) == false)
        assert(registered.transaction({ { values = {} } }) == false)
        assert(registered.transaction({ 'SELECT ?', { 1, 2 } }) == false, 'A parameter list where SQL belongs is not SQL')
        assert(registered.transaction({ { 'UPDATE a SET n = ?', { {} } } }) == false, 'Invalid parameters fail before any transaction starts')
    end)
    assert(lastBatch == nil, 'Nothing reaches the driver for an invalid list')
    caller = 'example-consumer'
end)
check('the isReady export answers a plain boolean', function()
    readyAnswer = function() return { ok = true, value = { ready = true } } end
    assert(registered.isReady() == true)
    readyAnswer = function() return { ok = true, value = { ready = false } } end
    assert(registered.isReady() == false)
    local got
    registered.isReady(function(ready) got = ready end)
    assert(got == false)
    readyAnswer = function() return { ok = true, value = { ready = true } } end
end)
check('the provider says when it stands in for oxmysql, and warns if a real one is also running', function()
    selfCheckAnswer = sample
    local function start() return capturePrint(function() handlers.onResourceStart('feather-mysql') end) end
    provides = {}
    assert(#start() == 1, 'Nothing extra is printed when oxmysql is not provided')
    provides, states.oxmysql = { 'oxmysql' }, 'missing'
    local lines = start()
    assert(#lines == 2 and lines[2]:find('Providing "oxmysql"', 1, true), lines[2])
    states.oxmysql = 'started'
    lines = start()
    assert(#lines == 2 and lines[2]:find('WARNING', 1, true) and lines[2]:find('is running', 1, true), lines[2])
    provides, states.oxmysql = { 'something-else' }, nil
    assert(#start() == 1)
    provides = {}
end)
check('the exports accept named placeholders, and refuse a mismatch without sending anything', function()
    caller = 'compat-caller'
    rows({ { z = 1 } })
    registered.query('DELETE FROM bcchousing WHERE houseid = @houseid', { houseid = 5 }, function() end)
    assert(lastRequest.sql == 'DELETE FROM bcchousing WHERE houseid = ?' and lastRequest.count == 1 and lastRequest.parameters[1].value == 5)
    write(0, 1)
    assert(registered.update('UPDATE t SET a = @a WHERE id = :id AND b = @a', { a = 'x', id = 4 }) == 1)
    assert(lastRequest.sql == 'UPDATE t SET a = ? WHERE id = ? AND b = ?' and lastRequest.count == 3)
    assert(lastRequest.parameters[1].value == 'x' and lastRequest.parameters[2].value == 4 and lastRequest.parameters[3].value == 'x')
    rows({ { z = 3 } })
    assert(registered.scalar('SELECT z FROM t WHERE q = @q', { ['@q'] = false }) == 3 and lastRequest.parameters[1].value == false)
    local before = bridgeCalls
    local lines = capturePrint(function()
        assert(registered.query('DELETE FROM t WHERE id = @housid', { houseid = 5 }) == nil)
        assert(registered.update('SELECT ?', { id = 1 }) == nil)
    end)
    assert(bridgeCalls == before, 'Nothing reached the driver')
    assert(#lines_with(lines, 'INVALID_ARGUMENT') == 2, table.concat(lines, ' | '))
    batchAnswer = function() return { ok = true, value = true } end
    lastBatch = nil
    assert(registered.transaction({ { query = 'UPDATE t SET n = @n WHERE id = @id', values = { id = 2, n = 1 } } }) == true)
    assert(lastBatch.requests[1].sql == 'UPDATE t SET n = ? WHERE id = ?' and lastBatch.requests[1].parameters[1].value == 1 and lastBatch.requests[1].parameters[2].value == 2)
    lastBatch = nil
    capturePrint(function() assert(registered.transaction({ { query = 'UPDATE t SET n = @m', values = { n = 1 } } }) == false) end)
    assert(lastBatch == nil, 'A mismatched list never starts a transaction')
    caller = 'example-consumer'
end)
check('devmode turns the log switches on by default and an explicit convar still wins', function()
    local function configWith(convars)
        local env = setmetatable({ GetConvar = function(name, default) return convars[name] or default end }, { __index = _G })
        assert(loadfile('server/config.lua', 't', env))()
        return env.FeatherMySQL.config
    end
    local quiet = configWith({})
    assert(not quiet.devMode and not quiet.logQueries and not quiet.logSql)
    local dev = configWith({ feather_mysql_devmode = 'true' })
    assert(dev.devMode and dev.logQueries and dev.logSql)
    local mixed = configWith({ feather_mysql_devmode = 'true', feather_mysql_log_sql = 'false' })
    assert(mixed.logQueries and not mixed.logSql, 'An explicit false wins over devmode')
    assert(configWith({ feather_mysql_log_queries = 'true' }).logQueries, 'The switches still work without devmode')
end)
assert(DB.prepare == nil and type(DB.transaction) == 'function' and DB.startTransaction == nil)
print(('Lua contract tests: %d passed'):format(tests))
