-- Contract tests for the Lua transaction lifecycle. Driver behavior is simulated;
-- real connection affinity/rollback is tested separately against MariaDB.
local tests, registered, handlers = 0, {}, {}
local caller, state = 'transaction-consumer', 'started'
local history, transactions, sequence = {}, {}, 0
local failFinish, failBegin, holdQuery, held, malformed = nil, nil, false, nil, nil
local lastRequest, lastOwner, resolutions = nil, nil, 0
function GetConvar(_, default) return default end
function GetCurrentResourceName() return 'feather-mysql' end
function GetInvokingResource() return caller end
function GetResourceState() return state end
function GetGameTimer() return 10 end
-- Minimal Cfx scheduler: the watchdog sweeper only runs when a test pumps it.
local threads = {}
function CreateThread(fn) threads[#threads + 1] = coroutine.create(fn) end
function Wait() coroutine.yield() end
function SetTimeout() end
local function capturePrint(fn)
    local lines, real = {}, print
    print = function(...)
        local parts = {}
        for i = 1, select('#', ...) do parts[i] = tostring((select(i, ...))) end
        lines[#lines + 1] = table.concat(parts, ' ')
    end
    local ok, failure = pcall(fn)
    print = real
    assert(ok, failure)
    return lines
end
function IsDuplicityVersion() return true end
function AddEventHandler(name, fn)
    handlers[name] = handlers[name] or {}
    handlers[name][#handlers[name] + 1] = fn
end
local function stopped(resource)
    for _, fn in ipairs(handlers.onResourceStop or {}) do fn(resource) end
end
json = { encode = function() return 'test-log' end }
local function err(code)
    return { code = code, message = code, driverCode = code == 'DATABASE_ERROR' and 'ER_PARSE_ERROR' or nil }
end
local function response(value) return { ok = true, value = value } end
local function rejected(code) return { ok = false, error = err(code) } end
local function result(request, tx)
    lastRequest = request
    if request.sql == 'FAIL' then
        if tx then tx.failure = 'DATABASE_ERROR' end
        return rejected('DATABASE_ERROR')
    elseif request.sql == 'DUPLICATE' then
        if tx then tx.failure = 'DATABASE_ERROR' end
        return { ok = false, error = { code = 'DATABASE_ERROR', message = 'Duplicate', driverCode = 'ER_DUP_ENTRY' } }
    elseif request.sql:find('SELECT', 1, true) then
        return response({ kind = 'rows', firstColumn = 'z', first = tx and tx.id or 'pool', rows = { { z = tx and tx.id or 'pool' }, { z = 2 } } })
    end
    return response({ kind = 'write', header = { insertId = '9007199254740993', affectedRows = 1 } })
end
exports = setmetatable({}, {
    __call = function(_, name, fn) registered[name] = fn end,
    __index = function()
        return setmetatable({}, { __index = function(_, name)
            return function(_, ...)
                local args = table.pack(...)
                local callback = args[args.n]
                if name:sub(1, 6) ~= 'Driver' then
                    args[args.n] = setmetatable({}, { __call = function(_, ...) return callback(...) end })
                    return registered[name](table.unpack(args, 1, args.n))
                end
                history[#history + 1] = name
                if malformed then local value = malformed; malformed = nil; callback(value); return end
                if name == 'DriverExecuteV1' then
                    lastOwner = args[1].resource
                    if holdQuery then held = callback else callback(result(args[1])) end
                elseif name == 'DriverTransactionBeginV1' then
                    lastOwner = args[1]
                    if failBegin then callback(rejected(failBegin)); return end
                    sequence = sequence + 1
                    local id = 'tx-' .. sequence
                    transactions[id] = { id = id, owner = args[1], closed = false }
                    callback(response({ id = id }))
                elseif name == 'DriverTransactionQueryV1' then
                    local owner, id, request = table.unpack(args)
                    lastOwner = owner
                    local tx = transactions[id]
                    if tx.owner ~= owner then callback(rejected('TRANSACTION_OWNER'))
                    elseif tx.failure then callback(rejected(tx.failure))
                    elseif tx.closed then callback(rejected('TRANSACTION_CLOSED'))
                    elseif holdQuery then held = callback
                    else callback(result(request, tx)) end
                elseif name == 'DriverTransactionFinishV1' then
                    local owner, id, commit = table.unpack(args)
                    local tx = transactions[id]
                    lastOwner = owner
                    tx.closed = true
                    tx.commit = commit
                    if tx.owner ~= owner then callback(rejected('TRANSACTION_OWNER'))
                    elseif failFinish then callback(rejected(failFinish))
                    elseif tx.failure then callback(rejected(tx.failure))
                    else callback(response(commit)) end
                else error('Unknown bridge export ' .. name) end
            end
        end })
    end,
})
promise = { new = function()
    return { resolve = function(self, value) self.value, self.done = value, true; resolutions = resolutions + 1 end }
end }
Citizen = { Await = function(deferred)
    while not deferred.done do coroutine.yield() end
    return deferred.value
end }
for _, name in ipairs({ 'config', 'errors', 'validation', 'results', 'logger', 'main' }) do dofile('server/' .. name .. '.lua') end
dofile('lib/DB.lua')
local function check(name, fn)
    local co = coroutine.create(fn)
    local ok, failure = coroutine.resume(co)
    assert(ok, name .. ': ' .. (type(failure) == 'table' and failure.code or tostring(failure)))
    assert(coroutine.status(co) == 'dead', name .. ': unexpected pending request')
    tests = tests + 1
    print('PASS ' .. name)
end
local function expect(code, fn)
    local ok, failure = pcall(fn)
    assert(not ok and type(failure) == 'table' and failure.code == code,
        'expected ' .. code .. ', got ' .. (type(failure) == 'table' and failure.code or tostring(failure)))
    return failure
end
local function latest() return transactions['tx-' .. sequence] end

check('transaction true commits and all five explicit tx methods preserve shapes', function()
    assert(DB.transaction(function(tx)
        for _, method in ipairs({ 'query', 'one', 'value', 'insert', 'exec' }) do assert(type(tx[method]) == 'function') end
        assert(#tx.query('SELECT 1') == 2)
        assert(tx.one('SELECT 1').z == latest().id)
        assert(tx.value('SELECT 1') == latest().id)
        assert(tx.insert('INSERT X') == '9007199254740993')
        assert(tx.exec('UPDATE X') == 1)
        return true
    end))
    assert(latest().closed and latest().commit)
end)
check('false and nil intentionally roll back', function()
    assert(DB.transaction(function() return false end) == false)
    assert(latest().closed and not latest().commit)
    assert(DB.transaction(function() end) == false)
    assert(latest().closed and not latest().commit)
end)
check('parameters preserve special characters, NULL, false, zero and empty strings', function()
    DB.transaction(function(tx)
        tx.query('SELECT ?, ?, ?, ?, ?', "O'Brien ? --", nil, false, 0, '')
        assert(lastRequest.count == 5 and lastRequest.parameters[1].value == "O'Brien ? --")
        assert(lastRequest.parameters[2].isNull and lastRequest.parameters[3].value == false)
        assert(lastRequest.parameters[4].value == 0 and lastRequest.parameters[5].value == '')
        tx.query('SELECT ?', nil)
        assert(lastRequest.count == 1 and lastRequest.parameters[1].isNull)
        return true
    end)
end)
check('SQL failure rolls back and retains code and driver detail', function()
    local failure = expect('DATABASE_ERROR', function()
        DB.transaction(function(tx) tx.query('FAIL'); return true end)
    end)
    assert(failure.driverCode == 'ER_PARSE_ERROR' and latest().closed and not latest().commit)
end)
check('caught SQL failure poisons transaction and prevents later queries and commit', function()
    expect('DATABASE_ERROR', function()
        DB.transaction(function(tx)
            expect('DATABASE_ERROR', function() tx.query('FAIL') end)
            local count = #history
            expect('DATABASE_ERROR', function() tx.value('SELECT 1') end)
            assert(#history == count)
            return true
        end)
    end)
    assert(latest().closed and not latest().commit)
end)
check('duplicate-key failure retains ER_DUP_ENTRY', function()
    local failure = expect('DATABASE_ERROR', function() DB.transaction(function(tx) tx.exec('DUPLICATE') end) end)
    assert(failure.driverCode == 'ER_DUP_ENTRY' and latest().closed)
end)
check('Lua callback errors roll back and clear coroutine nesting guard', function()
    local failure = expect('LUA_ERROR', function() DB.transaction(function() error('callback exploded') end) end)
    assert(failure.message:find('callback exploded', 1, true) and latest().closed and not latest().commit)
    assert(DB.transaction(function() return true end))
end)
check('non-string Lua callback errors remain structured', function()
    expect('LUA_ERROR', function() DB.transaction(function() error({}) end) end)
    assert(latest().closed and not latest().commit)
end)
check('callback contract errors roll back', function()
    expect('INVALID_ARGUMENT', function() DB.transaction(function() return 'yes' end) end)
    assert(latest().closed and not latest().commit)
end)
check('caught validation and result type errors poison transactions', function()
    for _, call in ipairs({ function(tx) tx.query('SELECT ?', {}) end, function(tx) tx.value('UPDATE X') end }) do
        local captured
        local ok, failure = pcall(function()
            DB.transaction(function(tx)
                local worked, why = pcall(call, tx)
                assert(not worked); captured = why.code
                return true
            end)
        end)
        assert(not ok and failure.code == captured and latest().closed and not latest().commit)
    end
end)
check('normal DB calls inside callback use independent pool exports', function()
    DB.transaction(function(tx)
        assert(tx.value('SELECT CONNECTION_ID()') == latest().id)
        assert(DB.value('SELECT CONNECTION_ID()') == 'pool')
        assert(tx.value('SELECT CONNECTION_ID()') == latest().id)
        return true
    end)
end)
check('nested same-coroutine transactions fail before acquiring another connection', function()
    local before = sequence
    expect('NESTED_TRANSACTION', function()
        DB.transaction(function() DB.transaction(function() return true end); return true end)
    end)
    assert(sequence == before + 1 and latest().closed and not latest().commit)
end)
check('closed transaction object cannot issue another query', function()
    local saved
    DB.transaction(function(tx) saved = tx; return true end)
    local count = #history
    expect('TRANSACTION_CLOSED', function() saved.query('SELECT 1') end)
    assert(#history == count)
end)
check('begin failures clear the nesting guard without invoking callback', function()
    failBegin = 'UNAVAILABLE'
    local called = false
    expect('UNAVAILABLE', function() DB.transaction(function() called = true end) end)
    assert(not called)
    failBegin = nil
    assert(DB.transaction(function() return true end))
end)
check('commit failure is raised rather than converted to success', function()
    failFinish = 'TRANSACTION_TIMEOUT'
    expect('TRANSACTION_TIMEOUT', function() DB.transaction(function() return true end) end)
    failFinish = nil
end)
check('timeout after callback suspension prevents successful commit', function()
    local worked, failure
    local co = coroutine.create(function()
        worked, failure = pcall(DB.transaction, function() coroutine.yield(); return true end)
    end)
    assert(coroutine.resume(co)); assert(coroutine.status(co) == 'suspended')
    latest().failure = 'TRANSACTION_TIMEOUT'
    assert(coroutine.resume(co))
    assert(not worked and failure.code == 'TRANSACTION_TIMEOUT' and latest().closed)
end)
check('timeout stays an error when callback requests intentional rollback', function()
    expect('TRANSACTION_TIMEOUT', function()
        DB.transaction(function() latest().failure = 'TRANSACTION_TIMEOUT'; return false end)
    end)
end)
check('different coroutines may have independent transactions simultaneously', function()
    local ids, results = {}, {}
    local function start(index)
        local co = coroutine.create(function()
            results[index] = DB.transaction(function(tx)
                ids[index] = tx.value('SELECT CONNECTION_ID()')
                coroutine.yield()
                assert(tx.value('SELECT CONNECTION_ID()') == ids[index])
                return true
            end)
        end)
        assert(coroutine.resume(co)); return co
    end
    local first, second = start(1), start(2)
    assert(ids[1] ~= ids[2])
    assert(coroutine.resume(second)); assert(coroutine.resume(first))
    assert(results[1] and results[2])
end)
check('exports derive transaction and ordinary-query ownership from caller', function()
    caller = 'owner-A'
    DB.transaction(function(tx)
        assert(lastOwner == 'owner-A')
        tx.value('SELECT 1'); assert(lastOwner == 'owner-A' and lastRequest.resource == 'owner-A')
        return true
    end)
    DB.value('SELECT 1'); assert(lastOwner == 'owner-A')
    caller = 'transaction-consumer'
end)
check('changing caller cannot use another resource transaction', function()
    expect('TRANSACTION_OWNER', function()
        DB.transaction(function(tx)
            caller = 'intruder'
            local ok, failure = pcall(tx.value, 'SELECT 1')
            caller = 'transaction-consumer'
            assert(not ok and failure.code == 'TRANSACTION_OWNER')
            return true
        end)
    end)
end)
check('provider stop settles pending tx query once and ignores late results', function()
    holdQuery = true
    local worked, failure
    local co = coroutine.create(function()
        worked, failure = pcall(DB.transaction, function(tx) tx.value('SELECT 1'); return true end)
    end)
    assert(coroutine.resume(co)); assert(coroutine.status(co) == 'suspended')
    state = 'stopped'; stopped('feather-mysql')
    assert(coroutine.resume(co))
    assert(not worked and failure.code == 'RESOURCE_STOPPED')
    local count = resolutions
    held(response({ kind = 'rows', rows = {}, firstColumn = 'z' }))
    assert(resolutions == count)
    holdQuery, state = false, 'started'
    assert(DB.transaction(function() return true end))
end)
check('provider stop during suspended callback is sticky across restart', function()
    local worked, failure
    local co = coroutine.create(function()
        worked, failure = pcall(DB.transaction, function() coroutine.yield(); return true end)
    end)
    assert(coroutine.resume(co)); stopped('feather-mysql')
    assert(coroutine.resume(co))
    assert(not worked and failure.code == 'RESOURCE_STOPPED' and not latest().commit)
end)
check('malformed bridge success and failure envelopes reject rather than hang', function()
    for _, payload in ipairs({ {}, { ok = false }, { ok = true, value = {} } }) do
        malformed = payload
        expect('BRIDGE_ERROR', function() DB.value('SELECT 1') end)
    end
end)
check('invalid transaction callback is rejected before acquire', function()
    local before = sequence
    expect('INVALID_ARGUMENT', function() DB.transaction({}) end)
    assert(sequence == before)
end)
local before = sequence
local worked, failure = pcall(DB.transaction, function() return true end)
check('transaction needs a yieldable context before acquire', function()
    assert(not worked and failure.code == 'INVALID_CONTEXT' and sequence == before)
end)
check('a plain DB call inside a transaction callback warns once, and still runs outside the transaction', function()
    local lines = capturePrint(function()
        DB.transaction(function(tx)
            assert(DB.value('SELECT CONNECTION_ID()') == 'pool')
            assert(DB.value('SELECT CONNECTION_ID()') == 'pool')
            assert(tx.value('SELECT CONNECTION_ID()') == latest().id)
            return true
        end)
    end)
    assert(#lines == 1, 'One warning per transaction, not one per call')
    assert(lines[1]:find('NOT part of the transaction', 1, true) and lines[1]:find('DB.value', 1, true), lines[1])
    assert(lines[1]:find('tx.value', 1, true) and lines[1]:find('feather-mysql', 1, true))
    lines = capturePrint(function()
        DB.value('SELECT 1')
        DB.transaction(function(tx) tx.value('SELECT 1'); return true end)
    end)
    assert(#lines == 0, 'No warning outside a transaction or for tx.* calls')
    lines = capturePrint(function()
        DB.transaction(function() DB.exec('UPDATE X'); return true end)
        DB.transaction(function() DB.exec('UPDATE X'); return true end)
    end)
    assert(#lines == 2, 'Each transaction warns independently')
end)
check('the warning names the call site, reports each place once, and repeats under devmode', function()
    local function busy()
        return capturePrint(function()
            for _ = 1, 3 do
                DB.transaction(function() DB.exec('UPDATE X'); return true end)   -- the same place every time
            end
        end)
    end
    local lines = busy()
    assert(#lines == 1, 'One place is reported once, not once per transaction')
    assert(lines[1]:find(' at ', 1, true) and lines[1]:find('transaction_spec.lua:%d+'), lines[1])
    assert(not lines[1]:find('lib/DB.lua', 1, true), 'The site is the caller, not the library')
    local real = GetConvar
    GetConvar = function(name, default) if name == 'feather_mysql_devmode' then return 'true' end return real(name, default) end
    lines = busy()
    GetConvar = real
    assert(#lines == 3, 'Devmode reports every transaction: ' .. #lines)
end)
check('transaction errors are readable, keep their cause and say what to assume', function()
    local failure = expect('LUA_ERROR', function() DB.transaction(function() error('callback exploded') end) end)
    assert(failure.outcome == 'rolled_back', 'A failed callback is always rolled back')
    local text = tostring(failure)
    assert(text:find('LUA_ERROR', 1, true) and text:find('callback exploded', 1, true) and text:find('outcome=rolled_back', 1, true))
    assert(type(failure.traceback) == 'string')
    local nested = expect('NESTED_TRANSACTION', function() DB.transaction(function() DB.transaction(function() return true end) end) end)
    assert(nested.outcome == 'not_executed')
    local closed
    DB.transaction(function(tx) closed = tx; return true end)
    assert(expect('TRANSACTION_CLOSED', function() closed.value('SELECT 1') end).outcome == 'not_executed')
end)
check('a non-table failure while waiting for the provider becomes a structured error and still rolls back', function()
    local realAwait, armed = Citizen.Await, false
    Citizen.Await = function(deferred)
        if armed then armed = false; error('await exploded') end
        return realAwait(deferred)
    end
    local failure = expect('BRIDGE_ERROR', function()
        DB.transaction(function(tx) armed = true; tx.value('SELECT 1'); return true end)
    end)
    Citizen.Await = realAwait
    assert(type(failure) == 'table' and failure.outcome == 'unknown' and failure.message:find('await exploded', 1, true))
    assert(latest().closed and not latest().commit, 'The transaction was rolled back, not committed')
    assert(DB.transaction(function() return true end), 'Later transactions work')
end)
print(('Lua transaction contract tests: %d passed'):format(tests))
