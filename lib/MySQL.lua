-- Migration adapter for code written against oxmysql's MySQL library. It is what
-- @oxmysql/lib/MySQL.lua resolves to when this resource provides 'oxmysql', and it can also be
-- imported directly. New resources should import lib/DB.lua and use DB.* instead.
--
-- Covered: query, single, scalar, insert, update, prepare and transaction (callback and .await),
-- startTransaction, ready, isReady, awaitConnection. Everything else raises UNSUPPORTED_API.
if not IsDuplicityVersion() then error('feather-mysql is server-only', 0) end
if MySQL ~= nil then error('MySQL is already defined; load only one provider library', 0) end

-- Support replacing just the consumer's @oxmysql/lib/MySQL.lua import.
-- Execute the native library in this consumer's environment to preserve attribution.
local PROVIDER = 'feather-mysql'
if DB == nil then
    local source = LoadResourceFile(PROVIDER, 'lib/DB.lua')
    if not source then error('Unable to load ' .. PROVIDER .. '/lib/DB.lua', 0) end
    local initialize, loadError = load(source, '@' .. PROVIDER .. '/lib/DB.lua', 't', _ENV)
    if not initialize then error(loadError, 0) end
    initialize()
end

-- Named placeholders (@name, :name) are rewritten to positional ones before anything is sent.
local Named
do
    local source = LoadResourceFile(PROVIDER, 'lib/Named.lua')
    if not source then error('Unable to load ' .. PROVIDER .. '/lib/Named.lua', 0) end
    local chunk, loadError = load(source, '@' .. PROVIDER .. '/lib/Named.lua', 't', _ENV)
    if not chunk then error(loadError, 0) end
    Named = chunk()
end

local function failure(code, message, method)
    return { code = code, message = message, resource = GetCurrentResourceName(), method = method }
end

local function isCallback(value)
    if type(value) == 'function' then return true end
    -- Cfx cross-resource function references are callable Lua tables.
    local mt = type(value) == 'table' and getmetatable(value)
    return type(mt) == 'table' and type(mt.__call) == 'function'
end

local function parametersFor(method, parameters)
    if parameters == nil then return {}, 0 end
    if type(parameters) ~= 'table' then
        error(failure('INVALID_ARGUMENT', 'Parameters must be a dense positional table or nil', method), 0)
    end
    local count = 0
    for key in pairs(parameters) do
        if type(key) ~= 'number' or key < 1 or key % 1 ~= 0 then
            error(failure('INVALID_ARGUMENT', 'Only positional parameter tables are supported', method), 0)
        end
        count = count + 1
    end
    local values = {}
    for i = 1, count do
        local value = rawget(parameters, i)
        if value == nil then
            error(failure('INVALID_ARGUMENT', 'Parameter tables cannot contain holes', method), 0)
        end
        values[i] = value
    end
    return values, count
end

-- Returns the statement to run and its positional values. A plain list goes straight through; a
-- table with named keys has its placeholders rewritten first.
local function bind(method, sql, parameters)
    if Named.isNamed(parameters) then
        local statement, values = Named.convert(sql, parameters)
        if not statement then error(failure('INVALID_ARGUMENT', values, method), 0) end
        return statement, values, #values
    end
    local values, count = parametersFor(method, parameters)
    return sql, values, count
end

local function describe(err)
    if type(err) == 'table' and err.code then return tostring(err.code) .. ': ' .. tostring(err.message) end
    return tostring(err)
end

-- These calls report failure by returning false (as callers of oxmysql expect)
-- instead of raising, so the reason is printed here.
local function warn(method, err)
    print(('[%s] MySQL.%s failed in %s: %s'):format(PROVIDER, method, GetCurrentResourceName(), describe(err)))
end

-- The Cfx runtime reports an uncaught error that is not a string as only "error object is not a
-- string": no reason and no place. Scripts written for oxmysql raise and catch text, so every error
-- that leaves this adapter is text carrying the code, the reason and the caller's stack trace.
-- Inside the adapter, and in the callback form, errors stay tables that callers can inspect.
local function raiseText(err)
    local parts = { ('[%s] %s'):format(PROVIDER, describe(err)) }
    if type(err) == 'table' then
        local details = {}
        for _, key in ipairs({ 'method', 'driverCode', 'sqlState', 'outcome' }) do
            if err[key] ~= nil then details[#details + 1] = key .. '=' .. tostring(err[key]) end
        end
        if #details > 0 then parts[1] = parts[1] .. ' (' .. table.concat(details, ' ') .. ')' end
        if err.detail then parts[#parts + 1] = 'detail: ' .. tostring(err.detail) end
    end
    if type(debug) == 'table' and debug.traceback then parts[#parts + 1] = (debug.traceback('', 2):gsub('^\n', '')) end
    error(table.concat(parts, '\n'), 0)
end

local function guarded(fn)
    return function(...)
        local results = table.pack(pcall(fn, ...))
        if results[1] then return table.unpack(results, 2, results.n) end
        raiseText(results[2])
    end
end

local function awaitResult(method, operation, sql, parameters)
    local ok, result = pcall(function()
        local statement, values, count = bind(method, sql, parameters)
        return operation(statement, table.unpack(values, 1, count))
    end)
    if not ok then raiseText(result) end
    return result
end

local function withCallback(method, operation, sql, parameters, callback)
    if isCallback(parameters) and callback == nil then callback, parameters = parameters, nil end
    if not isCallback(callback) then
        error(failure('INVALID_CALLBACK', 'Callback required; use .await to receive a result directly', method), 0)
    end
    -- Snapshot the table before scheduling so later caller mutations cannot alter
    -- the statement's parameters. Value validation remains in the native API.
    local valid, statement, values, count = pcall(bind, method, sql, parameters)
    CreateThread(function()
        local ok, result = valid, statement   -- when the parameters were invalid, `statement` is the error
        if valid then ok, result = pcall(operation, statement, table.unpack(values, 1, count)) end
        local delivered, callbackError = pcall(function()
            if ok then callback(result, nil) else callback(nil, result) end
        end)
        if not delivered then
            -- The SQL already ran and is never retried. Show what the callback raised
            -- so the consumer's bug is not hidden.
            print(('[%s] MySQL.%s callback failed in %s: %s'):format(PROVIDER, method, GetCurrentResourceName(), tostring(callbackError)))
        end
    end)
end

-- prepare: one parameter list runs the statement once. A list of parameter lists runs it once
-- per list, all in one transaction, and returns one result per list.
local function prepareOperation(sql, ...)
    local sets = table.pack(...)
    if sets.n == 0 or type(sets[1]) ~= 'table' then return DB.raw(sql, ...) end
    local results = {}
    local ok, committed = pcall(DB.transaction, function(tx)
        for index = 1, sets.n do
            local statement, values, count = bind('prepare', sql, sets[index])
            results[index] = tx.raw(statement, table.unpack(values, 1, count))
        end
        return true
    end)
    if not ok then error(committed, 0) end
    return results
end

-- transaction: a list of statements, each a SQL string, { query = sql, values = {...} } or
-- { sql, {...} }. Returns true when all of them committed, false otherwise.
local function statementsFor(queries)
    if type(queries) ~= 'table' or queries[1] == nil then
        error(failure('INVALID_ARGUMENT', 'Transaction queries must be a non-empty list', 'transaction'), 0)
    end
    local statements = {}
    for index, item in ipairs(queries) do
        local sql, parameters
        if type(item) == 'string' then
            sql = item
        elseif type(item) == 'table' then
            sql, parameters = item.query or item[1], item.values or item[2]
        end
        if type(sql) ~= 'string' then
            error(failure('INVALID_ARGUMENT', ('Transaction query %d has no SQL string'):format(index), 'transaction'), 0)
        end
        local statement, values, count = bind('transaction', sql, parameters)
        statements[index] = { sql = statement, values = values, count = count }
    end
    return statements
end

local function runTransaction(statements)
    local ok, committed = pcall(DB.transaction, function(tx)
        for _, statement in ipairs(statements) do
            tx.raw(statement.sql, table.unpack(statement.values, 1, statement.count))
        end
        return true
    end)
    if not ok then warn('transaction', committed); return false end
    return committed == true
end

MySQL = { query = {}, single = {}, scalar = {}, insert = {}, update = {}, prepare = {}, transaction = {} }

-- query returns rows for a statement that returns rows, and { affectedRows, insertId,
-- warningStatus } for a write, like oxmysql.
function MySQL.query.await(sql, parameters)
    return awaitResult('query', DB.raw, sql, parameters)
end

function MySQL.single.await(sql, parameters)
    return awaitResult('single', DB.one, sql, parameters)
end

function MySQL.scalar.await(sql, parameters)
    return awaitResult('scalar', DB.value, sql, parameters)
end

function MySQL.insert.await(sql, parameters)
    return awaitResult('insert', DB.insert, sql, parameters)
end

function MySQL.update.await(sql, parameters)
    return awaitResult('update', DB.exec, sql, parameters)
end

function MySQL.prepare.await(sql, parameters)
    return awaitResult('prepare', prepareOperation, sql, parameters)
end

MySQL.transaction.await = guarded(function(queries)
    return runTransaction(statementsFor(queries))
end)

-- A callable table is needed for the legacy method(...) and method.await(...)
-- syntax to coexist. Define each entry explicitly; do not generate API methods.
setmetatable(MySQL.query, { __call = guarded(function(_, sql, parameters, callback)
    return withCallback('query', DB.raw, sql, parameters, callback)
end) })

setmetatable(MySQL.single, { __call = guarded(function(_, sql, parameters, callback)
    return withCallback('single', DB.one, sql, parameters, callback)
end) })

setmetatable(MySQL.scalar, { __call = guarded(function(_, sql, parameters, callback)
    return withCallback('scalar', DB.value, sql, parameters, callback)
end) })

setmetatable(MySQL.insert, { __call = guarded(function(_, sql, parameters, callback)
    return withCallback('insert', DB.insert, sql, parameters, callback)
end) })

setmetatable(MySQL.update, { __call = guarded(function(_, sql, parameters, callback)
    return withCallback('update', DB.exec, sql, parameters, callback)
end) })

setmetatable(MySQL.prepare, { __call = guarded(function(_, sql, parameters, callback)
    return withCallback('prepare', prepareOperation, sql, parameters, callback)
end) })

-- The callback is optional here, as in oxmysql.
setmetatable(MySQL.transaction, { __call = guarded(function(_, queries, callback)
    local statements = statementsFor(queries)
    if callback ~= nil and not isCallback(callback) then
        error(failure('INVALID_CALLBACK', 'The transaction callback must be a function', 'transaction'), 0)
    end
    CreateThread(function()
        local committed = runTransaction(statements)
        if callback then
            local delivered, callbackError = pcall(callback, committed)
            if not delivered then
                print(('[%s] MySQL.transaction callback failed in %s: %s'):format(PROVIDER, GetCurrentResourceName(), tostring(callbackError)))
            end
        end
    end)
end) })

-- startTransaction(function(query) ... end): `query(sql, parameters)` runs on the transaction's
-- connection and returns rows or a write header like MySQL.query. As in oxmysql,
-- returning false rolls back and anything else, including returning nothing, commits. That
-- differs from DB.transaction, where returning nothing rolls back. An error inside the callback
-- or in a query rolls back. Returns true when committed, false otherwise.
MySQL.startTransaction = guarded(function(callback)
    if type(callback) ~= 'function' then
        error(failure('INVALID_CALLBACK', 'startTransaction requires a function', 'startTransaction'), 0)
    end
    local ok, committed = pcall(DB.transaction, function(tx)
        local function query(sql, parameters)
            local statement, values, count = bind('startTransaction', sql, parameters)
            return tx.raw(statement, table.unpack(values, 1, count))
        end
        return callback(query) ~= false
    end)
    if not ok then warn('startTransaction', committed); return false end
    return committed == true
end)

-- Runs the callback once the provider has reached the database.
MySQL.ready = guarded(function(callback)
    if type(callback) ~= 'function' then
        error(failure('INVALID_CALLBACK', 'ready requires a function', 'ready'), 0)
    end
    CreateThread(function()
        DB.awaitReady()
        local ok, callbackError = pcall(callback)
        if not ok then
            print(('[%s] MySQL.ready callback failed in %s: %s'):format(PROVIDER, GetCurrentResourceName(), tostring(callbackError)))
        end
    end)
end)

-- False outside a coroutine, because asking needs to wait for the provider.
function MySQL.isReady()
    if not coroutine.isyieldable() then return false end
    return DB.isReady()
end

MySQL.awaitConnection = guarded(function()
    DB.awaitReady()
    return true
end)

-- No stubs: reject unknown API access before any SQL is dispatched. In particular, never emulate
-- rawExecute, Sync/Async aliases or named-parameter forms by guessing.
setmetatable(MySQL, { __index = function(_, method)
    raiseText(failure('UNSUPPORTED_API', 'MySQL.' .. tostring(method) .. ' is not supported by ' .. PROVIDER .. ' compatibility', tostring(method)))
end })
