-- Exports named like oxmysql's, for callers that use exports.oxmysql:query(...) and friends.
-- They are reachable under that name once this resource provides 'oxmysql' (see the README).
-- Only a small subset exists. The oxmysql Lua library (@oxmysql/lib/MySQL.lua) does not use
-- these; it is replaced by lib/MySQL.lua, which runs inside each consumer.
local provider = GetCurrentResourceName()

-- Named placeholders (@name, :name) are rewritten to positional ones before anything is sent.
local Named = assert(load(assert(LoadResourceFile(provider, 'lib/Named.lua')), '@' .. provider .. '/lib/Named.lua', 't', _ENV))()

-- Returns the SQL and parameters to send, or nil and the reason.
local function bind(sql, parameters)
    if Named.isNamed(parameters) then
        local statement, values = Named.convert(sql, parameters)
        if not statement then return nil, values end
        return statement, values
    end
    return sql, parameters
end

local function callable(value)
    if type(value) == 'function' then return true end
    local mt = type(value) == 'table' and getmetatable(value)
    return type(mt) == 'table' and type(mt.__call) == 'function'
end

-- With a callback the result is passed to it. Without one the caller's coroutine waits and the
-- result is returned. A failure is logged and produces `failed` (nil or false) instead of an
-- error, which is what callers of these exports expect.
local function call(name, start, callback, failed, transform)
    local resource = GetInvokingResource() or provider
    local function report(err)
        print(('[%s] %s failed for %s: %s%s'):format(provider, name, resource, tostring(err.code),
            err.message and (': ' .. tostring(err.message)) or ''))
    end
    local function outcome(envelope)
        if not envelope.ok then report(envelope.error); return failed end
        if transform then return transform(envelope.value) end
        return envelope.value
    end
    if callable(callback) then
        start(function(envelope)
            local ok = pcall(callback, outcome(envelope))
            if not ok then print(('[%s] %s callback failed in %s'):format(provider, name, resource)) end
        end)
        return
    end
    if not coroutine.isyieldable() then
        report({ code = 'INVALID_CONTEXT', message = 'waiting for a result needs a coroutine; pass a callback instead' })
        return failed
    end
    local waiting = promise.new()
    start(function(envelope) waiting:resolve(envelope) end)
    return outcome(Citizen.Await(waiting))
end

-- query returns rows for a SELECT and { affectedRows, insertId, warningStatus } for a write.
for export, method in pairs({ query = 'raw', single = 'one', scalar = 'value', insert = 'insert', update = 'exec' }) do
    exports(export, function(sql, parameters, callback)
        return call(export, function(done)
            local statement, values = bind(sql, parameters)
            if not statement then
                done({ ok = false, error = { code = 'INVALID_ARGUMENT', message = values } })
            else
                FeatherMySQL.execute(method, statement, values, done)
            end
        end, callback, nil)
    end)
end

-- Accepts a list whose items are a SQL string, { query = sql, values = {...} } or { sql, {...} }.
local function statementsFrom(queries)
    if type(queries) ~= 'table' then return nil, 'Transaction queries must be a list' end
    local statements = {}
    for index, item in ipairs(queries) do
        local sql, values
        if type(item) == 'string' then
            sql = item
        elseif type(item) == 'table' then
            sql, values = item.query or item[1], item.values or item[2]
        end
        if type(sql) ~= 'string' then return nil, ('Query %d has no SQL string'):format(index) end
        local statement, bound = bind(sql, values)
        if not statement then return nil, ('Query %d: %s'):format(index, bound) end
        statements[index] = { sql = statement, parameters = bound }
    end
    return statements
end

-- true when every statement succeeded and the transaction committed, false otherwise.
exports('transaction', function(queries, callback)
    local statements, problem = statementsFrom(queries)
    return call('transaction', function(done)
        if not statements then
            done({ ok = false, error = { code = 'INVALID_ARGUMENT', message = problem } })
        else
            FeatherMySQL.batch(statements, done)
        end
    end, callback, false)
end)

-- Whether the database has been reached since this resource started.
exports('isReady', function(callback)
    return call('isReady', function(done) FeatherMySQL.ready(done) end, callback, false,
        function(value) return type(value) == 'table' and value.ready == true end)
end)
