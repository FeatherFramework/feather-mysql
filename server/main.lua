local sequence, pending = 0, {}
local provider = GetCurrentResourceName()

local function operation(method, sql, callback)
    -- Cfx deserializes cross-resource Lua function references as callable tables.
    local callbackMt = type(callback) == 'table' and getmetatable(callback)
    if type(callback) ~= 'function' and not (type(callbackMt) == 'table' and type(callbackMt.__call) == 'function') then
        return
    end
    -- Capture before asynchronous work; never accept caller-supplied attribution.
    local resource = GetInvokingResource() or provider
    sequence = sequence + 1
    local context = { resource = resource, method = type(method) == 'string' and method or 'invalid', id = sequence, sql = sql }
    local started, completed = GetGameTimer(), false
    local function finish(value, err)
        if completed then return end
        completed = true
        pending[context.id] = nil
        FeatherMySQL.log(context, (GetGameTimer() - started) % 4294967296, err)
        local ok = pcall(callback, { ok = err == nil, value = value, error = err })
        if not ok then print(('[%s] Consumer callback unavailable: %s'):format(provider, resource)) end
    end
    local function fail(code, message, driverCode, extra)
        finish(nil, FeatherMySQL.error(code, message, resource, context.method, context.id, driverCode, extra))
    end
    pending[context.id] = fail
    return context, finish, fail
end

local function bridge(name, arguments, finish, fail, convert)
    arguments.n = arguments.n + 1
    arguments[arguments.n] = function(response)
        if type(response) ~= 'table' or type(response.ok) ~= 'boolean' then
            fail('BRIDGE_ERROR', 'Invalid database bridge response')
        elseif not response.ok then
            local err = response.error
            if type(err) ~= 'table' or type(err.code) ~= 'string' or type(err.message) ~= 'string' then
                fail('BRIDGE_ERROR', 'Invalid database bridge error')
            else
                fail(err.code, err.message, err.driverCode, err)
            end
        elseif convert then
            local payload = response.value
            if type(payload) ~= 'table' or (payload.kind ~= 'rows' and payload.kind ~= 'write') then
                fail('BRIDGE_ERROR', 'Invalid database result'); return
            end
            local ok, value, resultError = pcall(convert, response.value)
            if not ok then fail('BRIDGE_ERROR', 'Invalid database result')
            elseif resultError then fail('RESULT_TYPE', resultError)
            else finish(value) end
        else
            finish(response.value)
        end
    end
    local ok = pcall(function()
        local driver = exports[provider]
        driver[name](driver, table.unpack(arguments, 1, arguments.n))
    end)
    if not ok then fail('UNAVAILABLE', 'Database bridge unavailable') end
end

-- One statement. compat.lua calls this directly, so attribution still comes from
-- GetInvokingResource() of whichever export the caller used.
function FeatherMySQL.execute(method, sql, parameters, callback)
    local context, finish, fail = operation(method, sql, callback)
    if not context then return end
    local request, validationError = FeatherMySQL.validate(method, sql, parameters)
    if not request then fail('INVALID_ARGUMENT', validationError); return end
    request.resource = context.resource
    bridge('DriverExecuteV1', table.pack(request), finish, fail, function(payload)
        return FeatherMySQL.result(method, payload)
    end)
end
exports('ExecuteV1', FeatherMySQL.execute)

-- { ready = boolean, code = string|nil }: whether the database has been reached since start.
function FeatherMySQL.ready(callback)
    local context, finish, fail = operation('ready', nil, callback)
    if not context then return end
    bridge('DriverReadyV1', table.pack(), finish, fail)
end
exports('ReadyV1', FeatherMySQL.ready)

-- A fixed list of { sql, parameters } as one transaction; the value is true once committed.
function FeatherMySQL.batch(statements, callback)
    local context, finish, fail = operation('transaction.batch', nil, callback)
    if not context then return end
    local requests = {}
    for index, statement in ipairs(type(statements) == 'table' and statements or {}) do
        local request, problem = FeatherMySQL.validate('raw', statement.sql, statement.parameters)
        if not request then fail('INVALID_ARGUMENT', ('Statement %d: %s'):format(index, problem)); return end
        requests[index] = request
    end
    if #requests == 0 then fail('INVALID_ARGUMENT', 'A transaction needs at least one statement'); return end
    bridge('DriverTransactionBatchV1', table.pack(context.resource, requests), finish, fail)
end

exports('BeginTransactionV1', function(callback)
    local context, finish, fail = operation('transaction.begin', nil, callback)
    if not context then return end
    bridge('DriverTransactionBeginV1', table.pack(context.resource), finish, fail)
end)

exports('TransactionQueryV1', function(id, method, sql, parameters, callback)
    local context, finish, fail = operation(method, sql, callback)
    if not context then return end
    if type(id) ~= 'string' or id == '' then fail('INVALID_ARGUMENT', 'Invalid transaction ID'); return end
    local request, validationError = FeatherMySQL.validate(method, sql, parameters)
    if not request then fail('INVALID_ARGUMENT', validationError); return end
    request.resource = context.resource
    bridge('DriverTransactionQueryV1', table.pack(context.resource, id, request), finish, fail, function(payload)
        return FeatherMySQL.result(method, payload)
    end)
end)

exports('FinishTransactionV1', function(id, commit, callback)
    local context, finish, fail = operation('transaction.finish', nil, callback)
    if not context then return end
    if type(id) ~= 'string' or id == '' or type(commit) ~= 'boolean' then
        fail('INVALID_ARGUMENT', 'Invalid transaction completion arguments'); return
    end
    bridge('DriverTransactionFinishV1', table.pack(context.resource, id, commit), finish, fail)
end)

-- The mocked test suites cannot prove the real Cfx boundary, so the provider
-- checks it once at start: the private export must accept its own calls (the
-- caller-identity guard) and the value shapes a result contains must arrive intact.
local function selfCheck()
    local expectedText = 'h\u{e9}llo \u{1F600} \u{65e5}\u{672c}\u{8a9e}'
    local answered = false
    local function report(problem)
        if problem then
            print(('[%s] Bridge self-check FAILED: %s. Database calls will not work.'):format(provider, problem))
        else
            print(('[%s] Bridge self-check OK.'):format(provider))
        end
    end
    local ok = pcall(function()
        local driver = exports[provider]
        driver.DriverSelfCheckV1(driver, function(response)
            answered = true
            if type(response) ~= 'table' or response.ok ~= true then
                local err = type(response) == 'table' and response.error
                report(type(err) == 'table' and ('%s %s'):format(tostring(err.code), tostring(err.message)) or 'invalid response')
                return
            end
            local row = type(response.value) == 'table' and type(response.value.rows) == 'table' and response.value.rows[1]
            if type(row) ~= 'table' or row.text ~= expectedText or row.number ~= 42 or row.nothing ~= nil
                or type(row.bytes) ~= 'table' or row.bytes[1] ~= 0 or row.bytes[2] ~= 255
                or row.big ~= '9007199254740993' or row.flag ~= false or response.value.first ~= expectedText then
                report('values did not round-trip through the Cfx boundary')
            else
                report(nil)
            end
        end)
    end)
    if not ok then report('the bridge export is unavailable'); return end
    SetTimeout(5000, function()
        if not answered then report('no response within 5 seconds') end
    end)
end

local function provides(name)
    for index = 0, (GetNumResourceMetadata(provider, 'provide') or 0) - 1 do
        if GetResourceMetadata(provider, 'provide', index) == name then return true end
    end
    return false
end

-- Say so when this resource stands in for oxmysql, and warn about the one setup
-- that is ambiguous: both a real oxmysql and a provider of that name running.
local function announceProvision()
    if not provides('oxmysql') then return end
    local state = GetResourceState('oxmysql')
    if state == 'started' or state == 'starting' then
        print(('[%s] WARNING: this resource provides "oxmysql" while a resource named oxmysql is running. '
            .. 'Stop one of them, or remove the provide line from fxmanifest.lua.'):format(provider))
    else
        print(('[%s] Providing "oxmysql": resources that import @oxmysql/lib/MySQL.lua or depend on oxmysql use this resource.')
            :format(provider))
    end
end

AddEventHandler('onResourceStart', function(resource)
    if resource ~= provider then return end
    selfCheck()
    announceProvision()
end)

AddEventHandler('onResourceStop', function(resource)
    if resource ~= provider then return end
    local callbacks = {}
    for _, fail in pairs(pending) do callbacks[#callbacks + 1] = fail end
    for _, fail in ipairs(callbacks) do
        fail('RESOURCE_STOPPED', 'Database resource stopped; write outcome may be unknown')
    end
end)
