-- `raw` returns rows for statements that produce rows and a write header otherwise. It exists for
-- callers that cannot know the statement kind in advance (the oxmysql compatibility layer).
local methods = { query = true, one = true, value = true, insert = true, exec = true, raw = true }
local MAX_SAFE_INTEGER = 9007199254740991

-- Integers must survive the trip through a JavaScript number exactly. Floats
-- only need to be finite. (math.abs would wrap math.mininteger, so compare both ends.)
local function numberIsSafe(value)
    if value ~= value then return false end
    if math.type(value) == 'integer' then return value >= -MAX_SAFE_INTEGER and value <= MAX_SAFE_INTEGER end
    return value ~= math.huge and value ~= -math.huge
end

function FeatherMySQL.validate(method, sql, parameters)
    if type(method) ~= 'string' or not methods[method] then return nil, 'Unsupported query method' end
    if type(sql) ~= 'string' or not sql:find('%S') then return nil, 'SQL must be a nonempty string' end
    if parameters == nil then parameters = {} end
    if type(parameters) ~= 'table' then return nil, 'Parameters must be a dense positional array' end
    local count = 0
    for key in pairs(parameters) do
        if type(key) ~= 'number' or key < 1 or key % 1 ~= 0 then return nil, 'Only positional parameters are supported' end
        count = count + 1
    end
    local packed = {}
    for i = 1, count do
        local value = parameters[i]
        local kind = type(value)
        if kind == 'table' and value.__feather_mysql_null == true then
            -- Check the entire table, irrespective of key iteration order.
            for key in pairs(value) do
                if key ~= '__feather_mysql_null' then return nil, 'Invalid NULL parameter' end
            end
            packed[i] = { isNull = true }
        elseif kind == 'string' or kind == 'boolean' then
            packed[i] = { value = value }
        elseif kind == 'number' and numberIsSafe(value) then
            packed[i] = { value = value }
        else
            return nil, 'Parameters must be strings, finite numbers (integers within +/-2^53-1), booleans or nil (SQL NULL)'
        end
    end
    return { sql = sql, parameters = packed, count = count }
end
