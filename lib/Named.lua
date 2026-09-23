-- Named placeholders for the MySQL-style adapter and exports.
--
--   isNamed(parameters)          true when the table has string keys, such as { id = 5 }
--   convert(sql, parameters)     returns sql, values   (or nil, problem)
--
-- Each @name or :name in the SQL that has a matching key (name, '@name' or ':name') becomes a
-- positional ? and its value is appended in order of appearance, so a name used twice is bound
-- twice. Values are never written into the SQL: only the placeholder text changes.
--
-- Left alone: text inside quotes, backticks and comments; user variables (@rownum); system
-- variables (@@version); assignments (:=). The native DB.* functions do not use this.
local Named = {}

local function isIdentifierStart(byte)
    return byte and (byte == 95 or (byte >= 65 and byte <= 90) or (byte >= 97 and byte <= 122))
end

local function isIdentifierPart(byte)
    return isIdentifierStart(byte) or (byte and byte >= 48 and byte <= 57)
end

function Named.isNamed(parameters)
    if type(parameters) ~= 'table' then return false end
    for key in pairs(parameters) do
        if type(key) == 'string' then return true end
    end
    return false
end

-- Index just after the quoted text that starts at `start` (a quote character).
local function skipQuoted(sql, start, quote)
    local index, length = start + 1, #sql
    while index <= length do
        local byte = sql:byte(index)
        if byte == 92 and quote ~= 96 then          -- backslash escapes the next character (not in `...`)
            index = index + 2
        elseif byte == quote then
            if sql:byte(index + 1) == quote then index = index + 2   -- doubled quote is an escaped quote
            else return index + 1 end
        else
            index = index + 1
        end
    end
    return length + 1
end

-- A name may be keyed as name, '@name' or ':name', whichever marker the SQL uses.
local function lookup(parameters, name)
    for _, key in ipairs({ name, '@' .. name, ':' .. name }) do
        local value = parameters[key]
        if value ~= nil then return value, key end
    end
    return nil, nil
end

function Named.convert(sql, parameters)
    if type(sql) ~= 'string' then return nil, 'SQL must be a string' end
    local integerKeys, stringKeys = false, false
    for key in pairs(parameters) do
        if type(key) == 'string' then stringKeys = true else integerKeys = true end
    end
    if integerKeys and stringKeys then
        return nil, 'Parameters cannot mix positional and named entries'
    end

    local out, values, used = {}, {}, {}
    local unresolved = {}
    local index, copied, length = 1, 1, #sql
    while index <= length do
        local byte = sql:byte(index)
        if byte == 39 or byte == 34 or byte == 96 then                       -- ' " `
            index = skipQuoted(sql, index, byte)
        elseif byte == 45 and sql:byte(index + 1) == 45 and (sql:byte(index + 2) or 32) <= 32 then   -- -- comment
            index = (sql:find('\n', index, true) or length) + 1
        elseif byte == 35 then                                                -- # comment
            index = (sql:find('\n', index, true) or length) + 1
        elseif byte == 47 and sql:byte(index + 1) == 42 then                  -- /* comment */
            local close = sql:find('*/', index + 2, true)
            index = close and close + 2 or length + 1
        elseif byte == 64 or byte == 58 then                                  -- @ or :
            local previous = index > 1 and sql:byte(index - 1) or nil
            local marker = byte == 64 and '@' or ':'
            local nextByte = sql:byte(index + 1)
            if byte == 64 and nextByte == 64 then
                index = index + 2                                             -- @@system_variable
            elseif isIdentifierPart(previous) or previous == 64 or previous == 58 or previous == 36 or not isIdentifierStart(nextByte) then
                index = index + 1                                             -- user@host, ::, :=, a lone marker
            else
                local finish = index + 1
                while isIdentifierPart(sql:byte(finish + 1)) do finish = finish + 1 end
                local name = sql:sub(index + 1, finish)
                local value, key = lookup(parameters, name)
                if key ~= nil then
                    out[#out + 1] = sql:sub(copied, index - 1)
                    out[#out + 1] = '?'
                    values[#values + 1] = value
                    used[key] = true
                    copied = finish + 1
                else
                    unresolved[#unresolved + 1] = marker .. name
                end
                index = finish + 1
            end
        else
            index = index + 1
        end
    end
    out[#out + 1] = sql:sub(copied)

    -- A key nothing used, together with a name nothing matched, is almost always a typo. Sending the
    -- unmatched name on would run the statement with an empty user variable, so refuse instead.
    local unused = {}
    for key in pairs(parameters) do
        if not used[key] then unused[#unused + 1] = key end
    end
    if #unresolved > 0 and #unused > 0 then
        table.sort(unused)
        return nil, ('Named parameters do not match the statement (unused: %s; not in the parameters: %s)')
            :format(table.concat(unused, ', '), table.concat(unresolved, ', '))
    end
    -- A named table that nothing in the statement refers to (for example { id = 1 } with SELECT ?)
    -- would send no values at all.
    if next(parameters) ~= nil and #values == 0 then
        return nil, 'None of the named parameters appear in the statement'
    end
    return table.concat(out), values
end

return Named
