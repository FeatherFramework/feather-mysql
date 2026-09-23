-- Run from the resource directory: lua5.4 tests/named_spec.lua
local Named = dofile('lib/Named.lua')
local passed = 0
local function check(name, fn)
    local ok, err = pcall(fn)
    if not ok then error(name .. ': ' .. tostring(err), 0) end
    passed = passed + 1
    print('PASS ' .. name)
end

local function convert(sql, parameters)
    local out, values = Named.convert(sql, parameters)
    return out, values
end
local function same(actual, expected)
    assert(#actual == #expected, ('expected %d values, got %d'):format(#expected, #actual))
    for i = 1, #expected do assert(actual[i] == expected[i], ('value %d: expected %s, got %s'):format(i, tostring(expected[i]), tostring(actual[i]))) end
end

check('only tables with string keys are named', function()
    assert(Named.isNamed({ id = 1 }) and Named.isNamed({ ['@id'] = 1 }) and Named.isNamed({ [':id'] = 1 }))
    assert(not Named.isNamed({}) and not Named.isNamed({ 1, 2, 3 }) and not Named.isNamed(nil) and not Named.isNamed('x') and not Named.isNamed(5))
end)

check('@name and :name become ? with the values in order of appearance', function()
    local sql, values = convert('SELECT * FROM t WHERE b = :second AND a = @first', { first = 'A', second = 'B' })
    assert(sql == 'SELECT * FROM t WHERE b = ? AND a = ?', sql)
    same(values, { 'B', 'A' })
end)

check('a name used twice is bound twice', function()
    local sql, values = convert('WHERE SQRT(POW(x - @x, 2) + POW(y - @y, 2)) <= @radius ORDER BY POW(x - @x, 2)', { x = 1, y = 2, radius = 9 })
    assert(sql == 'WHERE SQRT(POW(x - ?, 2) + POW(y - ?, 2)) <= ? ORDER BY POW(x - ?, 2)', sql)
    same(values, { 1, 2, 9, 1 })
end)

check('keys may be written with or without the marker', function()
    for _, keys in ipairs({ { id = 7 }, { ['@id'] = 7 }, { [':id'] = 7 } }) do
        local sql, values = convert('DELETE FROM t WHERE id = @id', keys)
        assert(sql == 'DELETE FROM t WHERE id = ?'); same(values, { 7 })
        sql, values = convert('DELETE FROM t WHERE id = :id', keys)
        assert(sql == 'DELETE FROM t WHERE id = ?'); same(values, { 7 })
    end
end)

check('false, zero and the empty string are values, not missing', function()
    local sql, values = convert('UPDATE t SET a = @a, b = @b, c = @c', { a = false, b = 0, c = '' })
    assert(sql == 'UPDATE t SET a = ?, b = ?, c = ?')
    same(values, { false, 0, '' })
end)

check('text inside quotes, backticks and comments is never touched', function()
    local sql, values = convert("SELECT `col@x`, 'a@x b:x', \"@x :x\", 'it\\'s @x', 'it''s @x' FROM t -- @x\nWHERE n = @x # :x\n/* @x */", { x = 3 })
    assert(sql == "SELECT `col@x`, 'a@x b:x', \"@x :x\", 'it\\'s @x', 'it''s @x' FROM t -- @x\nWHERE n = ? # :x\n/* @x */", sql)
    same(values, { 3 })
end)

check('user variables, system variables, assignments and user@host are left for the database', function()
    local sql, values = convert('SELECT @rownum := @rownum + 1 AS n, @@version, x FROM t WHERE id = @id AND a:=1 AND u = user@host', { id = 4 })
    assert(sql == 'SELECT @rownum := @rownum + 1 AS n, @@version, x FROM t WHERE id = ? AND a:=1 AND u = user@host', sql)
    same(values, { 4 })
end)

check('a mistyped name is refused instead of running with an empty variable', function()
    local sql, problem = Named.convert('DELETE FROM bcchousing WHERE houseid = @housid', { houseid = 5 })
    assert(sql == nil and problem:find('houseid', 1, true) and problem:find('@housid', 1, true), tostring(problem))
end)

check('an unused key alone is harmless; an unused key next to an unmatched name is refused', function()
    local sql, values = convert('SELECT 1 WHERE a = @a', { a = 1, extra = 2 })
    assert(sql == 'SELECT 1 WHERE a = ?'); same(values, { 1 })
    -- A user variable is fine while every key is used ...
    sql, values = convert('SELECT @n := @n + 1 FROM t WHERE id = @id', { id = 1 })
    assert(sql == 'SELECT @n := @n + 1 FROM t WHERE id = ?'); same(values, { 1 })
    -- ... but an unrelated key beside an unmatched name cannot be told apart from a typo.
    local refused, problem = Named.convert('SELECT @n := 1', { unrelated = 1 })
    assert(refused == nil and problem:find('unrelated', 1, true) and problem:find('@n', 1, true))
end)

check('a named table that nothing in the statement uses is refused', function()
    local sql, problem = Named.convert('SELECT ?', { id = 1 })
    assert(sql == nil and problem:find('None of the named parameters', 1, true))
    sql, problem = Named.convert('SELECT 1', { x = 1 })
    assert(sql == nil)
end)

check('mixed positional and named entries are refused', function()
    local sql, problem = Named.convert('SELECT ?, @a', { 1, a = 2 })
    assert(sql == nil and problem:find('mix', 1, true))
end)

check('multi-line SQL and unicode pass through unchanged around the placeholders', function()
    local sql, values = convert('SELECT name\n  FROM `t\u{e9}`\n WHERE city = @city -- \u{65e5}\u{672c}\n  AND note = \'\u{1F600}\'', { city = 'X' })
    assert(sql == 'SELECT name\n  FROM `t\u{e9}`\n WHERE city = ? -- \u{65e5}\u{672c}\n  AND note = \'\u{1F600}\'', sql)
    same(values, { 'X' })
end)

check('an unterminated quote or comment does not loop or lose text', function()
    local sql, values = convert("SELECT @y, 'open @x", { x = 1, y = 2 })
    assert(sql == "SELECT ?, 'open @x", sql); same(values, { 2 })
    sql, values = convert('SELECT @y /* open @x', { x = 1, y = 2 })
    assert(sql == 'SELECT ? /* open @x', sql); same(values, { 2 })
    sql, values = convert('SELECT @y -- @x', { x = 1, y = 2 })
    assert(sql == 'SELECT ? -- @x', sql); same(values, { 2 })
end)

check('values are carried as given, never written into the SQL', function()
    local hostile = "x'; DROP TABLE t; -- @y"
    local sql, values = convert('SELECT * FROM t WHERE a = @a AND b = @b', { a = hostile, b = '@a' })
    assert(sql == 'SELECT * FROM t WHERE a = ? AND b = ?' and not sql:find('DROP', 1, true))
    same(values, { hostile, '@a' })
end)

print(('Named placeholder tests: %d passed'):format(passed))
