"""Optional Linux runner when liblua5.4 is installed but the Lua CLI is absent."""
import ctypes
import ctypes.util
import os
from pathlib import Path

root = Path(__file__).resolve().parents[1]
os.chdir(root)
library = ctypes.util.find_library("lua5.4")
if not library:
    raise SystemExit("Install Lua 5.4, or run lua5.4 tests/lua_spec.lua directly")
lua = ctypes.CDLL(library)
lua.luaL_newstate.restype = ctypes.c_void_p
lua.luaL_openlibs.argtypes = [ctypes.c_void_p]
lua.luaL_loadfilex.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
lua.lua_pcallk.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_ssize_t, ctypes.c_void_p]
lua.lua_tolstring.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p]
lua.lua_tolstring.restype = ctypes.c_char_p
lua.lua_settop.argtypes = [ctypes.c_void_p, ctypes.c_int]
lua.lua_close.argtypes = [ctypes.c_void_p]
state = lua.luaL_newstate()
lua.luaL_openlibs(state)

def checked(code):
    if code:
        message = lua.lua_tolstring(state, -1, None)
        raise SystemExit(message.decode() if message else "Lua failed with a non-string error")

try:
    files = [p for p in root.rglob("*.lua") if "node_modules" not in p.parts]
    example = root.parent.parent / "[test]" / "feather-mysql-test"
    files.extend(example.glob("*.lua"))
    for path in files:
        checked(lua.luaL_loadfilex(state, os.fsencode(path), None))
        lua.lua_settop(state, 0)
    print(f"Lua 5.4 syntax: {len(files)} files passed", flush=True)
    for suite in (b"tests/lua_spec.lua", b"tests/compat_spec.lua", b"tests/transaction_spec.lua", b"tests/named_spec.lua"):
        # Each suite gets an isolated global environment, like separate resources.
        lua.lua_close(state)
        state = lua.luaL_newstate()
        lua.luaL_openlibs(state)
        checked(lua.luaL_loadfilex(state, suite, None))
        checked(lua.lua_pcallk(state, 0, 0, 0, 0, None))
finally:
    lua.lua_close(state)
