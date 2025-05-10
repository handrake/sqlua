package = "sqlua"
version = "0.1-3"

source = {
  url = "git+https://github.com/handrake/sqlua",
  tag = "v0.1"
}

description = {
  summary = "Pure FFI-based SQLite3 binding for LuaJIT",
  detailed = [[
    sqlua is a minimal SQLite3 binding for LuaJIT via FFI.
    No C compilation, supports named/positional params and cached prepared statements.
  ]],
  homepage = "https://github.com/handrake/sqlua",
  license = "MIT"
}

dependencies = {
  "luajit >= 2.0"
}

build = {
  type = "builtin",
  modules = {
    ["sqlua"] = "sqlua/init.lua"
  }
}
