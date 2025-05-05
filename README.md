# Sqlua

[![GitHub Stars](https://img.shields.io/github/stars/handrake/sqlua?style=social)](https://github.com/handrake/sqlua/stargazers)
[![GitHub Watchers](https://img.shields.io/github/watchers/handrake/sqlua?style=social)](https://github.com/handrake/sqlua/watchers)

Pure LuaJIT+FFI SQLite3 binding with:

- Named + positional binding
- Statement caching
- Zero C code

## Example

```lua
local sqlua = require("sqlua")
local db = sqlua.connect(":memory:")

db:execute("CREATE TABLE users (id INT, name TEXT)")
db:execute("INSERT INTO users VALUES (?, ?)", {1, "Alice"})

for row in db:rows("SELECT * FROM users") do
    print(row.id, row.name)
end

db:close()
