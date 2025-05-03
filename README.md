# Sqlua

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
