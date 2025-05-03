package.path = "./?.lua;./?/init.lua;" .. package.path

local sqlua = require("sqlua")

local db = sqlua.connect(":memory:")

db:execute("CREATE TABLE users (id INTEGER, name TEXT)")
db:execute("INSERT INTO users VALUES (1, 'Alice')")

local rows = db:execute("SELECT * FROM users")
assert(#rows == 1, "Expected 1 row")
assert(rows[1].id == "1", "Expected id=1")
assert(rows[1].name == "Alice", "Expected name='Alice'")

db:close()
print("✅ sqlua test passed.")
