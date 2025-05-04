package.path = "./?.lua;./?/init.lua;" .. package.path

local sqlua = require("sqlua")

local function test_insert()
  local db = sqlua.connect(":memory:")

  db:execute("CREATE TABLE users (id INTEGER, name TEXT)")
  db:execute("INSERT INTO users VALUES (?, ?)", { 1, "Alice" })

  local rows = db:execute("SELECT * FROM users")
  assert(#rows == 1, "Expected 1 row")
  assert(rows[1].id == "1", "Expected id=1")
  assert(rows[1].name == "Alice", "Expected name='Alice'")

  db:close()
  print("✅ single insert test passed.")
end

local function test_insert_file()
  local test_file_name = "./_test.db"
  local db = sqlua.connect(test_file_name)

  db:execute("CREATE TABLE users (id INTEGER, name TEXT)")
  db:execute("INSERT INTO users VALUES (?, ?)", { 1, "Alice" })

  local rows = db:execute("SELECT * FROM users")
  assert(#rows == 1, "Expected 1 row")
  assert(rows[1].id == "1", "Expected id=1")
  assert(rows[1].name == "Alice", "Expected name='Alice'")

  db:close()
  os.remove(test_file_name)
  print("✅ insert file test passed.")
end

local function test_insert_placeholder()
  local db = sqlua.connect(":memory:")

  db:execute("CREATE TABLE users (id INTEGER, name TEXT)")
  db:execute("INSERT INTO users VALUES (:id, :name)", {
    id = 1,
    name = "Alice"
  })

  local rows = db:execute("SELECT * FROM users")
  assert(#rows == 1, "Expected 1 row")
  assert(rows[1].id == "1", "Expected id=1")
  assert(rows[1].name == "Alice", "Expected name='Alice'")

  db:close()
  print("✅ placeholder insert test passed.")
end

local function test_rows()
  local db = sqlua.connect(":memory:")
  assert(db.rows, "db:rows() not found!")

  -- Setup test table
  db:execute("CREATE TABLE users (id INTEGER, name TEXT)")
  db:execute("INSERT INTO users VALUES (?, ?)", {1, "Alice"})
  db:execute("INSERT INTO users VALUES (?, ?)", {2, "Bob"})

  -- Row iterator test
  local seen = {}

  for row in db:rows("SELECT * FROM users ORDER BY id") do
    print(row.id, row.name)
    seen[tonumber(row.id)] = row.name
  end

  -- Check values
  assert(seen[1] == "Alice", "Expected id=1 to be Alice")
  assert(seen[2] == "Bob",   "Expected id=2 to be Bob")

  -- Cleanup
  db:close()
  print("✅ row iterator test passed.")
end

local function test_changes()
  local db = sqlua.connect(":memory:")
  assert(db.rows, "db:rows() not found!")

  db:execute("CREATE TABLE users (id INTEGER, name TEXT)")
  db:execute("INSERT INTO users VALUES (?, ?)", {1, "Alice"})
  db:execute("INSERT INTO users VALUES (?, ?)", {2, "Bob"})

  local ok, affected = db:execute("UPDATE users SET name = ? WHERE id = ?", {"Jane", 1})
  assert(ok)
  assert(affected == 1)

  db:close()
  print("🔄 rows updated:", affected)
end

local function test_stmt_cache_leak()
  local db1 = sqlua.connect(":memory:")
  local sql1 = "CREATE TABLE users (id INTEGER, name TEXT)"

  db1:execute(sql1)
  db1:execute("INSERT INTO users VALUES (?, ?)", { 1, "Alice" })

  local rows1 = db1:execute("SELECT * FROM users")
  assert(#rows1 == 1, "Expected 1 row")
  assert(rows1[1].id == "1", "Expected id=1")
  assert(rows1[1].name == "Alice", "Expected name='Alice'")

  assert(db1:stmt_cache_size() ~= 0)

  db1:close()

  assert(db1:stmt_cache_size() == 0)

  local db2 = sqlua.connect(":memory:")

  db2:execute("CREATE TABLE users (id INTEGER, name TEXT)")
  db2:execute("INSERT INTO users VALUES (?, ?)", { 1, "Alice" })

  local rows2 = db2:execute("SELECT * FROM users")
  assert(#rows2 == 1, "Expected 1 row")
  assert(rows2[1].id == "1", "Expected id=1")
  assert(rows2[1].name == "Alice", "Expected name='Alice'")

  db2:close()

  print("✅ stmt cache leak test passed.")
end

test_insert()
test_insert_file()
test_insert_placeholder()
test_rows()
test_changes()
test_stmt_cache_leak()
