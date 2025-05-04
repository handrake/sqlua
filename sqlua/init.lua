local ffi = require("ffi")
local sqlite3 = ffi.load("sqlite3")

ffi.cdef[[
  typedef long long sqlite3_int64;
  typedef struct sqlite3 sqlite3;
  typedef struct sqlite3_stmt sqlite3_stmt;

  typedef void (*sqlite3_destructor_type)(void*);

  int sqlite3_open(const char *filename, sqlite3 **ppDb);
  int sqlite3_close(sqlite3*);
  int sqlite3_prepare_v2(sqlite3*, const char *zSql, int nByte,
                         sqlite3_stmt **ppStmt, const char **pzTail);

  int sqlite3_bind_null(sqlite3_stmt*, int);
  int sqlite3_bind_int64(sqlite3_stmt*, int, sqlite3_int64);
  int sqlite3_bind_int(sqlite3_stmt*, int, int);
  int sqlite3_bind_double(sqlite3_stmt*, int, double);
  int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int, sqlite3_destructor_type);
  int sqlite3_bind_parameter_count(sqlite3_stmt*);
  int sqlite3_bind_parameter_index(sqlite3_stmt*, const char *zName);

  int sqlite3_changes(sqlite3*);
  int sqlite3_step(sqlite3_stmt*);
  int sqlite3_finalize(sqlite3_stmt*);
  const char *sqlite3_errmsg(sqlite3*);
  int sqlite3_column_count(sqlite3_stmt*);
  const char *sqlite3_column_name(sqlite3_stmt*, int N);
  const unsigned char *sqlite3_column_text(sqlite3_stmt*, int iCol);
  int sqlite3_column_type(sqlite3_stmt*, int iCol);
  int sqlite3_reset(sqlite3_stmt*);
]]

local SQLITE_TRANSIENT = ffi.cast("sqlite3_destructor_type", -1)

local M = {}

function M.connect(path)
  local db_ptr = ffi.new("sqlite3*[1]")
  local rc = sqlite3.sqlite3_open(path, db_ptr)
  if rc ~= 0 then
    error("sqlite3_open failed: " .. ffi.string(sqlite3.sqlite3_errmsg(db_ptr[0])))
  end

  local db = {
    _db = db_ptr[0],
    _stmt_cache = {}
  }
  setmetatable(db, {
    __index = M._db_methods,
  })
  return db
end

M._db_methods = {}

function M._db_methods:close()
  for _, stmt in pairs(self._stmt_cache or {}) do
    stmt:finalize()
  end
  sqlite3.sqlite3_close(self._db)
  self._stmt_cache = {}
end

local function bind_value(stmt, idx, v)
    local t = type(v)

    if t == "number" then
      if v % 1 == 0 and math.abs(v) <= 9007199254740991 then
        sqlite3.sqlite3_bind_int64(stmt, idx, v)
      else
        sqlite3.sqlite3_bind_double(stmt, idx, v)
      end

    elseif t == "string" then
      sqlite3.sqlite3_bind_text(stmt, idx, v, #v, SQLITE_TRANSIENT)

    elseif v == nil then
      sqlite3.sqlite3_bind_null(stmt, idx)

    elseif ffi.istype("int64_t", v) then
      sqlite3.sqlite3_bind_int64(stmt, idx, v)

    else
      error("unsupported bind type: " .. t)
    end
end

local function bind_named(stmt, params)
  for k, v in pairs(params) do
    local idx
    if type(k) == "string" then
      idx = sqlite3.sqlite3_bind_parameter_index(stmt, ":" .. k)
      if idx == 0 then
        error("no such named parameter: :" .. k)
      end
    else
      error("invalid parameter key: " .. tostring(k))
    end
    bind_value(stmt, idx, v)
  end
end

local function bind_positional(stmt, params)
  for idx, v in ipairs(params) do
    bind_value(stmt, idx, v)
  end
end

function M._db_methods:_get_cached_stmt(sql)
  local cached = self._stmt_cache[sql]
  if cached and not cached._finalized then
    sqlite3.sqlite3_reset(cached._stmt)
    return cached
  end

  local stmt_ptr = ffi.new("sqlite3_stmt*[1]")
  local rc = sqlite3.sqlite3_prepare_v2(self._db, sql, #sql, stmt_ptr, nil)
  if rc ~= 0 then
    error("prepare failed: " .. ffi.string(sqlite3.sqlite3_errmsg(self._db)))
  end

  local stmt = {
    _stmt = stmt_ptr[0],
    _db = self._db,
    _finalized = false,
  }

  function stmt:finalize()
    if not self._finalized then
      sqlite3.sqlite3_finalize(self._stmt)
      self._finalized = true
    end
  end

  function stmt:reset()
    sqlite3.sqlite3_reset(self._stmt)
  end

  function stmt:bind(params)
    if not params then return end

    local is_array = #params > 0

    if is_array then
      bind_positional(self._stmt, params)
    else
      bind_named(self._stmt, params)
    end
  end

  function stmt:step()
    return sqlite3.sqlite3_step(self._stmt)
  end

  function stmt:collect_row()
    local row = {}
    local col_count = sqlite3.sqlite3_column_count(self._stmt)
    for i = 0, col_count - 1 do
      local name = ffi.string(sqlite3.sqlite3_column_name(self._stmt, i))
      local text = sqlite3.sqlite3_column_text(self._stmt, i)
      row[name] = text ~= nil and ffi.string(text) or nil
    end
    return row
  end

  self._stmt_cache[sql] = stmt
  return stmt
end

function M._db_methods:_prepare_and_bind(sql, params)
  local stmt = self:_get_cached_stmt(sql)
  if params then stmt:bind(params) end
  return stmt
end

function M._db_methods:_row_iterator(stmt)
  local done = false
  return function()
    if done then return nil end
    local rc = stmt:step()
    if rc == 100 then
      return stmt:collect_row()
    elseif rc == 101 then
      done = true
      stmt:reset()
      return nil
    else
      done = true
      stmt:reset()
      error("step failed: " .. ffi.string(sqlite3.sqlite3_errmsg(stmt._db)))
    end
  end
end

function M._db_methods:rows(sql, params)
  local stmt = self:_prepare_and_bind(sql, params)
  return self:_row_iterator(stmt)
end

function M._db_methods:execute(sql, params)
  local stmt = self:_prepare_and_bind(sql, params)
  local returns_rows = sqlite3.sqlite3_column_count(stmt._stmt) > 0

  if returns_rows then
    local result = {}
    for row in self:_row_iterator(stmt) do
      table.insert(result, row)
    end
    return result
  else
    local rc = stmt:step()
    if rc ~= 101 and rc ~= 100 then
      error("step failed: " .. ffi.string(sqlite3.sqlite3_errmsg(self._db)))
    end
    stmt:reset()
    local changes = sqlite3.sqlite3_changes(self._db)
    return true, changes
  end
end

return M
