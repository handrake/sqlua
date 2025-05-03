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

  local db = { _db = db_ptr[0] }
  setmetatable(db, {
    __index = M._db_methods,
  })
  return db
end

M._db_methods = {
  _stmt_cache = {}
}

function M._db_methods:close()
  for _, stmt in pairs(self._stmt_cache or {}) do
    stmt:finalize()
  end
  sqlite3.sqlite3_close(self._db)
end

local function bind_params(stmt, params)
  for i, val in ipairs(params or {}) do
    local idx = i  -- 1-based
    local t = type(val)

    if t == "number" then
      -- Heuristic: treat as int if whole and within safe range
      if val % 1 == 0 and math.abs(val) <= 9007199254740991 then
        sqlite3.sqlite3_bind_int64(stmt, idx, val)
      else
        sqlite3.sqlite3_bind_double(stmt, idx, val)
      end

    elseif t == "string" then
      sqlite3.sqlite3_bind_text(stmt, idx, val, #val, SQLITE_TRANSIENT)

    elseif val == nil then
      sqlite3.sqlite3_bind_null(stmt, idx)

    elseif ffi.istype("int64_t", val) then
      sqlite3.sqlite3_bind_int64(stmt, idx, val)

    else
      error("unsupported bind type: " .. t)
    end
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
    bind_params(self._stmt, params)
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

function M._db_methods:_get_iterator(sql, params)
  local stmt = self:_get_cached_stmt(sql)
  if params then
    stmt:bind(params)
  end

  local done = false

  local function iter()
    if done then
      return nil
    end
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
      error("step failed: " .. ffi.string(sqlite3.sqlite3_errmsg(self._db)))
    end
  end

  return stmt, iter
end

function M._db_methods:rows(sql, params)
  local _, iter = self:_get_iterator(sql, params)
  return iter
end

function M._db_methods:execute(sql, params)
  local stmt, iter = self:_get_iterator(sql, params)

  local returns_rows = sqlite3.sqlite3_column_count(stmt._stmt) > 0
  if not returns_rows then
    while true do
      local rc = stmt:step()
      if rc == 101 then -- SQLITE_DONE
        break
      elseif rc ~= 100 then
        error("sqlite3_step failed")
      end
    end
    return true
  end

  local result = {}
  for row in iter do
    table.insert(result, row)
  end
  return result
end

return M
