local ffi = require("ffi")
local sqlite3 = ffi.load("sqlite3")

ffi.cdef[[
  typedef struct sqlite3 sqlite3;
  typedef struct sqlite3_stmt sqlite3_stmt;

  int sqlite3_open(const char *filename, sqlite3 **ppDb);
  int sqlite3_close(sqlite3*);
  int sqlite3_prepare_v2(sqlite3*, const char *zSql, int nByte,
                         sqlite3_stmt **ppStmt, const char **pzTail);
  int sqlite3_step(sqlite3_stmt*);
  int sqlite3_finalize(sqlite3_stmt*);
  const char *sqlite3_errmsg(sqlite3*);
  int sqlite3_column_count(sqlite3_stmt*);
  const char *sqlite3_column_name(sqlite3_stmt*, int N);
  const unsigned char *sqlite3_column_text(sqlite3_stmt*, int iCol);
  int sqlite3_column_type(sqlite3_stmt*, int iCol);
  int sqlite3_reset(sqlite3_stmt*);
]]

local M = {}

function M.connect(path)
  local db_ptr = ffi.new("sqlite3*[1]")
  local rc = sqlite3.sqlite3_open(path, db_ptr)
  if rc ~= 0 then
    error("sqlite3_open failed: " .. ffi.string(sqlite3.sqlite3_errmsg(db_ptr[0])))
  end

  local db = { _db = db_ptr[0] }
  setmetatable(db, { __index = M._db_methods })
  return db
end

M._db_methods = {}

function M._db_methods:close()
  sqlite3.sqlite3_close(self._db)
end

function M._db_methods:execute(sql)
  local stmt_ptr = ffi.new("sqlite3_stmt*[1]")
  local rc = sqlite3.sqlite3_prepare_v2(self._db, sql, #sql, stmt_ptr, nil)
  if rc ~= 0 then
    error("sqlite3_prepare_v2 failed: " .. ffi.string(sqlite3.sqlite3_errmsg(self._db)))
  end

  local stmt = stmt_ptr[0]
  local result = {}

  while true do
    local step = sqlite3.sqlite3_step(stmt)
    if step == 100 then -- SQLITE_ROW
      local row = {}
      local col_count = sqlite3.sqlite3_column_count(stmt)
      for i = 0, col_count - 1 do
        local name = ffi.string(sqlite3.sqlite3_column_name(stmt, i))
        local text = sqlite3.sqlite3_column_text(stmt, i)
        row[name] = text ~= nil and ffi.string(text) or nil
      end
      table.insert(result, row)
    elseif step == 101 then -- SQLITE_DONE
      break
    else
      error("sqlite3_step failed")
    end
  end

  sqlite3.sqlite3_finalize(stmt)
  return result
end

return M
