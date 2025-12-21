--[[

	libmdbx binding.
	Written by Cosmin Apreutsei. Public Domain.

	libmdbx is a super-fast mmap-based MVCC key-value store in 40 KLOC of C.
	libmdbx provides ACID with serializable semantics, good for read-heavy loads.

DATABASES

	[try_]mdbx_open(file_path, [opt]) -> db open/create a database
		opt.max_readers    4K                max read txns across all processes
		opt.max_tables     4K                max named tables
		opt.readonly       false             open in r/o mode and don't create
		opt.file_mode      0660
		opt.flags                            see MDBX_env_flags
	db:close()                              close db
	db:db_max_key_size() -> n               get max key size in bytes

TRANSACTIONS

	db:tx([flags])  -> tx                   open r/o transaction
	db:txw([parent_tx], [flags]) -> tx      open r/w transaction (flags is MDBX_txn_flags)
	tx:txw([flags]) -> tx                   open r/w nested transaction
	tx:commit()
	tx:abort()
	tx:closed() -> t|f
	db:atomic(['w',], fn, ...) -> ...       run fn in transaction
		fn(tx, ...) -> ...
	tx:atomic(fn, ...) -> ...               run fn in sub-transaction
		fn(tx, ...) -> ...

TABLES

	tx:dbi(table_name|dbi, ['w']) -> dbi
	tx:[try_]stat(table_name|dbi) -> MDBX_stat    get storage metrics on table

	tx:[try_]rename_table (table_name|dbi)  rename table
	tx:[try_]drop_table   (table_name|dbi)  drop table
	tx:[try_]clear_table  (table_name|dbi)  delete all records

	tx:each_table() -> iter() -> table_name
	tx:table_count() -> n
	tx:table_exists(table_name) -> t|f

CRUD

	tx:[must_]get_raw    (table_name|dbi, key_data, key_size) -> val_data, val_size | nil,0,err
	tx:[try_]put_raw     (table_name|dbi, key_data, key_size, val_data, val_size, [flags])
	tx:[try_]insert_raw  (table_name|dbi, key_data, key_size, val_data, val_size, [flags]) -> true | nil,'exists'
	tx:[try_]update_raw  (table_name|dbi, key_data, key_size, val_data, val_size, [flags]) -> true | nil,'not_found'
	tx:[try__]del_raw    (table_name|dbi, key_data, key_size, [val_data], [val_size], [flags]) -> true|nil,err
	tx:[try_]move_key_raw(table_name|dbi, key_data, key_size, new_key_data, new_key_size)

	tx:gen_id            (table_name|dbi) -> n     next sequence

CURSORS

	tx:cursor(table_name|dbi[, 'w']) -> cur
	cur:close()
	cur:closed() -> t|f
	tx:close_cursors()

	cur:next_raw         () -> key,key_size, val,val_size
	cur:current_raw      () -> key,key_size, val,val_size
	cur:[must_]get_raw   (key_data, key_size) -> val,val_size | nil
	cur:set_raw          (key_data, key_size, val_data, val_size)
	cur:del              ([flags])

	tx:each_raw(table_name[, 'w']) -> iter() -> cur, mdbx_key, mdbx_val

]]

require'glue'
require'fs'

require'mdbx_h'
local isnum = isnum
local C = ffi.load'mdbx'

mdbx = C

if config'mdbx_debug' then
	require'mdbx_debug'
	C = mdbx
end

--[[ STATELESS (UNSAFE) API --------------------------------------------------

	mdbx_env_open(file, [opt]) -> env
	mdbx_env_close(env)
	mdbx_env_delete(file_path, [flags])
	mdbx_env_get_maxkeysize(env) -> sz

	mdbx_txn_begin(env, ['r'|'w'], [parent_txn], [flags]) -> txn
	mdbx_txn_commit(txn)
	mdbx_txn_abort(txn)
	mdbx_txn_reset(txn)
	mdbx_txn_renew(txn)

	mdbx_dbi_open(txn, name|nil, create, [flags]) -> dbi, created
	mdbx_dbi_close(env, dbi)
	mdbx_dbi_rename(txn, dbi, new_table_name)
	mdbx_dbi_drop(txn, dbi, [clear_only])
	mdbx_dbi_stat(txn, dbi) -> stat

	mdbx_get(txn, dbi, key_data, key_size, val_data, val_buf_size) -> val_data, val_size
	mdbx_put(txn, dbi, key_data, key_size, val_data, val_size, [flags]) -> true
	mdbx_del(txn, dbi, key_data, key_size, [val_data, val_size], [flags])
	mdbx_dbi_sequence(txn, dbi, [inc=1]) -> n

	mdbx_cursor_open(txn, dbi) -> cur
	mdbx_cursor_bind(txn, cur, dbi)
	mdbx_cursor_unbind(cur)
	mdbx_cursor_get(cur, flags) -> k, k_sz, v, v_sz | nil, 0, nil, 0
	mdbx_cursor_get_key(k, k_sz) -> v, v_sz
	mdbx_cursor_set(cur, v, v_sz)
	mdbx_cursor_del(cur, [flags])

]]

local function try_checkz(rc)
	if rc == 0 then return true end
	return false, str(C.mdbx_strerror(rc))
end
local function checkz(rc)
	local ok, err = try_checkz(rc)
	if not ok then error(err, 3) end
end

--databases

local envp = new'MDBX_env*[1]'
local function mdbx_env_open(file, opt)

	opt = opt or empty

	if not opt.readonly then
		local ok, err = try_mkdirs(file)
		if not ok then return nil, err end
	end

	checkz(C.mdbx_env_create(envp))
	local env = envp[0]

	local size = 1024e4
	checkz(C.mdbx_env_set_geometry(env, size, size, size, -1, -1, -1))
	checkz(C.mdbx_env_set_option(env, C.MDBX_opt_max_readers, opt.max_readers or 4096))
	checkz(C.mdbx_env_set_option(env, C.MDBX_opt_max_db, opt.max_tables or 4096))

	local ok, err = try_checkz(C.mdbx_env_open(env, file,
		bor(C.MDBX_NOSUBDIR, opt.readonly and C.MDBX_RDONLY or 0, opt.flags or 0),
		(unixperms_parse(opt.file_mode or '0660'))
	))
	if not ok then
		checkz(C.mdbx_env_close_ex(env, 0))
		return nil, err
	end

	return env
end

local function mdbx_env_close(env)
	checkz(C.mdbx_env_close_ex(env, 0))
end

local function mdbx_env_delete(file, flags)
	local rc = C.mdbx_env_delete(file, flags or 0)
	if rc == C.MDBX_RESULT_TRUE then return nil, 'not_found' end
	checkz(rc)
	return true
end

local function mdbx_env_get_maxkeysize(env)
	local rc = C.mdbx_env_get_maxkeysize_ex(env, C.MDBX_DB_DEFAULTS)
	assert(rc ~= -1)
	return rc
end

--transactions

local txnp = new'MDBX_txn*[1]'
local function mdbx_txn_begin(env, mode, parent_txn, flags)
	mode = mode or 'r'
	assert(mode == 'r' or mode == 'w')
	flags = bor(mode == 'r' and C.MDBX_RDONLY or 0, flags or 0)
	checkz(C.mdbx_txn_begin_ex(env, parent_txn, flags, txnp, nil))
	return txnp[0]
end

local function mdbx_txn_commit(txn)
	checkz(C.mdbx_txn_commit_ex(txn, nil))
end

local function mdbx_txn_abort(txn)
	checkz(C.mdbx_txn_abort(txn))
end

local function mdbx_txn_reset(txn)
	checkz(C.mdbx_txn_reset(txn))
end

local function mdbx_txn_renew(txn)
	checkz(C.mdbx_txn_renew(txn))
end

--tables

--pass nil or false to `tab` arg to open the "main" dbi with the list of tables.
local dbip = new'MDBX_dbi[1]'
local function mdbx_dbi_open(txn, name, create, flags)
	flags = flags or 0
	local rc = C.mdbx_dbi_open(txn, name or nil, flags, dbip)
	if rc == C.MDBX_NOTFOUND then
		if create then
			flags = bor(flags, C.MDBX_CREATE)
			checkz(C.mdbx_dbi_open(txn, name or nil, flags, dbip))
			local dbi = dbip[0]
			return dbi, true
		else
			return nil, 'not_found'
		end
	else
		checkz(rc)
		local dbi = dbip[0]
		return dbi, false
	end
end

local function mdbx_dbi_close(env, dbi)
	checkz(C.mdbx_dbi_close(env, dbi))
end

local function mdbx_dbi_rename(txn, dbi, new_table_name)
	local rc = C.mdbx_dbi_rename(txn, dbi, new_table_name)
	if rc == C.MDBX_KEYEXIST then return nil, 'exists' end
	checkz(rc)
	return true
end

local function mdbx_dbi_drop(txn, dbi, clear_only)
	local rc = C.mdbx_drop(txn, dbi, clear_only and 0 or 1)
	if rc == C.MDBX_NOTFOUND then return nil, 'not_found' end
	checkz(rc)
	return true
end

local stat = new'MDBX_stat'
local stat_sz = sizeof(stat)
local function mdbx_dbi_stat(txn, dbi)
	checkz(C.mdbx_dbi_stat(txn, dbi, stat, stat_sz))
	return stat
end

--table data

local key = new'MDBX_val'
local val = new'MDBX_val'

local function mdbx_get(txn, dbi, key_data, key_size, val_data, val_size)
	key.data = key_data
	key.size = key_size
	local rc = C.mdbx_get(txn, dbi, key, val)
	if rc == 0 then return val.data, num(val.size) end
	if rc == C.MDBX_NOTFOUND then return nil, 'not_found' end
	checkz(rc) --always throws
end

local function mdbx_put(txn, dbi, key_data, key_size, val_data, val_size, flags)
	key.data = key_data
	key.size = key_size
	val.data = val_data
	val.size = val_size
	local rc = C.mdbx_put(txn, dbi, key, val, flags or 0)
	if rc == C.MDBX_KEYEXIST then return nil, 'exists', val.data, num(val.size) end
	if rc == C.MDBX_NOTFOUND then return nil, 'not_found' end
	checkz(rc)
	return true
end

local function mdbx_del(txn, dbi, key_data, key_size, val_data, val_size)
	key.data = key_data
	key.size = key_size
	local val = val
	if val_data then
		val.data = val_data
		val.size = val_size
	else
		val = nil
	end
	local rc = C.mdbx_del(txn, dbi, key, val)
	if rc == C.MDBX_NOTFOUND then return nil, 'not_found' end
	checkz(rc)
	return true
end

local seqbuf = u64a(1)
local function mdbx_dbi_sequence(txn, dbi, inc)
	checkz(C.mdbx_dbi_sequence(txn, dbi, seqbuf, inc or 1))
	return num(seqbuf[0])
end

--cursors

local curp = new'MDBX_cursor*[1]'
local function mdbx_cursor_open(txn, dbi)
	checkz(C.mdbx_cursor_open(txn, dbi, curp))
	return curp[0]
end
local function mdbx_cursor_bind(txn, cur, dbi)
	checkz(C.mdbx_cursor_bind(self.txn, cur, dbi))
end
local function mdbx_cursor_unbind(cur)
	checkz(C.mdbx_cursor_unbind(cur))
end
local function mdbx_cursor_get(cur, flags)
	local rc = C.mdbx_cursor_get(cur, key, val, flags)
	if rc == 0 then
		return
			key.data, num(key.size),
			val.data, num(val.size)
	end
	if rc == C.MDBX_NOTFOUND then
		return nil, 0, nil, 0
	end
	checkz(rc) --always throws
end
local function mdbx_cursor_get_key(k, k_sz)
	key.data = k
	key.size = k_sz
	local _, _, v, v_sz = mdbx_cursor_get(C.MDBX_SET_KEY)
	return v, v_sz
end
local function mdbx_cursor_set(cur, v, v_sz)
	checkz(C.mdbx_cursor_get(cur, key, val, C.MDBX_GET_CURRENT))
	val.data = v
	val.size = v_sz
	checkz(C.mdbx_cursor_put(cur, key, val, C.MDBX_CURRENT))
end
local function mdbx_cursor_del(cur, flags)
	checkz(C.mdbx_cursor_del(cur, flags or C.MDBX_CURRENT))
end

--stateful API ---------------------------------------------------------------

--databases

local Db = {}; mdbx_db = Db

local function env_wrap(env, file, opt)
	local db = object(Db, {
		file = file,
		env = env,
		dbis = {}, --{dbi->name, name->dbi}
		readonly = opt and opt.readonly,
		_free_ro_tx = {},
		_free_rw_tx = {},
		_free_cur = {},
	})
	live(db, file)
	return db
end
function try_mdbx_open(file, opt)
	local env, err = mdbx_env_open(file, opt)
	if not env then return nil, err end
	return env_wrap(env, file, opt)
end
function mdbx_open(file, opt)
	local env, err = mdbx_env_open(file, opt)
	if not env then check('db', 'open', false, '%s: %s', file, err) end
	return env_wrap(env, file, opt)
end

function Db:close()
	mdbx_env_close(self.env)
	live(db, nil)
	self.dbis = nil
	self.env = nil
end

function Db:db_max_key_size()
	local sz = mdbx_env_get_maxkeysize(self.env)
	function self:db_max_key_size()
		return sz
	end
	return sz
end

--transactions ---------------------------------------------------------------

local Tx = {}; mdbx_tx = Tx

function Db:tx(flags)
	local fl = self._free_ro_tx
	local tx = fl[#fl]
	if tx then
		mdbx_txn_renew(tx.txn)
		pop(fl)
	else
		local txn = mdbx_txn_begin(self.env, 'r', nil, flags)
		tx = object(Tx, {
			db = self, txn = txn, readonly = true,
			dbis = self.dbis,
		})
	end
	return tx
end
function Db:txw(parent_tx, flags)
	local txn = mdbx_txn_begin(self.env, 'w', parent_tx and parent_tx.txn, flags)
	local tx = pop(self._free_rw_tx) or object(Tx, {
			db = self,
			readonly = false,
			dbis = setmetatable({}, {}),
		})
	tx.txn = txn
	if parent_tx then
		tx.parent_tx = parent_tx
		parent_tx.child_tx = tx
	end
	getmetatable(tx.dbis).__index = (parent_tx or self).dbis
	return tx
end

function Tx:txw(flags)
	return self.db:txw(self, flags)
end

function Tx:closed()
	return self.txn == nil
end

function Tx:close_cursors()
	if not self.cursors then return end
	for _,cur in ipairs(self.cursors) do
		cur:close()
	end
end

local function tx_ro_free(self)
	self:close_cursors()
	push(self.db._free_ro_tx, self)
end

local function tx_rw_free(self)
	self:close_cursors()
	self.txn = nil
	if self.parent_tx then
		self.parent_tx.child_tx = nil
	end
	self.parent_tx = nil
	push(self.db._free_rw_tx, self)
	clear(self.dbis)
end

function Tx:commit()
	if self.readonly then
		mdbx_txn_reset(self.txn)
		tx_ro_free(self)
	else
		assert(not self.child_tx, 'commit while child transaction is open')
		mdbx_txn_commit(self.txn)
		--move dbis that were created in this tx to parent tx or to env if top tx.
		--dbis created in this tx are now scoped in the parent tx.
		--dbis dropped in this tx are now invalid in the parent tx.
		update((self.parent_tx or self.db).dbis, self.dbis)
		if not self.parent_tx then --close dbis that were dropped in this tx.
			for k,v in pairs(self.dbis) do
				if v == false then
				 	if isnum(k) then -- k,v is dbi,name
						mdbx_dbi_close(self.db.env, k)
					end
					self.dbis[k] = nil --k,v is dbi,name or name,dbi
				end
			end
		end
		tx_rw_free(self)
	end
end

function Tx:abort()
	if self.readonly then
		mdbx_txn_reset(self.txn)
		tx_ro_free(self)
	else
		--close child txs recursively bottom-up.
		if self.child_tx then
			self.child_tx:abort()
		end
		mdbx_txn_abort(self.txn)
		tx_rw_free(self)
	end
end

do
local function finish(tx, ok, ...)
	if ok then
		tx:commit()
		return ...
	else
		tx:abort()
		error(..., 2)
	end
end
function Db:atomic(mode, f, ...)
	if isfunc(mode) then mode, f = 'r', mode end
	local tx = mode == 'w' and self:txw() or self:tx()
	return finish(tx, xpcall(f, traceback, tx, ...))
end
function Tx:atomic(f, ...)
	local tx = self:txw()
	return finish(tx, xpcall(f, traceback, tx, ...))
end
end

--tables ---------------------------------------------------------------------

function Tx:table_name(tab)
	if not tab then return '<main>' end
	if isstr(tab) then return tab end
	return self.dbis[tab]
end

function Tx:try_open_table(tab, mode, flags)
	local dbi = isnum(tab) and tab or self.dbis[tab]
	if dbi then return dbi end
	local create = mode == 'w' or mode == 'c'
	local dbi, created = try_mdbx_dbi_open(self.txn, name, create, flags)
	if not dbi then return nil, created end
	--created tables are available in parent tx after commit, and in env only
	--after top tx commit. existing tables are available in env immediately.
	local dbis = created and self.dbis or self.db.dbis
	dbis[name or false] = dbi
	dbis[dbi] = name or false
	if mode == 'c' and not created then
		self:clear_table(name)
	end
	return dbi, created
end
function Tx:open_table(tab, mode, flags, ...)
	local dbi, err = self:try_open_table(tab, mode, flags, ...)
	return check('db', 'open_table', dbi, '%s %s: %s', tab, mode or 'r', err)
end

function Tx:dbi(tab, mode)
	if mode == 'w' or mode == 'c' then
		return self:open_table(tab, mode)
	else
		return self:try_open_table(tab)
	end
end

function Tx:try_rename_table(tab, new_table_name)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'not_found' end
	local ok, err = mdbx_dbi_rename(self.txn, dbi, new_table_name)
	if not ok then return nil, err end
	local old_table_name = self.dbis[dbi]
	self.dbis[old_table_name] = false
	self.dbis[dbi] = new_table_name
	return true
end
function Tx:rename_table(tab, new_table_name)
	local ok, err = self:try_rename_table(tab, new_table_name)
	return check('db', 'rename_table', ok, '%s -> %s: %s',
		self:table_name(tab), new_table_name, err)
end

function Tx:try_drop_table(tab)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'not_found' end
	local ok, err = try_mdbx_dbi_drop(self.txn, dbi)
	local name = self.dbis[dbi]
	self.dbis[dbi]  = false
	self.dbis[name] = false
	return true
end
function Tx:drop_table(tab)
	local ok, err = self:try_drop_table(tab)
	if ok then return end
	check('db', 'drop_table', false, '%s: %s', self:table_name(tab), err)
end

function Tx:try_clear_table(tab)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'not_found' end
	return mdbx_dbi_drop(self.txn, dbi, true)
end
function Tx:clear_table(tab)
	local ok, err = self:try_clear_table(tab)
	if ok then return end
	check('db', 'clear_table', false, '%s: %s', self:table_name(tab), err)
end

function Tx:create_table(tbl_name, ...)
	self:open_table(tbl_name, 'c', ...)
end

function Tx:entries(tab)
	return num(self:stat(tab).entries)
end

local function next_table(self)
	local k, k_sz = self:next_raw()
	return k and str(k, k_sz)
end
function Tx:each_table()
	local cur = self:cursor()
	return next_table, cur
end
function Tx:table_count()
	return self:entries()
end
function Tx:table_exists(table_name)
	if not table_name then return true end --main table always exists.
	if self.dbis[table_name] then return true end --exists in current tx
	return self:get_raw(nil, cast(u8p, table_name), #table_name) ~= nil
end

function Tx:try_stat(tab)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'table_not_found' end
	return mdbx_dbi_stat(self.txn, dbi)
end
function Tx:stat(tab)
	local stat, err = self:try_stat(tab)
	if stat then return stat end
	check('db', 'stat', false, '%s: %s', self:table_name(tab), err)
end

--table data -----------------------------------------------------------------

function Tx:get_raw(tab, key_data, key_size, val_data, val_size)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'table_not_found' end
	return mdbx_get(self.txn, dbi, key_data, key_size, val_data, val_size)
end
function Tx:must_get_raw(...)
	return assert(self:get_raw(...))
end

function Tx:try_put_raw(tab, key_data, key_size, val_data, val_size, flags)
	local dbi = isnum(tab) and tab or self:dbi(tab, 'w')
	return mdbx_put(self.txn, dbi, key_data, key_size, val_data, val_size, flags)
end

function Tx:try_insert_raw(tab, key_data, key_size, val_data, val_size, flags)
	return self:try_put_raw(tab, key_data, key_size, val_data, val_size,
		bor(flags or 0, C.MDBX_NOOVERWRITE))
end

function Tx:try_update_raw(tab, key_data, key_size, val_data, val_size, flags)
	return self:try_put_raw(tab, key_data, key_size, val_data, val_size,
		bor(flags or 0, C.MDBX_CURRENT))
end

function Tx:put_raw(tab, ...)
	local ret, err = self:try_put_raw(tab, ...)
	if ret then return ret end
	check('db', 'put_raw', ret, '%s: %s', self:table_name(tab), err)
end
function Tx:insert_raw(tab, ...)
	local ret, err = self:try_insert_raw(tab, ...)
	if ret then return ret end
	check('db', 'insert_raw', ret, '%s: %s', self:table_name(tab), err)
end
function Tx:update_raw(tab, ...)
	local ret, err = self:try_update_raw(tab, ...)
	if ret then return end
	check('db', 'update_raw', ret, '%s: %s', self:table_name(tab), err)
end

function Tx:try_del_raw(tab, key_data, key_size, val_data, val_size)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'table_not_found' end
	return mdbx_del(self.txn, dbi, key_data, key_size, val_data, val_size)
end
function Tx:del_raw(tab, ...)
	local ret, err = self:try_del_raw(tab, ...)
	if ret then return end
	check('db', 'del_raw', ret, '%s: %s', self:table_name(tab), err)
end

local seqbuf = u64a(1)
function Tx:gen_id(tab)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	assertf(dbi, 'gen_id(): table not found: %s', tab)
	return mdbx_dbi_sequence(self.txn, dbi, seqbuf)
end

function Tx:try_move_key_raw(tab, k1, k1_sz, k2, k2_sz)
	local v, v_sz = self:get_raw(tab, k1, k1_sz)
	if not v then return nil, v_sz end
	--NOTE: calling put before del because del invaldates the v pointer.
	local ok, err = self:try_insert_raw(tab, k2, k2_sz, v, v_sz)
	if not ok and err == 'exists' then return nil, err end
	self:del_raw(tab, k1, k1_sz)
	return true
end
function Tx:move_key_raw(tab, ...)
	local ret, err = self:try_move_key_raw(tab, ...)
	if ret then return end
	check('db', 'move_key_raw', ret, '%s: %s', self:table_name(tab), err)
end

--cursors --------------------------------------------------------------------

local Cur = {}; mdbx_cur = Cur

function Tx:cursor(tab, mode)
	local dbi = isnum(tab) and tab or self:dbi(tab, mode)
	if not dbi then return nil, 'not_found' end
	local cur = pop(self.db._free_cur)
	if cur then
		mdbx_cursor_bind(self.txn, cur.mdbx_cursor, dbi)
		cur.tx = self
	else
		cur = object(Cur, {tx = self})
		cur.mdbx_cursor = mdbx_cursor_open(self.txn, dbi)
	end
	add(attr(self, 'cursors'), cur)
	return cur
end

function Cur:closed()
	return self.tx == nil
end

function Cur:get_mdbx_cursor()
	assert(self.tx, 'closed')
	return self.mdbx_cursor
end

function Cur:close()
	if self:closed() then return end
	mdbx_cursor_unbind(self.mdbx_cursor)
	push(self.tx.db._free_cur, self)
	self.tx = nil
end

function Cur:next_raw()
	return mdbx_cursor_get(self:get_mdbx_cursor(), C.MDBX_NEXT)
end
function Cur:current_raw()
	return mdbx_cursor_get(self:get_mdbx_cursor(), C.MDBX_GET_CURRENT)
end
function Cur:get_raw(k, k_sz)
	return mdbx_cursor_get_key(self:get_mdbx_cursor(), k, k_sz)
end
function Cur:must_get_raw(k, k_sz)
	local v, v_sz = mdbx_cursor_get_key(self:get_mdbx_cursor(), k, k_sz)
	assert(v, 'not_found')
	return v, v_sz
end

function Cur:set_raw(v, v_sz)
	assert(not self:closed(), 'closed')
	mdbx_cursor_set(self:get_mdbx_cursor(), v, v_sz)
end

function Cur:del(flags)
	mdbx_cursor_del(self:get_mdbx_cursor())
end

local function each_raw_next(self)
	local k, k_sz, v, v_sz = self:next_raw()
	if not k then
		self:close()
		return
	end
	return self, k, k_sz, v, v_sz
end
function Tx:each_raw(tab, mode)
	local cur = self:cursor(tab, mode)
	if not cur then return noop end
	return each_raw_next, cur
end

-- test ----------------------------------------------------------------------

if not ... then

local function self_test()
	local db = mdbx_open('testdb')

	local tx = db:txw()
	tx:open_table('users', 'w')
	tx:commit()

	local tx = db:txw()
	s = _('%03x %d foo bar', 32, 3141592)
	tx:put_raw('users', new('int[1]', 123456789), 4, cast(u8p, s), #s)
	tx:commit()

	local tx = db:tx()
	for cur,k,k_sz,v,v_sz in tx:each_raw'users' do
		printf('key: %s %s, data: %s %s\n',
			k_sz, cast('int*', k)[0],
			v_sz, str(v, v_sz))
	end
	tx:abort()

	db:close()
end

local function test_dbi_semantics()
	assert(mdbx_env_delete'testdb')
	local env = assert(mdbx_env_open('testdb'))
	local txn = mdbx_txn_begin(env, 'w')
	local dbi, created = assert(mdbx_dbi_open(txn, 'test', true))
	assert(created)
	local txn0 = txn
	local txn = mdbx_txn_begin(env, 'w', txn)
	mdbx_dbi_drop(txn, dbi)
	mdbx_txn_abort(txn)
	--mdbx_txn_commit(txn)
	txn = txn0
	pr(dbi)
	local dbi, created = assert(mdbx_dbi_open(txn, 'test', true))
	pr(dbi)
	assert(not created)
	pr(mdbx_dbi_stat(txn, dbi))
	pr(env, txn, dbi, created)
	--mdbx_txn_commit(txn0)
	mdbx_env_close(env)
end

test_dbi_semantics()
--self_test()

end
