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

local function try_checkz(rc)
	if rc == 0 then return true end
	return false, str(C.mdbx_strerror(rc))
end
local function checkz(rc)
	local ok, err = try_checkz(rc)
	if not ok then error(err, 3) end
end

--databases ------------------------------------------------------------------

local Db = {}; mdbx_db = Db

function Db:close()
	checkz(C.mdbx_env_close_ex(self.env, 0))
	self.dbis = nil
	self.table_names = nil
	self.env = nil
end

--opt.readonly
--opt.file_mode
do
local envp = new'MDBX_env*[1]'
function try_mdbx_open(file, opt)

	local ok, err = try_mkdirs(file)
	if not ok then return nil, err end

	opt = opt or empty

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
	log(opt.readonly and '' or 'note', 'db',
		'open', '%s (%s)', file, opt.readonly and 'r/o' or 'r/w')

	local db = object(Db, {
		file = file,
		env = env,
		open_tables = {}, --{dbi->name, name->dbi}
		readonly = opt.readonly,
		_free_ro_tx = {},
		_free_rw_tx = {},
		_free_cur = {},
	})

	return db
end
end
function mdbx_open(file, opt, ...)
	local db, err = try_mdbx_open(file, opt, ...)
	return check('db', 'open', db, '%s (%s): %s',
		file, opt and opt.readonly and 'r/o' or 'r/w', err)
end

function mdbx_drop(file, mode)
	checkz(C.mdbx_env_delete(file, mode))
end

function Db:db_max_key_size()
	local sz = C.mdbx_env_get_maxkeysize_ex(self.env, C.MDBX_DB_DEFAULTS)
	function self:db_max_key_size()
		return sz
	end
	return sz
end

--transactions ---------------------------------------------------------------

local Tx = {}; mdbx_tx = Tx

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
end

do
local txnp = new'MDBX_txn*[1]'
function Db:tx(flags)
	local fl = self._free_ro_tx
	local tx = fl[#fl]
	if tx then
		checkz(C.mdbx_txn_renew(tx.txn))
		pop(fl)
	else
		flags = bor(C.MDBX_RDONLY, flags or 0)
		checkz(C.mdbx_txn_begin_ex(self.env, nil, flags, txnp, nil))
		tx = object(Tx, {
			db = self, txn = txnp[0], readonly = true,
			open_tables = setmetatable({}, {__index = self.open_tables}),
		})
	end
	return tx
end
function Db:txw(parent_tx, flags)
	flags = flags or 0
	checkz(C.mdbx_txn_begin_ex(self.env, parent_tx and parent_tx.txn, flags, txnp, nil))
	local tx = pop(self._free_rw_tx) or object(Tx, {
			db = self,
			readonly = false,
			open_tables = setmetatable({}, {}),
		})
	end
	tx.txn = txnp[0]
	if parent_tx then
		tx.parent_tx = parent_tx
		parent_tx.child_tx = tx
	end
	getmetatable(tx.open_tables).__index = (parent_tx or self).open_tables
	return tx
end
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

function Tx:commit()
	assert(not self:closed(), 'transaction closed')
	if self.readonly then
		checkz(C.mdbx_txn_reset(self.txn))
		tx_ro_free(self)
	else
		assert(not self.child_tx, 'commit while child transaction is open')
		checkz(C.mdbx_txn_commit_ex(self.txn, nil))
		tx_rw_free(self)
	end
end

function Tx:close_all_dbis()
	for dbi in pairs(self.db.open_tables) do
		if isnum(dbi) then
			checkz(C.mdbx_dbi_close(self.db.env, dbi))
		end
	end
	clear(self.db.open_tables)
end

function Tx:abort()
	assert(not self:closed(), 'transaction closed')
	if self.readonly then
		checkz(C.mdbx_txn_reset(self.txn))
		tx_ro_free(self)
	else
		--close child txs recursively bottom-up.
		if self.child_tx then
			self.child_tx:abort()
		end
		checkz(C.mdbx_txn_abort(self.txn))
		--dbis of tables that were created in this transactions are invalid now.
		--close all dbis since we're not tracking dbis per transaction otherwise
		--we'd only close the ones that actually need closing.
		self:close_all_dbis()
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
	return self.db.open_tables[tab]
end
local table_name = Tx.table_name

--pass nil or false to `tab` arg to open the "main" dbi with the list of tables.
do
local dbip = new'MDBX_dbi[1]'
function Tx:try_open_table(tab, mode, flags)
	local dbi = isnum(tab) and tab or self.db.open_tables[tab or false]
	if dbi then return dbi end
	local name = tab
	local create = mode == 'w' or mode == 'c'
	flags = bor(flags or 0, create and C.MDBX_CREATE or 0)
	local created = create and not self:table_exists(name)
	local rc = C.mdbx_dbi_open(self.txn, name, flags, dbip)
	if not create and rc == C.MDBX_NOTFOUND then
		return nil, 'not_found'
	end
	checkz(rc)
	local dbi = dbip[0]
	self.db.open_tables[name or false] = dbi
	self.db.open_tables[dbi] = name or false
	if mode == 'c' and not created then
		self:clear_table(name)
	end
	return dbi, created
end
end
function Tx:open_table(tab, mode, flags, ...)
	local t, err = self:try_open_table(tab, mode, flags, ...)
	return check('db', 'open_table', t, '%s%s%s: %s',
		tab, mode and ' ' or '', mode or '', err)
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
	if self:table_exists(new_table_name) then return nil, 'exists' end
	checkz(C.mdbx_dbi_rename(self.txn, dbi, new_table_name))
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
	checkz(C.mdbx_drop(self.txn, dbi, 1))
	checkz(C.mdbx_dbi_close(self.db.env, dbi))
	local name = self.db.open_tables[dbi]
	self.db.open_tables[dbi] = nil
	self.db.open_tables[name] = nil
	return true
end
function Tx:drop_table(tab)
	local ok, err = self:drop_table(tab)
	if ok then return end
	check('db', 'drop_table', false, '%s: %s', table_name(self, tab), err)
end

function Tx:try_clear_table(tab)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'not_found' end
	checkz(C.mdbx_drop(self.txn, dbi, 0))
	return true
end
function Tx:clear_table(tab)
	local ok, err = self:try_clear_table(tab)
	if ok then return end
	check('db', 'clear_table', false, '%s: %s', table_name(self, tab), err)
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
	return self:get_raw(nil, cast(u8p, table_name), #table_name) ~= nil
end

do
local stat = new'MDBX_stat'
local stat_sz = sizeof(stat)
function Tx:try_stat(tab)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'table_not_found' end
	checkz(C.mdbx_dbi_stat(self.txn, dbi, stat, stat_sz))
	return stat
end
function Tx:stat(tab)
	local ret, err = self:try_stat(tab)
	if ret then return ret end
	check('db', 'stat', false, '%s: %s', table_name(self, tab), err)
end
end

--table data -----------------------------------------------------------------

local key = new'MDBX_val'
local val = new'MDBX_val'

function Tx:get_raw(tab, key_data, key_size, val_data, val_size)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'table_not_found' end
	key.data = key_data
	key.size = key_size
	local rc = C.mdbx_get(self.txn, dbi, key, val)
	if rc == 0 then return val.data, num(val.size) end
	if rc == C.MDBX_NOTFOUND then return nil, 'not_found' end
	checkz(rc)
end
function Tx:must_get_raw(...)
	return assert(self:get_raw(...))
end

function Tx:try_put_raw(tab, key_data, key_size, val_data, val_size, flags)
	local dbi = isnum(tab) and tab or self:dbi(tab, 'w')
	key.data = key_data
	key.size = key_size
	val.data = val_data
	val.size = val_size
	local rc = C.mdbx_put(self.txn, dbi, key, val, flags or 0)
	if rc == C.MDBX_KEYEXIST then return nil, 'exists', val.data, num(val.size) end
	if rc == C.MDBX_NOTFOUND then return nil, 'not_found' end
	checkz(rc)
	return true
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
	check('db', 'put_raw', ret, '%s: %s', table_name(self, tab), err)
end
function Tx:insert_raw(tab, ...)
	local ret, err = self:try_insert_raw(tab, ...)
	if ret then return ret end
	check('db', 'insert_raw', ret, '%s: %s', table_name(self, tab), err)
end
function Tx:update_raw(tab, ...)
	local ret, err = self:try_update_raw(tab, ...)
	if ret then return end
	check('db', 'update_raw', ret, '%s: %s', table_name(self, tab), err)
end

function Tx:try_del_raw(tab, key_data, key_size, val_data, val_size)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	if not dbi then return nil, 'table_not_found' end
	key.data = key_data
	key.size = key_size
	local val = val
	if val_data then
		val.data = val_data
		val.size = val_size
	else
		val = nil
	end
	local rc = C.mdbx_del(self.txn, dbi, key, val)
	if rc == C.MDBX_NOTFOUND then return nil, 'not_found' end
	checkz(rc)
	return true
end
function Tx:del_raw(tab, ...)
	local ret, err = self:try_del_raw(tab, ...)
	if ret then return end
	check('db', 'del_raw', ret, '%s: %s', table_name(self, tab), err)
end

local seqbuf = u64a(1)
function Tx:gen_id(tab)
	local dbi = isnum(tab) and tab or self:dbi(tab)
	assertf(dbi, 'gen_id(): table not found: %s', tab)
	checkz(C.mdbx_dbi_sequence(self.txn, dbi, seqbuf, 1))
	return num(seqbuf[0])
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
	check('db', 'move_key_raw', ret, '%s: %s', table_name(self, tab), err)
end

--cursors --------------------------------------------------------------------

local Cur = {}; mdbx_cur = Cur

local curp = new'MDBX_cursor*[1]'
function Tx:cursor(tab, mode)
	local dbi = isnum(tab) and tab or self:dbi(tab, mode)
	if not dbi then return nil, 'not_found' end
	local cur = pop(self.db._free_cur)
	if cur then
		checkz(C.mdbx_cursor_bind(self.txn, cur.mdbx_cursor, dbi))
		cur.tx = self
	else
		cur = object(Cur, {tx = self})
		checkz(C.mdbx_cursor_open(self.txn, dbi, curp))
		cur.mdbx_cursor = curp[0]
	end
	add(attr(self, 'cursors'), cur)
	return cur
end

function Cur:closed()
	return self.tx == nil
end

function Cur:close()
	if self:closed() then return end
	checkz(C.mdbx_cursor_unbind(self.mdbx_cursor))
	push(self.tx.db._free_cur, self)
	self.tx = nil
end

function Cur:_get_raw(flags)
	assert(not self:closed(), 'closed')
	local rc = C.mdbx_cursor_get(self.mdbx_cursor, key, val, flags)
	if rc == 0 then
		return
			key.data, num(key.size),
			val.data, num(val.size)
	end
	if rc == C.MDBX_NOTFOUND then
		return nil, 0, nil, 0
	end
	checkz(rc)
end
function Cur:next_raw()
	return self:_get_raw(C.MDBX_NEXT)
end
function Cur:current_raw()
	return self:_get_raw(C.MDBX_GET_CURRENT)
end
function Cur:get_raw(k, k_sz)
	key.data = k
	key.size = k_sz
	local _, _, v, v_sz = self:_get_raw(C.MDBX_SET_KEY)
	return v, v_sz
end
function Cur:must_get_raw(k, k_sz)
	key.data = k
	key.size = k_sz
	local _, _, v, v_sz = self:_get_raw(C.MDBX_SET_KEY)
	assert(v, 'not_found')
	return v, v_sz
end

function Cur:set_raw(v, v_sz)
	assert(not self:closed(), 'closed')
	checkz(C.mdbx_cursor_get(self.mdbx_cursor, key, val, C.MDBX_GET_CURRENT))
	val.data = v
	val.size = v_sz
	checkz(C.mdbx_cursor_put(self.mdbx_cursor, key, val, C.MDBX_CURRENT))
end

function Cur:del(flags)
	checkz(C.mdbx_cursor_del(self.mdbx_cursor, flags or C.MDBX_CURRENT))
end

local function each_raw_next(self)
	local k, k_sz, v, v_sz = self:_get_raw(C.MDBX_NEXT)
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
