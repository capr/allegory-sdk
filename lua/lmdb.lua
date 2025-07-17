--go@ ssh -ic:\users\cosmin\.ssh\id_ed25519 root@10.0.0.8 ~/sdk/bin/linux/luajit sdk/lua/lmdb.lua
--go@ c:\tools\plink.exe -i c:\users\woods\.ssh\id_ed25519.ppk root@172.20.10.9 ~/sdk/bin/debian12/luajit sdk/tests/lmdb_test.lua
--[[

	LMDB binding.
	Written by Cosmin Apreutsei. Public Domain.

	LMDB is a super-fast mmap-based MVCC key-value store in 12 KLOC of C.
	LMDB offers ACID with serializable semantics, good for read-heavy loads.
	TODO: LMDB alternatives: libmdbx (30 KLOC), bbolt (Golang).

	LMDB will blow up in your face if:
	- you write too much data in a single write transaction, which blows up the
	  freelist of free pages. libmdbx solves this.
	- you stay too long in a read-only transaction, which prevents writers from
	  reclaiming free pages, which blows up the DB while this happens.
	- you stay too long in a write transaction which blocks all other write
	  transactions.

]]

require'glue'
require'ffi'
local isnum = isnum
local C = ffi.load'lmdb'

-- mdb_env Environment Flags
MDB_FIXEDMAP     =      0x01
MDB_NOSUBDIR     =    0x4000
MDB_NOSYNC       =   0x10000
MDB_RDONLY       =   0x20000
MDB_NOMETASYNC   =   0x40000
MDB_WRITEMAP     =   0x80000
MDB_MAPASYNC     =  0x100000
MDB_NOTLS        =  0x200000
MDB_NOLOCK       =  0x400000
MDB_NORDAHEAD    =  0x800000
MDB_NOMEMINIT    = 0x1000000
MDB_PREVSNAPSHOT = 0x2000000

--mdb_dbi_open Database Flags
MDB_REVERSEKEY   =    0x02
MDB_DUPSORT      =    0x04
MDB_INTEGERKEY   =    0x08
MDB_DUPFIXED     =    0x10
MDB_INTEGERDUP   =    0x20
MDB_REVERSEDUP   =    0x40
MDB_CREATE       = 0x40000

-- mdb_put Write Flags
MDB_NOOVERWRITE  =    0x10
MDB_NODUPDATA    =    0x20
MDB_CURRENT      =    0x40
MDB_RESERVE      = 0x10000
MDB_APPEND       = 0x20000
MDB_APPENDDUP    = 0x40000
MDB_MULTIPLE     = 0x80000

-- mdb_copy Copy Flags
MDB_CP_COMPACT = 0x01

--cursor ops
MDB_FIRST          =  0
MDB_FIRST_DUP      =  1
MDB_GET_BOTH       =  2
MDB_GET_BOTH_RANGE =  3
MDB_GET_CURRENT    =  4
MDB_GET_MULTIPLE   =  5
MDB_LAST           =  6
MDB_LAST_DUP       =  7
MDB_NEXT           =  8
MDB_NEXT_DUP       =  9
MDB_NEXT_MULTIPLE  = 10
MDB_NEXT_NODUP     = 11
MDB_PREV           = 12
MDB_PREV_DUP       = 13
MDB_PREV_NODUP     = 14
MDB_SET            = 15
MDB_SET_KEY        = 16
MDB_SET_RANGE      = 17
MDB_PREV_MULTIPLE  = 18

cdef[[
typedef unsigned int mode_t;
typedef mode_t mdb_mode_t;
typedef size_t mdb_size_t;
typedef int mdb_filehandle_t;

typedef struct MDB_env MDB_env;
typedef struct MDB_txn MDB_txn;
typedef unsigned int	MDB_dbi;
typedef struct MDB_cursor MDB_cursor;

typedef struct MDB_val {
	size_t  size;
	void   *data;
} MDB_val;

typedef int (MDB_cmp_func)(const MDB_val *a, const MDB_val *b);

typedef struct MDB_stat {
	unsigned int	ms_psize;
	unsigned int	ms_depth;
	mdb_size_t		ms_branch_pages;
	mdb_size_t		ms_leaf_pages;
	mdb_size_t		ms_overflow_pages;
	mdb_size_t		ms_entries;
} MDB_stat;

typedef struct MDB_envinfo {
	void	*me_mapaddr;
	mdb_size_t	me_mapsize;
	mdb_size_t	me_last_pgno;
	mdb_size_t	me_last_txnid;
	unsigned int me_maxreaders;
	unsigned int me_numreaders;
} MDB_envinfo;

char *mdb_version(int *major, int *minor, int *patch);
char *mdb_strerror(int err);

int  mdb_env_create(MDB_env **env);
int  mdb_env_open(MDB_env *env, const char *path, unsigned int flags, mdb_mode_t mode);
int  mdb_env_copy(MDB_env *env, const char *path);
int  mdb_env_copyfd(MDB_env *env, mdb_filehandle_t fd);
int  mdb_env_copy2(MDB_env *env, const char *path, unsigned int flags);
int  mdb_env_copyfd2(MDB_env *env, mdb_filehandle_t fd, unsigned int flags);
int  mdb_env_stat(MDB_env *env, MDB_stat *stat);
int  mdb_env_info(MDB_env *env, MDB_envinfo *stat);
int  mdb_env_sync(MDB_env *env, int force);
void mdb_env_close(MDB_env *env);
int  mdb_env_set_flags(MDB_env *env, unsigned int flags, int onoff);
int  mdb_env_get_flags(MDB_env *env, unsigned int *flags);
int  mdb_env_get_path(MDB_env *env, const char **path);
int  mdb_env_get_fd(MDB_env *env, mdb_filehandle_t *fd);
int  mdb_env_set_mapsize(MDB_env *env, mdb_size_t size);
int  mdb_env_set_maxreaders(MDB_env *env, unsigned int readers);
int  mdb_env_get_maxreaders(MDB_env *env, unsigned int *readers);
int  mdb_env_set_maxdbs(MDB_env *env, MDB_dbi dbs);
int  mdb_env_get_maxkeysize(MDB_env *env);
int  mdb_env_set_userctx(MDB_env *env, void *ctx);
void *mdb_env_get_userctx(MDB_env *env);

typedef void MDB_assert_func(MDB_env *env, const char *msg);
int  mdb_env_set_assert(MDB_env *env, MDB_assert_func *func);

int  mdb_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn);
MDB_env *mdb_txn_env(MDB_txn *txn);
mdb_size_t mdb_txn_id(MDB_txn *txn);
int  mdb_txn_commit(MDB_txn *txn);
void mdb_txn_abort(MDB_txn *txn);
void mdb_txn_reset(MDB_txn *txn);
int  mdb_txn_renew(MDB_txn *txn);

int  mdb_dbi_open(MDB_txn *txn, const char *name, unsigned int flags, MDB_dbi *dbi);
int  mdb_stat(MDB_txn *txn, MDB_dbi dbi, MDB_stat *stat);
int  mdb_dbi_flags(MDB_txn *txn, MDB_dbi dbi, unsigned int *flags);
void mdb_dbi_close(MDB_env *env, MDB_dbi dbi);
int  mdb_drop(MDB_txn *txn, MDB_dbi dbi, int del);

int  mdb_set_compare(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func *cmp);
int  mdb_set_dupsort(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func *cmp);
int  mdb_set_relctx(MDB_txn *txn, MDB_dbi dbi, void *ctx);
int  mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
int  mdb_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags);
int  mdb_del(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);

int  mdb_cursor_open(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor);
void mdb_cursor_close(MDB_cursor *cursor);
int  mdb_cursor_renew(MDB_txn *txn, MDB_cursor *cursor);
MDB_txn *mdb_cursor_txn(MDB_cursor *cursor);
MDB_dbi mdb_cursor_dbi(MDB_cursor *cursor);
int  mdb_cursor_get(MDB_cursor *cursor, MDB_val *key, MDB_val *data, int op);
int  mdb_cursor_put(MDB_cursor *cursor, MDB_val *key, MDB_val *data, unsigned int flags);
int  mdb_cursor_del(MDB_cursor *cursor, unsigned int flags);
int  mdb_cursor_count(MDB_cursor *cursor, mdb_size_t *countp);

int  mdb_cmp(MDB_txn *txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b);
int  mdb_dcmp(MDB_txn *txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b);

typedef int (MDB_msg_func)(const char *msg, void *ctx);
int	mdb_reader_list(MDB_env *env, MDB_msg_func *func, void *ctx);
int	mdb_reader_check(MDB_env *env, int *dead);
]]

require'fs'
require'sock'

lmdb = {}

local function check(rc)
	if rc == 0 then return end
	error(str(C.mdb_strerror(rc)), 2)
end

local Db = {}; lmdb.Db = Db

function Db:close()
	for table_name,dbi in pairs(self.dbis) do
		C.mdb_dbi_close(self.env, dbi)
	end
	C.mdb_env_close(self.env)
	self.schema = nil
	self.dbis = nil
	self.env = nil
end

--opt.readonly
--opt.file_mode
function lmdb.open(dir, opt)
	opt = opt or {}

	local env = new'MDB_env*[1]'
	check(C.mdb_env_create(env)); env = env[0]
	check(C.mdb_env_set_mapsize(env, 1024e4))
	check(C.mdb_env_set_maxreaders(env, opt.max_readers or 1024))
	check(C.mdb_env_set_maxdbs(env, opt.max_dbs or 1024))
	check(C.mdb_env_open(env, dir,
		opt.readonly and MDB_RDONLY or 0,
		(unixperms_parse(opt.file_mode or '0660'))
	))

	local dbis = {}
	local db = object(Db, {
		dir = dir,
		env = env,
		dbis = dbis,
		readonly = opt.readonly,
		_free_tx = {},
		_free_cur = {},
	})

	return db
end

function Db:open_tables(tables)
	local dbi = new'MDB_dbi[1]'
	local tx = self:tx'w'
	local iter = isstr(tables) and words or pairs
	for table_name in iter(tables) do
		check(C.mdb_dbi_open(tx.txn[0], table_name, MDB_CREATE, dbi))
		local dbi = dbi[0]
		self.dbis[table_name] = dbi
	end
	tx:commit()
end

function Db:dbi(table_name)
	return assertf(self.dbis[table_name], 'table not found: %s', table_name)
end

function Db:db_max_key_size()
	return C.mdb_env_get_maxkeysize(self.env)
end

local Tx = {}; lmdb.Tx = Tx

function Db:tx(flags, parent_tx)
	flags = flags or (parent_tx and not parent_tx:is_readonly() and 'w') or 'r'
	if flags == 'r' then flags = MDB_RDONLY end
	if flags == 'w' then flags = 0 end
	local tx = pop(self._free_tx) or object(Tx, {
		db = self,
		txn = new'MDB_txn*[1]',
	})
	check(C.mdb_txn_begin(self.env, parent_tx and parent_tx.txn[0], flags, tx.txn))
	if parent_tx then
		self.parent_tx = parent_tx
		add(attr(parent_tx, 'child_txs'), self)
	end
	tx.flags = flags
	return tx
end

function Tx:is_readonly()
	return band(self.flags, MDB_RDONLY) ~= 0
end

function Tx:tx()
	return self.db:tx('w', self)
end

function Tx:closed()
	return self.txn[0] == nil
end

function Tx:close_cursors()
	if not self.cursors then return end
	for cur in pairs(self.cursors) do
		cur:close()
	end
end

function Tx:commit()
	assert(not self:closed(), 'transaction closed')
	self:close_cursors()
	check(C.mdb_txn_commit(self.txn[0]))
	self.txn[0] = nil
	push(self.db._free_tx, self)
	if self.parent_tx then --child takes place of parent.
		self.parent_tx.txn[0] = nil
		push(self.db._free_tx, self.parent_tx)
		self.parent_tx = nil
	end
end

function Tx:abort()
	assert(not self:closed(), 'transaction closed')
	self:close_cursors()
	C.mdb_txn_abort(self.txn[0])
	self.txn[0] = nil
	push(self.db._free_tx, self)
	if self.child_txs then --child txs are aborted automatically.
		for _,tx in ipairs(self.child_txs) do
			tx.txn[0] = nil
			push(self.db._free_tx, tx)
			tx.parent_tx = nil
		end
		self.child_txs = nil
	end
end

do
local key = new'MDB_val'
local val = new'MDB_val'
function Tx:get(tab)
	check(C.mdb_get(self.txn[0], isnum(tab) and tab or self.db:dbi(tab), key, val, 0))
	return key, val
end
end

function Tx:put_kv(tab, key, val, flags)
	check(C.mdb_put(self.txn[0], isnum(tab) and tab or self.db:dbi(tab), key, val, flags or 0))
end

do
local key = new'MDB_val'
local val = new'MDB_val'
function Tx:put(tab, key_data, key_size, val_data, val_size, flags)
	key.data = key_data
	key.size = key_size
	val.data = val_data
	val.size = val_size
	self:put_kv(tab, key, val, flags)
end
end

local Cur = {}
function Tx:cursor(tab)
	local cur = pop(self.db._free_cur)
	if cur then
		cur.tx = self
	else
		cur = object(Cur, {tx = self, cursor = new'MDB_cursor*[1]'})
	end
	check(C.mdb_cursor_open(self.txn[0], isnum(tab) and tab or self.db:dbi(tab), cur.cursor))
	attr(self, 'cursors')[cur] = true
	return cur
end

function Cur:closed()
	return self.cursor[0] == nil
end

function Cur:close()
	if self:closed() then return end
	C.mdb_cursor_close(self.cursor[0])
	self.cursor[0] = nil
	self.tx.cursors[self] = nil
	push(self.tx.db._free_cur, self)
end

do
local key = new'MDB_val'
local val = new'MDB_val'
local MDB_NOTFOUND = -30798
function Cur:next()
	if self:closed() then return end
	local rc = C.mdb_cursor_get(self.cursor[0], key, val, MDB_NEXT)
	if rc == 0 then return key, val end
	if rc == MDB_NOTFOUND then
		self:close()
		return
	end
	check(rc)
end
end

function Tx:each(tab)
	local cur = self:cursor(tab)
	return Cur.next, cur
end

-- test ----------------------------------------------------------------------

if not ... then

	local db = lmdb.open('testdb')

	db:open_tables'users'

	s = _('%03x %d foo bar', 32, 3141592)

	local tx = db:tx'w'
	tx:put('users', new('int[1]', 123456789), sizeof'int', cast('char*', s), #s)
	tx:commit()

	local tx = db:tx()
	for k,v in tx:each'users' do
		printf('key: %s %s, data: %s %s\n',
			k.size, cast('int*', k.data)[0],
			v.size, str(v.data, v.size))
	end
	tx:abort()

	db:close()
end
