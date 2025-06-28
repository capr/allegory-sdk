--go@ ssh -ic:\users\cosmin\.ssh\id_ed25519 root@10.0.0.8 ~/sdk/bin/linux/luajit sdk/tests/lmdb_test.lua
--go@ c:\tools\plink.exe -i c:\users\woods\.ssh\id_ed25519.ppk root@172.20.10.9 ~/sdk/bin/debian12/luajit sdk/tests/lmdb_test.lua
--[[

DATA TYPES:
	f64 (enc/dec + native asc/desc)
	u32 i32 u64 i64 (native enc/dec + native asc/desc)
	str (MAXN) (16 + data + optional padding; utf8 lowercase for CI indexing)

COMPOSITE KEYS:
	- need to invert components for asc/desc (invert all bytes)
	- need to invert offsets

]]
require'glue'
require'ffi'
local isnum = isnum
local C = ffi.load'lmdb'

-- mdb_env Environment Flags
local MDB_FIXEDMAP     =      0x01
local MDB_NOSUBDIR     =    0x4000
local MDB_NOSYNC       =   0x10000
local MDB_RDONLY       =   0x20000
local MDB_NOMETASYNC   =   0x40000
local MDB_WRITEMAP     =   0x80000
local MDB_MAPASYNC     =  0x100000
local MDB_NOTLS        =  0x200000
local MDB_NOLOCK       =  0x400000
local MDB_NORDAHEAD    =  0x800000
local MDB_NOMEMINIT    = 0x1000000
local MDB_PREVSNAPSHOT = 0x2000000

--mdb_dbi_open Database Flags
local MDB_REVERSEKEY   =    0x02
local MDB_DUPSORT      =    0x04
local MDB_INTEGERKEY   =    0x08
local MDB_DUPFIXED     =    0x10
local MDB_INTEGERDUP   =    0x20
local MDB_REVERSEDUP   =    0x40
local MDB_CREATE       = 0x40000

-- mdb_put Write Flags
local MDB_NOOVERWRITE  =    0x10
local MDB_NODUPDATA    =    0x20
local MDB_CURRENT      =    0x40
local MDB_RESERVE      = 0x10000
local MDB_APPEND       = 0x20000
local MDB_APPENDDUP    = 0x40000
local MDB_MULTIPLE     = 0x80000

-- mdb_copy Copy Flags
local MDB_CP_COMPACT = 0x01

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

typedef enum MDB_cursor_op {
	MDB_FIRST,
	MDB_FIRST_DUP,
	MDB_GET_BOTH,
	MDB_GET_BOTH_RANGE,
	MDB_GET_CURRENT,
	MDB_GET_MULTIPLE,
	MDB_LAST,
	MDB_LAST_DUP,
	MDB_NEXT,
	MDB_NEXT_DUP,
	MDB_NEXT_MULTIPLE,
	MDB_NEXT_NODUP,
	MDB_PREV,
	MDB_PREV_DUP,
	MDB_PREV_NODUP,
	MDB_SET,
	MDB_SET_KEY,
	MDB_SET_RANGE,
	MDB_PREV_MULTIPLE
} MDB_cursor_op;

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
int  mdb_cursor_get(MDB_cursor *cursor, MDB_val *key, MDB_val *data, MDB_cursor_op op);
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

local M = {}

local function check(rc)
	if rc == 0 then return end
	error(str(C.mdb_strerror(rc)), 2)
end

local Db = {}

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
function M.open(dir, opt)
	opt = opt or {}

	local schema_file = dir..'/schema.lua'
	if not opt.readonly then
		mkdir(dir)
		if not exists(schema_file) then
			save(schema_file, 'return {}')
		end
	end
	local schema = eval_file(schema_file)

	local env = new'MDB_env*[1]'
	check(C.mdb_env_create(env)); env = env[0]
	check(C.mdb_env_set_mapsize(env, 1024e4))
	check(C.mdb_env_set_maxreaders(env, schema.max_readers or 1024))
	check(C.mdb_env_set_maxdbs(env, schema.max_dbs or 1024))
	check(C.mdb_env_open(env, dir,
		opt.readonly and MDB_RDONLY or 0,
		(unixperms_parse(opt.file_mode or '0660'))
	))

	local dbis = {}
	local db = object(Db, {
		env = env,
		dbis = dbis,
		schema = schema,
		readonly = opt.readonly,
		_free_tx = {},
		_free_cur = {},
	})

	local max_key_size = C.mdb_env_get_maxkeysize(env)

	--compute column layout and encoding parameters based on schema.
	--NOTE: changing this algorithm or making it non-deterministic in any way
	--will trash your existing databses, so better version it or something!
	for table_name, table_schema in pairs(schema.tables or {}) do

		--split cols into key cols and val cols based on pk.
		local pk_cols = {}
		local pk_order = {}
		for s in words(table_schema.pk) do
			local pk, order = s:match'^(.-):(.*)'
			if not pk then
				pk, order = s, 'asc'
			else
				assert(order == 'desc' or order == 'asc')
				assert(#pk > 0)
			end
			add(pk_cols, pk)
			add(pk_order, order)
		end
		local key_cols = {}
		local val_cols = {}
		for i,col in ipairs(table_schema.columns) do
			if indexof(col.name, pk_cols) then
				add(key_cols, col)
				col.index = #key_cols
			else
				add(val_cols, col)
				col.index = #val_cols
			end
			--typecheck the column while we're at it.
			col.elem_size =
				col.type == 'f64' and 8 or
				col.type == 'u64' and 8 or
				col.type == 'i64' and 8 or
				col.type == 'u32' and 4 or
				col.type == 'i32' and 4 or
				col.type == 'u16' and 2 or
				col.type == 'i16' and 2 or
				col.type == 'i8'  and 1 or
				col.type == 'u8'  and 1 or
				assertf(false, 'unknown col type %s', col.type)
			assert(col.elem_size < 2^4) --must fit 4bit (see sort below)
		end
		assert(#key_cols < 2^16)
		assert(#val_cols < 2^16)

		table_schema.key_cols = key_cols
		table_schema.val_cols = val_cols

		for _,cols in ipairs{key_cols, val_cols} do

			if cols == val_cols then --we can layout val_cols freely.
				--move varsize cols at the end to minimize the size of the dyn offset table.
				--order cols by elem_size to maintain alignment.
				sort(cols, function(col1, col2)
					--elem_size fits in 4bit; col_index fits in 16bit; 4+16 = 20 bits,
					--so any bit from bit 21+ can be used for extra conditions.
					local i1 = (col1.maxlen and 2^22 or 0) + (2^4-1 - col1.elem_size) * 2^16 + col1.index
					local i2 = (col2.maxlen and 2^22 or 0) + (2^4-1 - col2.elem_size) * 2^16 + col2.index
					return i1 < i2
				end)
			end

			--compute dynamic offset table (d.o.t.) length.
			--all cols after the first varsize col need a dyn offset.
			local dot_len = 0
			for i,col in ipairs(cols) do
				if col.maxlen then --varsize
					dot_len = #cols - i
					break
				end
			end

			--compute max row size, excluding d.o.t.
			local max_row_size = 0
			for _,col in ipairs(cols) do
				max_row_size = max_row_size + (col.maxlen or col.len or 1) * col.elem_size
			end

			--compute min offset size.
			local offset_size = 8
			if max_row_size - dot_len < 2^8 then
				offset_size = 1
			elseif max_row_size - dot_len * 2 < 2^16 then
				offset_size = 2
			elseif max_row_size - dot_len * 4 < 2^32 then
				offset_size = 4
			end

			--compute max row size, includin d.o.t.
			local max_row_size = max_row_size + dot_len * offset_size

			cols.max_row_size = max_row_size
			cols.offset_size = offset_size

			--compute column value offsets.
			--layout: fixsize_val1, ..., dyn_offset1, ..., varsize_val1, remaining_val1, ...
			local offset = 0
			local in_dot
			for _,col in ipairs(cols) do
				if not in_dot then
					if dot_len > 0 and col.maxlen then
						--first varsize col: insert d.o.t. before it.
						col.offset = offset + dot_len * offset_size --first col after the d.o.t.
						in_dot = true --subsequent cols have dyn offsets from the d.o.t.
						--offset stays the same at first entry in the d.o.t.
					else --fixsize col or the only varsize col.
						col.offset = offset
						offset = offset + col.elem_size * (col.len or 1)
					end
				else --col after varsize col.
					col.offset = offset
					col.in_dot = true
					offset = offset + offset_size --next offset in the d.o.t.
				end
			end

			if cols == key_cols then
				assert(max_row_size <= max_key_size,
					'pk too big: %d bytes (max is %d bytes)', max_row_size, max_key_size)
			end

		end --for cols

		pr(table_schema)

	end --for tables

	--open all tables
	local dbi = new'MDB_dbi[1]'
	local tx = db:tx'w'
	for table_name, table_schema in pairs(schema.tables or {}) do
		check(C.mdb_dbi_open(tx.txn[0], table_name, MDB_CREATE, dbi))
		local dbi = dbi[0]
		dbis[table_name] = dbi
		--check(C.mdb_set_compare(self.txn[0], dbi, cmp))
	end
	tx:commit()

	return db
end

function Db:dbi(table_name)
	return assertf(self.dbis[table_name], 'table not found: %s', table_name)
end

local Tx = {}

function Db:tx(mode)
	mode = mode or 'r'
	assert(mode == 'w' or mode == 'r')
	local tx = pop(self._free_tx) or object(Tx, {
		db = self,
		txn = new'MDB_txn*[1]',
		cursors = {},
	})
	check(C.mdb_txn_begin(self.env, nil, mode == 'w' and 0 or MDB_RDONLY, tx.txn))
	return tx
end

function Tx:closed()
	return self.txn[0] == nil
end

function Tx:close_cursors()
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
end

function Tx:abort()
	assert(not self:closed(), 'transaction closed')
	self:close_cursors()
	C.mdb_txn_abort(self.txn[0])
	self.txn[0] = nil
	push(self.db._free_tx, self)
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

function Tx:upsert(tab, row)
	--
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
	self.cursors[cur] = true
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
	local rc = C.mdb_cursor_get(self.cursor[0], key, val, C.MDB_NEXT)
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

local lmdb = M

local db = lmdb.open('testdb')

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
