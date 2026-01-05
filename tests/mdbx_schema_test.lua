--go@ plink -t root@m1 sdk/bin/debian12/luajit sdk/tests/mdbx_schema_test.lua

require'mdbx_schema'
logging.debug = true

pr('libmdbx.so vesion: ',
	mdbx.mdbx_version.major..'.'..
	mdbx.mdbx_version.minor..'.'..
	mdbx.mdbx_version.patch,
	str(mdbx.mdbx_version.git.commit, 6))
pr()

rm'mdbx_schema_test.mdb'
rm'mdbx_schema_test.mdb-lck'
local schema = schema.new()
local db = mdbx_open('mdbx_schema_test.mdb')
db.schema = schema

function test_encdec()

	local types = 'u8 u16 u32 u64 i8 i16 i32 i64 f32 f64'
	local num_tables = {}
	local varsize_key1_tables = {}
	local varsize_key2_tables = {}
	for order in words'asc desc' do
		--single numeric keys + vals at fixed offsets.
		for typ in words(types) do
			local name = typ..':'..order
			local tbl = {
				name = name,
				test_type = typ,
				fields = {
					{col = 'k' , mdbx_type = typ},
					{col = 'v1', mdbx_type = typ},
					{col = 'v2', mdbx_type = typ},
				},
				pk = {'k', desc = {order == 'desc'}},
			}
			add(num_tables, tbl)
			schema.tables[tbl.name] = tbl
		end

		--varsize key and val at fixed offset with utf8 enc/dec.
		local tbl = {
			name = 'varsize_key1'..':'..order,
			fields = {
				{col = 's', mdbx_type = 'utf8', maxlen = 100},
				{col = 'v', mdbx_type = 'utf8', maxlen = 100},
			},
			pk = {'s', desc = {order == 'desc'}},
		}
		add(varsize_key1_tables, tbl)
		schema.tables[tbl.name] = tbl

		--varsize key and val at dyn offset.
		for order2 in words'asc desc' do
			local tbl = {
				name = 'varsize_key2'..':'..order..':'..order2,
				fields = {
					{col = 's1', mdbx_type = 'utf8', maxlen = 100},
					{col = 's2', mdbx_type = 'utf8', maxlen = 100},
					{col = 's3', mdbx_type = 'utf8', maxlen = 100},
					{col = 's4', mdbx_type = 'utf8', maxlen = 100},
				},
				pk = {'s1', 's2', desc = {order == 'desc', order2 == 'desc'}},
			}
			add(varsize_key2_tables, tbl)
			schema.tables[tbl.name] = tbl
		end

	end

	--test int and float decoders and encoders
	for _,tbl in ipairs(num_tables) do
		local typ = tbl.test_type
		db:begin'w'
		local bits = num(typ:sub(2))
		local ntyp = typ:sub(1,1)
		local nums =
			ntyp == 'u'  and {0,1,2,2ULL^bits-1} or
			ntyp == 'i'  and {-2LL^(bits-1),-(2LL^(bits-1)-1),-2,-1,0,1,2,2LL^(bits-1)-2,2LL^(bits-1)-1} or
			typ == 'f64' and {-2^52,-2,-1,-0.1,-0,0,0.1,1,2^52} or
			typ == 'f32' and {-2^23,-2,-1,cast('float', -0.1),-0,0,cast('float', 0.1),1,2^23}
		assert(nums)
		local t = {}
		for _,i in ipairs(nums) do
			add(t, {i, i, i})
		end
		db:put_records(tbl.name, '[k v1 v2]', t)
		db:commit()

		if tbl.fields.k.descending then
			reverse(nums)
		end
		db:begin()
		local i = 1
		for cur, k, v1, v2 in db:each(tbl.name) do
			pr(tbl.name, k, v1, v2)
			assertf(k  == nums[i], '%q ~= %q', k , nums[i])
			assertf(v1 == nums[i], '%q ~= %q', v1, nums[i])
			assertf(v2 == nums[i], '%q ~= %q', v2, nums[i])
			i = i + 1
		end
		db:commit()
	end

	--test varsize_key1
	for _,tbl in ipairs(varsize_key1_tables) do
		local t = {
			{'a' , 'b' },
			{'bb', nil },
			{'aa', 'bb'},
			{'b' , nil },
		}
		db:begin'w'
		db:put_records(tbl.name, '[]', t)
		db:commit()

		sort(t, function(r1, r2)
			if tbl.fields.s.descending then
				return r2[1] < r1[1]
			else
				return r1[1] < r2[1]
			end
		end)
		db:begin()
		local t1 = {}
		for cur, t in db:each(tbl.name, '[]') do
			add(t1, t)
		end
		db:commit()
		for i=1,#t do
			assert(t1[i][1] == t[i][1], t[i][1])
			assert(t1[i][2] == t[i][2], t[i][2])
		end
		pr()
	end
	pr()

	--test varsize_key2
	for _,tbl in ipairs(varsize_key2_tables) do
		local t = {
			{'a'  , 'b'  , 'a'  , nil  , },
			{'a'  , 'a'  , 'a'  , 'a'  , },
			{'a'  , 'aaa', 'a'  , 'aaa', },
			{'a'  , 'bbb', 'a'  , nil  , },
			{'aa' , 'a'  , 'aa' , 'a'  , },
			{'aa' , 'b'  , 'aa' , nil  , },
			{'bb' , 'a'  , nil  , 'a'  , },
			{'bb' , 'aa' , nil  , 'aa' , },
			{'bb' , 'bb' , nil  , nil  , },
			{'aa' , 'bb' , 'aa' , nil  , },
			{'b'  , 'a'  , nil  , 'a'  , },
			{''   , 'a'  , ''   , 'a'  , },
			{'a'  , ''   , 'a'  , ''   , },
			{'xx' , 'y'  , 'z'  , 'zz' , },
		}
		db:begin'w'
		db:put_records(tbl.name, t)
		db:commit()

		local s1_desc = tbl.fields.s1.descending
		local s2_desc = tbl.fields.s2.descending
		sort(t,
			function(r1, r2)
				local c1; if s1_desc then c1 = r2[1] < r1[1] else c1 = r1[1] < r2[1] end
				local c2; if s2_desc then c2 = r2[2] < r1[2] else c2 = r1[2] < r2[2] end
				if r2[1] == r1[1] then return c2 else return c1 end
			end)
		db:begin()
		pr('***', tbl.pk, '***')
		local t1 = {}
		for cur, s1, s2, s3, s4 in db:each(tbl.name) do
			assert(db:is_null(tbl.name, 's3', s1, s2) == (s3 == nil))
			assert(db:is_null(tbl.name, 's4', s1, s2) == (s4 == nil))
			add(t1, {s1, s2, s3, s4})
		end
		local s3, s4 = db:get(tbl.name, 's3 s4', 'xx', 'y')
		assert(s3 == 'z')
		assert(s4 == 'zz')
		db:commit()
		for i=1,#t do
			pr(unpack(t1[i], 1, 4))
			assert(t[i][1] == t1[i][1])
			assert(t[i][2] == t1[i][2])
			assert(t[i][3] == t1[i][3])
			assert(t[i][4] == t1[i][4])
		end
		pr()
	end

end

function test_uks()

	schema.tables.test_uk = {
		fields = {
			{col = 'k1', mdbx_type = 'utf8', maxlen = 10},
			{col = 'k2', mdbx_type = 'utf8', maxlen = 10},
			{col = 'u1', mdbx_type = 'utf8', maxlen = 10},
			{col = 'u2', mdbx_type = 'utf8', maxlen = 10},
			{col = 'f1', mdbx_type = 'utf8', maxlen = 10},
			{col = 'f2', mdbx_type = 'utf8', maxlen = 10},
		},
		pk = {'k1', 'k2'},
	}

	db:begin'w'
	db:insert('test_uk', nil, 'k1', 'k1', 'u1', 'u1', 'f1')
	db:insert('test_uk', nil, 'k1', 'k2', 'u1', 'u2', 'f2')
	db:insert('test_uk', nil, 'k2', 'k1', 'u2', 'u1', 'f3')
	db:insert('test_uk', nil, 'k3', 'k1', 'u2', 'u1', 'f3') --duplicate uk
	db:commit()

	--test fail to create uk on account of duplicates
	db:begin'w'
	local ok, err = db:try_create_index('test_uk', {'u1', 'u2', is_unique = true})
	local ix_tbl = 'test_uk/u1-u2'
	assert(not ok)
	assert(tostring(err):has'exists')
	db:commit()

	db:begin()
	assert(not db:table_exists(ix_tbl))
	assert(not db.schema.tables[ix_tbl])
	db:commit()

	--remove duplicate and try again.
	db:begin'w'
	assert(not db:table_exists(ix_tbl))
	assert(not db:try_open_table(ix_tbl))
	db:del('test_uk', 'k3', 'k1')
	db:create_index('test_uk', {'u1', 'u2', is_unique = true})
	assert(db:table_exists(ix_tbl))
	--now try to insert a duplicate again, this time it should fail.
	db:insert('test_uk', nil, 'k3', 'k2', 'u3', 'u1', 'f4')
	db:insert('test_uk', nil, 'k4', 'k2', 'u3', 'u2', 'f5')
	db:insert('test_uk', nil, 'k4', 'k1', 'u4', 'u1', 'f6')
	local ok = db:try_insert('test_uk', nil, 'k5', 'k1', 'u4', 'u1', 'f7')
	assert(not ok)
	db:commit()

	db:begin()
	for _,u1,u2 in assert(db:each(ix_tbl)) do
		pr(u1, u2)
	end
	local t = db:must_get(ix_tbl, '{}', 'u1', 'u1')
	assert(t.k1 == 'k1')
	assert(t.k2 == 'k1')
	assert(t.u1 == 'u1')
	assert(t.u2 == 'u1')
	assert(t.f1 == 'f1')
	db:commit()

end

function test_rename_table()
	schema.tables.test_rename_table1 = {
		fields = {{col = 'id', mdbx_type = 'u8'}},
		pk = {'id'},
	}
	db:begin'w'
	db:create_table('test_rename_table1')
	assert(db:table_exists'test_rename_table1')
	db:rename_table(
		'test_rename_table1',
		'test_rename_table2'
	)
	db:commit()
end

function test_fks()
	schema:import(function()
		import'schema_std'
		tables.fk1 = {
			fk1, idpk,
		}
		tables.fk2 = {
			fk2, idpk,
		}
		tables.fk3 = {
			fk2, idpk,
		}
		tables.fkt = {
			id , idpk,
			fk1, id, fk(fk1),
			fk2, id, child_fk(fk2),
			fk3, id, weak_fk(fk3),
		}
	end)
	db:begin'w'
	for _, fk in pairs(schema.tables.fkt.fks) do
		db:create_fk(fk)
	end
	db:insert('fk1')
	db:insert('fk2')
	db:insert('fk3')
	db:insert('fkt', nil, 1, 1, 1)
	db:commit()
end

function test_stat()
	db:begin()
	pr(rpad('TABLE ('..db:table_count()..')', 24),
		'ENTRIES', 'PSIZE', 'DEPTH',
		'BR_PG', 'LEAF_PG', 'OVER_PG',
		'TXNID')
	pr(rep('-', 90))
	for table_name in db:each_table() do
		local s = db:table_stat(table_name)
		pr(rpad(table_name, 24),
			num(s.entries), s.psize, s.depth,
			num(s.branch_pages), num(s.leaf_pages), num(s.overflow_pages),
			num(s.mod_txnid))
	end
	db:abort()
	pr()
end

function test_each()
	db:begin()
	pr(rpad('TABLE ('..db:table_count()..')', 24),
		'KCOLS', 'VCOLS', 'PK')
	pr(rep('-', 90))
	for table_name in db:each_table() do
		local s = db.schema.tables[table_name]
		if s then
			pr(rpad(table_name, 24),
				cat(imap(s.key_fields, 'name'),','),
				cat(imap(s.val_fields, 'name'),','),
				s.pk)
		else
			pr(rpad(table_name, 24), '')
		end
	end
	db:abort()
	pr()
end

--close and reopen db to check that stored schema matches paper schema.
function test_schema()
	db:close()
	db = mdbx_open('mdbx_schema_test.mdb')
	db.schema = schema
	db:begin()
	for tab in db:each_table() do
		db:open_table(tab)
	end
	db:abort()
end

--test_encdec()
test_uks()
--test_rename_table()
--test_fks()
--test_stat()
--test_each()
--test_schema()

db:close()
