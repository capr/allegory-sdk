--go@ plink m1 -t sdk/bin/linux/luajit -lscite "sdk/tests/fs_test.lua ls"
--#!../bin/linux/luajit
require'glue'
require'fs'
require'logging'
require'sock'

--if luapower sits on a VirtualBox shared folder on a Windows host
--we can't mmap files, create symlinks or use locking on that, so we'll use
--$HOME, which is usually a disk mount.
local tests_dir = exedir()..'/../../tests'
local fs_test_lua = tests_dir..'/fs_test.lua'

local test = setmetatable({}, {__newindex = function(t, k, v)
	rawset(t, k, v)
	rawset(t, #t+1, k)
end})

--open/close -----------------------------------------------------------------

function test.open_close()
	local testfile = 'fs_testfile'
	local f = open(testfile, 'w')
	assert(isfile(f))
	assert(not f:closed())
	f:close()
	assert(f:closed())
	rmfile(testfile)
end

function test.open_not_found()
	local nonexistent = 'this_file_should_not_exist'
	local f, err = try_open(nonexistent)
	assert(not f)
	assert(err == 'not_found')
end

function test.open_already_exists_file()
	local testfile = 'fs_testfile'
	local f = try_open(testfile, 'w')
	f:close()
	local f, err = try_open({
			path = testfile,
			mode = false,
			flags = 'creat excl'
		})
	assert(not f)
	assert(err == 'already_exists')
	rmfile(testfile)
end

function test.open_already_exists_dir()
	local testfile = 'fs_test_dir_already_exists'
	rmdir(testfile)
	mkdir(testfile)
	local f, err = try_open({
			path = testfile,
			flags = 'creat excl',
			mode = false,
		})
	assert(not f)
	assert(err == 'already_exists')
	rmdir(testfile)
end

function test.open_dir()
	local testfile = 'fs_test_open_dir'
	local using_backup_semantics = true
	rmdir(testfile)
	mkdir(testfile)
	local f, err = try_open(testfile)
	assert(f)
	f:close()
	rmdir(testfile)
end

--pipes ----------------------------------------------------------------------

function test.pipe() --I/O test in proc_test.lua
	local rf, wf = pipe{async = false}
	rf:close()
	wf:close()
end

--NOTE: I/O tests in proc_test.lua!
function test.named_pipe()
	require'sock'
	local path = 'fs_test_pipe'
	rmfile(path)
	mkfifo(path)
	local p1, err1 = assert(open{path = path, type = 'pipe', async = true})
	local p2, err2 = assert(open{path = path, type = 'pipe', async = true})
	assert(not err1)
	assert(not err2)
	p1:close()
	p2:close()
	rmfile(path)
end

--i/o ------------------------------------------------------------------------

function test.read_write()
	local testfile = 'fs_test_read_write'
	local sz = 4096
	local buf = ffi.new('uint8_t[?]', sz)

	--write some patterns
	local f = open(testfile, 'w')
	for i=0,sz-1 do
		buf[i] = i
	end
	for i=1,4 do
		f:write(buf, sz)
	end
	f:close()

	--read them back
	local f = open(testfile)
	local t = {}
	while true do
		local readsz = f:read(buf, sz)
		if readsz == 0 then break end
		t[#t+1] = ffi.string(buf, readsz)
	end
	f:close()

	--check them out
	local s = table.concat(t)
	for i=1,#s do
		assert(s:byte(i) == (i-1) % 256)
	end

	rmfile(testfile)
end

function test.open_modes()
	local testfile = 'fs_test'
	--TODO:
	local f = open(testfile, 'w')
	f:close()
	rmfile(testfile)
end

function test.seek()
	local testfile = 'fs_test'
	local f = open(testfile, 'w')

	--test large file support by seeking past 32bit
	local newpos = 2^40
	local pos = f:seek('set', newpos)
	assert(pos == newpos)
	local pos = f:seek(-100)
	assert(pos == newpos -100)
	local pos = f:seek('end', 100)
	assert(pos == 100)

	--write some data and check again
	local newpos = 1024^2
	local buf = ffi.new'char[1]'
	local pos = f:seek('set', newpos)
	assert(pos == newpos) --seeked outside
	buf[0] = 0xaa
	f:write(buf, 1) --write outside cur
	local pos = f:seek()
	assert(pos == newpos + 1) --cur advanced
	local pos = f:seek('end')
	assert(pos == newpos + 1) --end updated
	assert(f:seek'end' == newpos + 1)
	f:close()

	rmfile(testfile)
end

--truncate -------------------------------------------------------------------

function test.truncate_seek()
	local testfile = 'fs_test_truncate_seek'
	--truncate/grow
	local f = open(testfile, 'w')
	local newpos = 1024^2
	f:truncate(newpos)
	assert(f:seek() == newpos)
	f:close()
	--check size
	local f = open(testfile, 'r+')
	local pos = f:seek'end'
	assert(pos == newpos)
	--truncate/shrink
	local pos = f:seek('end', -100)
	f:truncate(pos)
	assert(pos == newpos - 100)
	f:close()
	--check size
	local f = open(testfile, 'r')
	local pos = f:seek'end'
	assert(pos == newpos - 100)
	f:close()

	rmfile(testfile)
end

--filesystem operations ------------------------------------------------------

function test.cd_mkdir_remove()
	local testdir = 'fs_test_dir'
	local cd = cwd()
	mkdir(testdir) --relative paths should work
	chdir(testdir) --relative paths should work
	chdir(cd)
	assert(cwd() == cd)
	rmdir(testdir) --relative paths should work
end

function test.mkdir_recursive()
	mkdir('fs_test_dir/a/b/c', true)
	rmdir'fs_test_dir/a/b/c'
	rmdir'fs_test_dir/a/b'
	rmdir'fs_test_dir/a'
	rmdir'fs_test_dir'
end

function test.rm_rf()
	local rootdir = 'fs_test_rmdir_rec/'
	rm_rf(rootdir, true)
	local fs_mkdir = mkdir
	local function mkdir(dir)
		fs_mkdir(rootdir..dir, true)
	end
	local function mkfile(file)
		local f = open(rootdir..file, 'w')
		f:close()
	end
	mkdir'a/b/c'
	mkfile'a/b/c/f1'
	mkfile'a/b/c/f2'
	mkdir'a/b/c/d1'
	mkdir'a/b/c/d2'
	mkfile'a/b/f1'
	mkfile'a/b/f2'
	mkdir'a/b/d1'
	mkdir'a/b/d2'
	rm_rf(rootdir)
end

function test.ls_empty()
	local d = 'fs_test_dir_empty/a/b'
	rm_rf'fs_test_dir_empty/'
	mkdir(d, true)
	for name in ls(d) do
		print(name)
	end
	rm_rf'fs_test_dir_empty/'
end

function test.mkdir_already_exists_dir()
	mkdir'fs_test_dir'
	local ok, err = try_mkdir'fs_test_dir'
	assert(ok)
	assert(err == 'already_exists')
	rmdir'fs_test_dir'
end

function test.mkdir_already_exists_file()
	local testfile = 'fs_test_dir_already_exists_file'
	local f = open(testfile, 'w')
	f:close()
	local ok, err = try_mkdir(testfile)
	assert(ok)
	assert(err == 'already_exists')
	rmfile(testfile)
end

function test.mkdir_not_found()
	local ok, err = try_mkdir'fs_test_nonexistent/nonexistent'
	assert(not ok)
	assert(err == 'not_found')
end

function test.remove_dir_not_found()
	local testfile = 'fs_test_rmdir_not_found'
	rmdir(testfile)
	local ok, err = rmfile(testfile)
	assert(ok)
	assert(err == 'not_found')
end

function test.remove_not_empty()
	local dir1 = 'fs_test_rmdir'
	local dir2 = 'fs_test_rmdir/subdir'
	rmdir(dir2)
	rmdir(dir1)
	mkdir(dir1)
	mkdir(dir2)
	local ok, err = try_rmdir(dir1)
	assert(not ok)
	assert(err == 'not_empty')
	rmdir(dir2)
	rmdir(dir1)
end

function test.remove_file()
	local name = 'fs_test_remove_file'
	rmfile(name)
	assert(io.open(name, 'w')):close()
	rmfile(name)
	assert(not io.open(name, 'r'))
end

function test.cd_not_found()
	local ok, err = try_chdir'fs_test_nonexistent/nonexistent'
	assert(not ok)
	assert(err == 'not_found')
end

function test.remove()
	local testfile = 'fs_test_remove'
	local f = open(testfile, 'w')
	f:close()
	rmfile(testfile)
	assert(not try_open(testfile))
end

function test.remove_file_not_found()
	local testfile = 'fs_test_remove'
	local ok, err = try_rmfile(testfile)
	assert(ok)
	assert(err == 'not_found')
end

function test.move()
	local f1 = 'fs_test_move1'
	local f2 = 'fs_test_move2'
	local f = open(f1, 'w')
	f:close()
	rename(f1, f2)
	rmfile(f2)
	assert(select(2, try_rmfile(f1)) == 'not_found')
end

function test.move_not_found()
	local ok, err = try_rename('fs_nonexistent_file', 'fs_nonexistent2')
	assert(not ok)
	assert(err == 'not_found')
end

function test.move_replace()
	local f1 = 'fs_test_move1'
	local f2 = 'fs_test_move2'
	local buf = ffi.new'char[1]'

	local f = open(f1, 'w')
	buf[0] = ('1'):byte(1)
	f:write(buf, 1)
	f:close()

	local f = open(f2, 'w')
	buf[0] = ('2'):byte(1)
	f:write(buf, 1)
	f:close()

	rename(f1, f2)

	local f = open(f2)
	f:read(buf, 1)
	assert(buf[0] == ('1'):byte(1))
	f:close()

	rmfile(f2)
end

--symlinks -------------------------------------------------------------------

local function symlink_file(f1, f2)
	local buf = u8a(1)

	rmfile(f1)
	rmfile(f2)

	local f = open(f2, 'w')
	buf[0] = ('X'):byte(1)
	f:write(buf, 1)
	f:close()

	sleep(0.1)

	local ok, err = try_symlink(f1, f2)
	if ok then
		assert(file_is(f1, 'symlink'))
		local f = open(f1)
		f:read(buf, 1)
		assert(buf[0] == ('X'):byte(1))
		f:close()
	else
		rmfile(f1)
		rmfile(f2)
		assert(ok, err)
	end
end

function test.symlink_file()
	local f1 = 'fs_test_symlink_file'
	local f2 = 'fs_test_symlink_file_target'
	symlink_file(f1, f2)
	assert(file_is(f1, 'symlink'))
	rmfile(f1)
	rmfile(f2)
end

function test.symlink_dir()
	local link = 'fs_test_symlink_dir'
	local dir = 'fs_test_symlink_dir_target'
	rmfile(link)
	rmdir(dir..'/test_dir')
	rmdir(dir)
	mkdir(dir)
	mkdir(dir..'/test_dir')
	local ok,err = try_symlink(link, dir, 'replace')
	if ok then
		assert(file_is(link..'/test_dir', 'dir'))
		rmdir(link..'/test_dir')
		rmfile(link)
		rmdir(dir)
	else
		rm_rf(dir)
	end
	assert(ok,err)
end

function test.readlink_file()
	local f1 = 'fs_test_readlink_file'
	local f2 = 'fs_test_readlink_file_target'
	symlink_file(f1, f2)
	assert(readlink(f1) == f2)
	rmfile(f1)
	rmfile(f2)
end

function test.readlink_dir()
	local d1 = 'fs_test_readlink_dir'
	local d2 = 'fs_test_readlink_dir_target'
	rmdir(d1)
	rmdir(d2..'/test_dir')
	rmdir(d2)
	rmdir(d2)
	mkdir(d2)
	mkdir(d2..'/test_dir')
	local ok,err = try_symlink(d1, d2, 'replace')
	if ok then
		assert(file_is(d1, 'symlink'))
		local t = {}
		for d in ls(d1) do
			t[#t+1] = d
		end
		assert(#t == 1)
		assert(t[1] == 'test_dir')
		rmdir(d1..'/test_dir')
		assert(readlink(d1) == d2)
		rmdir(d1)
		rmdir(d2)
	else
		rmdir(d2, true)
	end
	assert(ok,err)
end

--TODO: readlink() with relative symlink chain
--TODO: attr() with defer and symlink chain
--TODO: dir() with defer and symlink chain

function test.attr_deref()
	--
end

function test.symlink_attr_deref()
	local f1 = 'fs_test_readlink_file'
	local f2 = 'fs_test_readlink_file_target'
	symlink_file(f1, f2)
	local lattr  = file_attr(f1, false)
	local tattr1 = file_attr(f1, true)
	local tattr2 = file_attr(f2)
	assert(lattr .type == 'symlink')
	assert(tattr1.type == 'file')
	assert(tattr2.type == 'file')
	assert(tattr1.inode == tattr2.inode) --same file
	assert(lattr.inode ~= tattr1.inode) --diff. file
	rmfile(f1)
	rmfile(f2)
end

--hardlinks ------------------------------------------------------------------

function test.hardlink() --hardlinks only work for files in NTFS
	local f1 = 'fs_test_hardlink'
	local f2 = 'fs_test_hardlink_target'
	rmfile(f1)
	rmfile(f2)

	local buf = ffi.new'char[1]'

	local f = open(f2, 'w')
	buf[0] = ('X'):byte(1)
	f:write(buf, 1)
	f:close()

	hardlink(f1, f2)

	local f = open(f1)
	f:read(buf, 1)
	assert(buf[0] == ('X'):byte(1))
	f:close()

	rmfile(f1)
	rmfile(f2)
end

--file times -----------------------------------------------------------------

function test.times()
	local testfile = 'fs_test_time'
	rmfile(testfile)
	local f = open(testfile, 'w')
	local t = f:attr()
	assert(t.atime >= 0)
	assert(t.mtime >= 0)
	assert(t.ctime >= 0)
	f:close()
	rmfile(testfile)
end

function test.times_set()
	local testfile = 'fs_test_time'
	local f = open(testfile, 'w')

	local frac = 1/2
	local t = math.floor(os.time())
	local mtime = t - 3600 - frac
	local ctime = t - 2800 - frac
	local atime = t - 1800 - frac

	f:attr{mtime = mtime, ctime = ctime, atime = atime}
	local mtime1 = f:attr'mtime'
	local ctime1 = f:attr'ctime'
	local atime1 = f:attr'atime'
	assert(mtime == mtime1)
	assert(atime == atime1)

	--change only mtime, should not affect atime
	mtime = mtime + 100
	f:attr{mtime = mtime}
	local mtime1 = f:attr().mtime
	local atime1 = f:attr().atime
	assert(mtime == mtime1)
	assert(atime == atime1)

	--change only atime, should not affect mtime
	atime = atime + 100
	f:attr{atime = atime}
	local mtime1 = f:attr'mtime'
	local atime1 = f:attr'atime'
	assert(mtime == mtime1)
	assert(atime == atime1)

	f:close()
	rmfile(testfile)
end

--common paths ---------------------------------------------------------------

function test.paths()
	print('homedir', homedir())
	print('tmpdir ', tmpdir())
	print('exepath', exepath())
	print('exedir' , exedir())
	print('scriptdir', scriptdir())
end

--file attributes ------------------------------------------------------------

function test.attr()
	local testfile = fs_test_lua
	local function test(attr)
		assert(attr.type == 'file')
		assert(attr.size > 10000)
		assert(attr.atime)
		assert(attr.mtime)
		assert(attr.ctime)
		assert(attr.inode)
		assert(attr.uid >= 0)
		assert(attr.gid >= 0)
		assert(attr.perms >= 0)
		assert(attr.nlink >= 1)
		assert(attr.perms > 0)
		assert(attr.blksize > 0)
		assert(attr.blocks > 0)
		assert(attr.dev >= 0)
	end
	local attr = file_attr(testfile, false)
	test(attr)
	assert(file_attr(testfile, 'type' , false) == attr.type)
	assert(file_attr(testfile, 'atime', false) == attr.atime)
	assert(file_attr(testfile, 'mtime', false) == attr.mtime)
	assert(file_attr(testfile, 'size' , false) == attr.size)
end

function test.attr_set()
	--TODO
end

--directory listing ----------------------------------------------------------

function test.ls()
	local found
	local n = 0
	local files = {}
	for file, d in ls(tests_dir) do
		if not file then break end
		found = found or file == 'fs_test.lua'
		n = n + 1
		local t = {}
		files[file] = t
		--these are fast to get on all platforms
		t.type = d:attr('type', false)
		t.inode = d:attr('inode', false)
		t.mtime = assert(d:attr('mtime', false))
		t.atime = assert(d:attr('atime', false))
		t.size  = assert(d:attr('size' , false))
		t._all_attrs = assert(d:attr(false))
		local noval, err = d:try_attr('non_existent_attr', false)
		assert(noval == nil) --non-existent attributes are free to get
		assert(not err) --and they are not an error
		--print('', d:attr('type', false), file)
	end
	assert(not files['.'])  --skipping this by default
	assert(not files['..']) --skipping this by default
	assert(files['fs_test.lua'].type == 'file')
	local t = files['fs_test.lua']
	print(string.format('  found %d dir/file entries in cwd', n))
	assert(found, 'fs_test.lua not found in cwd')
end

function test.scandir()
	local n = 0
	for sc in scandir('/proc') do
		local typ, err = sc:attr'type'
		local path, err = sc:path()
		print(string.format('%-5s %-60s %s', typ, path, err or ''))
		n = n + 1
		if n >= 20 then
			break
		end
	end
	print(n)
end

function test.ls_not_found()
	local n = 0
	local err
	for file, err1 in ls'nonexistent_dir' do
		if not file then
			err = err1
			break
		else
			n = n + 1
		end
	end
	assert(n == 0)
	assert(#err > 0)
	assert(err == 'not_found')
end

function test.ls_is_file()
	local n = 0
	local err
	for file, err1 in ls(fs_test_lua) do
		if not file then
			err = err1
			break
		else
			n = n + 1
		end
	end
	assert(n == 0)
	assert(#err > 0)
	assert(err == 'not_found')
end

--test cmdline ---------------------------------------------------------------

chdir(os.getenv'HOME')
mkdir'fs_test'
chdir'fs_test'

local name = ...
if not name or name == 'fs_test' then
	--run all tests in the order in which they appear in the code.
	local n,m = 0, 0
	for i,k in ipairs(test) do
		if not k:find'^_' then
			print('test.'..k)
			local ok, err = xpcall(test[k], debug.traceback)
			if not ok then
				print('FAILED: ', err)
				n=n+1
			else
				m=m+1
			end
		end
	end
	print(string.format('ok: %d, failed: %d', m, n))
elseif test[name] then
	test[name](select(2, ...))
else
	print('Unknown test "'..(name)..'".')
end

assert(basename(cwd()) == 'fs_test')
chdir'..'
rm_rf'fs_test'
