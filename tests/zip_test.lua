require'glue'
require'zip'
require'fs'

chdir(exedir()..'/../tests')

local function check_entries(z, expected)
	local i = 0
	for e in z:entries() do
		i = i + 1
		local exp = expected[i]
		assert(exp, 'unexpected entry: '..e.filename)
		assert(e.filename == exp.filename, e.filename)
		assert(z.entry_is_dir == exp.is_dir, 'is_dir mismatch: '..e.filename)
		assert(e.compression_method == exp.method, 'method mismatch: '..e.filename)
		if not exp.is_dir then
			assert(e.uncompressed_size > 0, 'zero usize: '..e.filename)
			assert(e.compressed_size > 0, 'zero csize: '..e.filename)
		end
		assert(z:open_entry())
		local buf = ffi.new'char[1]'
		z:read(buf, 1)
		z:close_entry()
		local s = z:read'*a'
		if exp.is_dir then
			assert(s == nil)
		else
			assert(s and s:find'^hello', 'content mismatch: '..e.filename)
		end
	end
	assert(i == #expected, format('expected %d entries, got %d', #expected, i))
end

--test reading AES-encrypted zip
local z = assert(zip_open{
	file = 'zip_test/test-aes.zip',
	password = '123',
})

check_entries(z, {
	{filename = 'test/',            is_dir = true,  method = 'store'},
	{filename = 'test/a/',          is_dir = true,  method = 'store'},
	{filename = 'test/a/x/',        is_dir = true,  method = 'store'},
	{filename = 'test/a/x/test1.txt', is_dir = false, method = 'deflate'},
	{filename = 'test/a/x/test2.txt', is_dir = false, method = 'deflate'},
	{filename = 'test/b/',          is_dir = true,  method = 'store'},
})

assert(z:find'test/a/x/test1.txt')
assert(z.entry_is_dir == false)
z:open_entry()

assert(z:find'test/a/')
assert(z.entry_is_dir == true)
z:open_entry()

--test hashing
assert(z:find'test/a/x/test1.txt')
local h = z:entry_hash'sha256'
assert(h and #h == 32, 'sha256 hash missing or wrong size')

assert(z:extract_all'tmp/minizip-test')

z:close()

--test writing zip (without AES for compatibility)
local z = zip_open{
	file = 'zip_test/test-aes2.zip',
	mode = 'w',
	password = '321',
}
z.aes = false
z:add_all('tmp/minizip-test')
z:close()

rm_rf'tmp/minizip-test'

--test reading back the written zip
local z = zip_open('zip_test/test-aes2.zip', 'r', '321')

local count = 0
for e in z:entries() do
	count = count + 1
	assert(e.filename, 'missing filename')
	if not z.entry_is_dir then
		local s = z:read'*a'
		assert(s and s:find'^hello', 'content mismatch: '..e.filename)
	end
end
assert(count > 0, 'no entries in written zip')

z:extract_all'tmp/minizip-test'

z:close()

rm_rf'tmp/minizip-test'
rmfile'zip_test/test-aes2.zip'
rmdir'tmp'

print'zip ok'
