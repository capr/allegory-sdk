require'gzip'
require'glue'
require'unit'

local function gen(n)
	local t = {}
	for i=1,n do
		t[i] = string.format('dude %g\r\n', math.random())
	end
	return table.concat(t)
end

local function reader(s)
	local done
	return function()
		if done then return end
		done = true
		return s
	end
end

local function writer()
	local t = {}
	return function(data, sz)
		if not data then return table.concat(t) end
		t[#t+1] = ffi.string(data, sz)
	end
end

local function test_deflate(format, size)
	local src = gen(size)
	local write = writer()
	deflate(reader(src), write, nil, format)
	local dst = write()
	local write = writer()
	inflate(reader(dst), write, nil, format)
	local src2 = write()
	assert(src == src2)
	print(string.format('format: %-8s size: %5dK ratio: %d%%',
		format, #src/1024, #dst / #src * 100))
end
for _,size in ipairs{0, 1, 13, 2049, 100000} do
	test_deflate('gzip', size)
	test_deflate('zlib', size)
	test_deflate('deflate', size)
end

--gzip_state compress + decompress roundtrip
for _,size in ipairs{0, 1, 13, 2049, 100000} do
	local src = gen(size)
	local cwrite = writer()
	local gz = gzip_state{op = 'compress', write = cwrite}
	gz:feed(src)
	gz:feed(nil, 'eof')
	local compressed = cwrite()
	local dwrite = writer()
	local gz = gzip_state{op = 'decompress', write = dwrite}
	gz:feed(compressed)
	gz:feed(nil, 'eof')
	local src2 = dwrite()
	assert(src == src2)
	print(string.format('gzip_state: size: %5dK ratio: %d%%',
		#src/1024, #compressed / math.max(#src, 1) * 100))
end

test(tohex(adler32'The game done changed.'), '587507ba')
test(tohex(crc32'Game\'s the same, just got more fierce.'), '2c40120a')

print'gzip ok'
