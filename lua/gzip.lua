--[[

	ZLIB binding, providing:
		* DEFLATE compression & decompression.
		* CRC32 & ADLER32 checksums.
	Written by Cosmin Apreutesei. Public Domain.

DEFLATE ----------------------------------------------------------------------

deflate(opt)
inflate(opt)

	Compress/decompress a data stream using the DEFLATE algorithm. Options:

	* `read` is a function `read() -> s[,size] | cdata,size | nil | false,err`.
	* `write` is a function `write(cdata, size) -> nil | false,err`.
	  * callbacks are allowed to yield and abort by returning `false,err`.
	  * errors raised in callbacks pass-through uncaught (but don't leak).
	  * `nil,err` is returned for zlib errors and callback aborts.
	  * an abandoned thread suspended in read/write callbacks is gc'ed leak-free.
	* `bufsize` affects the frequency and size of the writes (defaults to 64K).
	* `format` can be 'zlib' (default), 'gzip' or 'raw'.
	* `level` controls the compression level (0-9 from none to best).
	* `windowBits`, `memLevel` and `strategy`: refer to the zlib manual.
	  * note that our `windowBits` is always in the positive range 8..15.

CRC32 & ADLER32 --------------------------------------------------------------

adler32(cdata, size[, adler]) -> n
adler32(s, [size][, adler]) -> n
crc32(cdata, size[, crc]) -> n
crc32(s, [size][, crc]) -> n

NOTE: Adler-32 is much faster than CRC-32B and almost as reliable.
]]

if not ... then require'gzip_test'; return end

require'glue'
local C = ffi.load'z'

cdef[[
enum {
     Z_NO_FLUSH            =  0,
     Z_FINISH              =  4,
     Z_STREAM_END          =  1,
     Z_DEFAULT_COMPRESSION = -1,
     Z_DEFAULT_STRATEGY    =  0,
     Z_DEFLATED            =  8,
     Z_MAX_WBITS           =  15,
};

typedef void*    (* z_alloc_func)( void* opaque, unsigned items, unsigned size );
typedef void     (* z_free_func) ( void* opaque, void* address );

typedef struct z_stream_s {
   const char*   next_in;
   unsigned      avail_in;
   unsigned long total_in;
   char*         next_out;
   unsigned      avail_out;
   unsigned long total_out;
   char*         msg;
   void*         state;
   z_alloc_func  zalloc;
   z_free_func   zfree;
   void*         opaque;
   int           data_type;
   unsigned long adler;
   unsigned long reserved;
} z_stream;

const char*   zlibVersion(  );
const char*   zError(      int );

int           inflate(      z_stream*, int flush );
int           inflateEnd(   z_stream* );
int           inflateInit2_(z_stream*, int windowBits, const char* version, int stream_size);

int           deflate(      z_stream*, int flush );
int           deflateEnd(   z_stream* );
int           deflateInit2_(z_stream*, int level, int method, int windowBits, int memLevel,
                            int strategy, const char *version, int stream_size );

unsigned long adler32(      unsigned long adler, const char *buf, unsigned len );
unsigned long crc32(        unsigned long crc,   const char *buf, unsigned len );
]]

function gzip_state(opt)
	local gz = update({}, opt)
	local bufsize = gz.bufsize or 64 * 1024
	--range 8..15; 0=use-value-in-zlib-header; see gzip manual.
	local windowBits = gz.windowBits or C.Z_MAX_WBITS
	if gz.format == 'gzip' then windowBits = windowBits + 16 end
	if gz.format == 'raw'  then windowBits = -windowBits end
	local strm = new'z_stream'
	local ret, flate, flate_end
	if opt.op == 'compress' then
		local level = gz.level or C.Z_DEFAULT_COMPRESSION
		local method = gz.method or C.Z_DEFLATED
		local memLevel = gz.memLevel or 8
		local strategy = gz.strategy or C.Z_DEFAULT_STRATEGY
		flate, flate_end = C.deflate, C.deflateEnd
		ret = C.deflateInit2_(strm, level, method, windowBits, memLevel,
			strategy, C.zlibVersion(), sizeof(strm))
	elseif opt.op == 'decompress' then
		flate, flate_end = C.inflate, C.inflateEnd
		ret = C.inflateInit2_(strm, windowBits, C.zlibVersion(), sizeof(strm))
	else
		assertf(false, 'invalid op: %s', opt.op)
	end
	if ret ~= 0 then --usage error
		error(str(C.zError(ret)))
	end
	gc(strm, flate_end)

	local buf = u8a(bufsize)
	strm.next_out, strm.avail_out = buf, bufsize
	strm.next_in, strm.avail_in = nil, 0
	function gz:feed(data, size)
		if not strm then return true end
		local flush = data == nil and size == 'eof' and C.Z_FINISH or C.Z_NO_FLUSH
		size = data and (size or #data) or 0
		strm.next_in = data
		strm.avail_in = size
		while true do
			local ret = flate(strm, flush)
			if not (ret == 0 or ret == C.Z_STREAM_END) then
				flate_end(gc(strm, nil))
				strm = nil
				return nil, str(C.zError(ret))
			end
			if strm.avail_out < bufsize then
				local ok, err = gz.write(buf, bufsize - strm.avail_out)
				strm.next_out, strm.avail_out = buf, bufsize
				if ok == false then
					flate_end(gc(strm, nil))
					strm = nil
					return nil, err
				end
			end
			if ret == C.Z_STREAM_END then
				flate_end(gc(strm, nil))
				strm = nil
				return true
			end
			if strm.avail_in == 0 then break end
		end
		return true
	end
	return gz
end

local function inflate_deflate(deflate, read, write, bufsize, format, windowBits, ...)

	if isstr(read) then
		local s = read
		local done
		read = function()
			if done then return end
			done = true
			return s
		end
	elseif istab(read) then
		local t = read
		local i = 0
		read = function()
			i = i + 1
			return t[i]
		end
	end

	local t
	local asstring = write == ''
	if istab(write) or asstring then
		t = asstring and {} or write
		write = function(data, sz)
			t[#t+1] = str(data, sz)
		end
	end

	bufsize = bufsize or 64 * 1024

	--range 8..15; 0=use-value-in-zlib-header; see gzip manual.
	windowBits = windowBits or C.Z_MAX_WBITS
	if format == 'gzip' then windowBits = windowBits + 16 end
	if format == 'raw'  then windowBits = -windowBits end

	local strm = new'z_stream'
	local ret, flate, flate_end
	if deflate then
		local level, method, memLevel, strategy = ...
		level = level or C.Z_DEFAULT_COMPRESSION
		method = method or C.Z_DEFLATED
		memLevel = memLevel or 8
		strategy = strategy or C.Z_DEFAULT_STRATEGY
		flate, flate_end = C.deflate, C.deflateEnd
		ret = C.deflateInit2_(strm, level, method, windowBits, memLevel,
			strategy, C.zlibVersion(), sizeof(strm))
	else
		flate, flate_end = C.inflate, C.inflateEnd
		ret = C.inflateInit2_(strm, windowBits, C.zlibVersion(), sizeof(strm))
	end
	if ret ~= 0 then
		error(str(C.zError(ret)))
	end
	gc(strm, flate_end)

	local buf = u8a(bufsize)
	strm.next_out, strm.avail_out = buf, bufsize
	strm.next_in, strm.avail_in = nil, 0

	local ok, err, ret, data, size --data must be anchored as an upvalue!
	::read::
		data, size = read()
		if data == false then
			ok, err = false, size
			goto finish
		end
		size = size or (data and #data) or 0
		strm.next_in, strm.avail_in = data, size
	::flate::
		ret = flate(strm, size > 0 and C.Z_NO_FLUSH or C.Z_FINISH)
		if not (ret == 0 or ret == C.Z_STREAM_END) then
			ok, err = false, str(C.zError(ret))
			goto finish
		end
		if strm.avail_out == bufsize then --nothing to write, need more data.
			assert(strm.avail_in == 0)
			if ret ~= C.Z_STREAM_END then goto read end
		end
	::write::
		ok, err = write(buf, bufsize - strm.avail_out)
		if ok == false then goto finish end --abort
		strm.next_out, strm.avail_out = buf, bufsize
		if ret == C.Z_STREAM_END then ok = true; goto finish end
		if strm.avail_in > 0 then goto flate end --more data to flate.
		goto read
	::finish::
		flate_end(gc(strm, nil))
		if not ok then return nil, err end
		if asstring then return table.concat(t) end
		return t or true
end
function deflate(read, write, bufsize, format, level, method, windowBits, ...)
	return inflate_deflate(true, read, write, bufsize, format, windowBits, level, method, ...)
end
function inflate(read, write, bufsize, format, windowBits)
	return inflate_deflate(false, read, write, bufsize, format, windowBits)
end

--checksum functions ---------------------------------------------------------

function adler32(data, sz, adler)
	adler = adler or C.adler32(0, nil, 0)
	return tonumber(C.adler32(adler, data, sz or #data))
end

function crc32(data, sz, crc)
	crc = crc or C.crc32(0, nil, 0)
	return tonumber(C.crc32(crc, data, sz or #data))
end
