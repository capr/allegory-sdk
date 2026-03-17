--[[

	ZLIB binding: DEFLATE compression & decompression
		* CRC32 & ADLER32 checksums.
	Written by Cosmin Apreutesei. Public Domain.

DEFLATE ----------------------------------------------------------------------

gzip_state(gz) -> gz
	OPTIONS
		gz:write(buf, sz) -> true | nil,err
			* allowed to yield.
			* signal abort by returning `nil,err`.
			* errors are not caught.
		* bufsize: output buffer size (64K).
		* format: 'gzip' (default), 'zlib' or 'raw'.
		* level: compression level (0-9 from none to best).
		* windowBits (8..15), memLevel, strategy: see zlib manual.
	API
		gz:[try_]push(s | buf,len | nil,'eof') -> true,'more'|'eof' | nil,err
		gz:[try_]finish() -> true,'eof' | nil,err
		gz:free()

gzip   (s | buf,len) -> string_buffer
gunzip (s | buf,len) -> string_buffer

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

const char* zlibVersion();
const char* zError(int);

int  inflate      (z_stream*, int flush);
int  inflateEnd   (z_stream*);
int  inflateInit2_(z_stream*, int windowBits, const char* version, int stream_size);
int  deflate      (z_stream*, int flush);
int  deflateEnd   (z_stream*);
int  deflateInit2_(z_stream*, int level, int method, int windowBits, int memLevel,
	int strategy, const char *version, int stream_size );

unsigned long adler32 (unsigned long adler, const char *buf, unsigned len );
unsigned long crc32   (unsigned long crc,   const char *buf, unsigned len );
]]

function gzip_state(gz)

	local bufsize = gz.bufsize or 64 * 1024
	--range 8..15; 0=use-value-in-zlib-header; see gzip manual.
	local windowBits = gz.windowBits or C.Z_MAX_WBITS
	local format = gz.format or 'gzip'
	if format == 'gzip' then windowBits = windowBits + 16 end
	if format == 'raw'  then windowBits = -windowBits end
	local strm = new'z_stream'
	local ret, flate, flate_end
	if gz.op == 'compress' then
		local level = gz.level or C.Z_DEFAULT_COMPRESSION
		local method = gz.method or C.Z_DEFLATED
		local memLevel = gz.memLevel or 8
		local strategy = gz.strategy or C.Z_DEFAULT_STRATEGY
		flate, flate_end = C.deflate, C.deflateEnd
		ret = C.deflateInit2_(strm, level, method, windowBits, memLevel,
			strategy, C.zlibVersion(), sizeof(strm))
	elseif gz.op == 'decompress' then
		flate, flate_end = C.inflate, C.inflateEnd
		ret = C.inflateInit2_(strm, windowBits, C.zlibVersion(), sizeof(strm))
	else
		assertf(false, 'invalid op: %s', gz.op)
	end
	if ret ~= 0 then --usage error
		error(str(C.zError(ret)))
	end
	gc(strm, flate_end)
	local function free()
		flate_end(gc(strm, nil))
		strm = nil
	end

	local buf = u8a(bufsize)
	strm.next_out, strm.avail_out = buf, bufsize
	strm.next_in, strm.avail_in = nil, 0

	function gz:try_push(data, size)
		assert(strm, 'closed')
		local flush = data == nil and size == 'eof' and C.Z_FINISH or C.Z_NO_FLUSH
		size = data and (size or #data) or 0
		strm.next_in = data
		strm.avail_in = size
		while true do
			local ret = flate(strm, flush)
			if not (ret == 0 or ret == C.Z_STREAM_END) then
				free()
				return nil, str(C.zError(ret))
			end
			if strm.avail_out < bufsize then
				local ok, err = gz.write(buf, bufsize - strm.avail_out)
				strm.next_out, strm.avail_out = buf, bufsize
				if ok == false then
					free()
					return nil, err
				end
			end
			if ret == C.Z_STREAM_END then
				gz.write(nil, 'eof')
				free()
				return true, 'eof'
			end
			if strm.avail_in == 0 then break end
		end
		return true, 'more'
	end
	function gz:push(data, size)
		return select(2, assert(self:try_push(data, size)))
	end
	function gz:try_finish()
		return self:try_push(nil, 'eof')
	end
	function gz:finish()
		return self:push(nil, 'eof')
	end

	function gz:free()
		if not strm then return end
		free()
	end

	return gz
end

local function gzip_unzip(op, data, size)
	local gz = gzip_state{op = op}
	local b = string_buffer()
	function gz.write(data, size)
		if size == 'eof' then return end
		if isstr(data) then
			b:put(data)
		else
			b:putcdata(data, size)
		end
	end
	gz:push(data, size)
	if op == 'compress' then gz:finish() end
	gz:free()
	return b
end
function gzip   (...) return gzip_unzip('compress', ...) end
function gunzip (...) return gzip_unzip('decompress', ...) end

--checksum functions ---------------------------------------------------------

function adler32(data, sz, adler)
	adler = adler or C.adler32(0, nil, 0)
	return tonumber(C.adler32(adler, data, sz or #data))
end

function crc32(data, sz, crc)
	crc = crc or C.crc32(0, nil, 0)
	return tonumber(C.crc32(crc, data, sz or #data))
end
