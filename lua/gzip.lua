--[[

	ZLIB binding, providing:
		* DEFLATE compression & decompression.
		* GZIP file compression & decompression.
		* CRC32 & ADLER32 checksums.
	Written by Cosmin Apreutesei. Public Domain.

DEFLATE ----------------------------------------------------------------------

deflate(read, write, [bufsize], [format], [level], [method], [windowBits], [memLevel], [strategy])
inflate(read, write, [bufsize], [format], [windowBits])

	Compress/decompress a data stream using the DEFLATE algorithm.

	* `read` is a function `read() -> s[,size] | cdata,size | nil | false,err`,
	  but it can also be a string or a table of strings.

	* `write` is a function `write(cdata, size) -> nil | false,err`, but it
	  can also be '' (in which case a string with the output is returned) or
	  an output table (in which case a table of output chunks is returned).

	* callbacks are allowed to yield and abort by returning `false,err`.
	* errors raised in callbacks pass-through uncaught (but don't leak).
	* `nil,err` is returned for zlib errors and callback aborts.
	* an abandoned thread suspended in read/write callbacks is gc'ed leak-free.

	* `bufsize` affects the frequency and size of the writes (defaults to 64K).
	* `format` can be 'zlib' (default), 'gzip' or 'raw'.
	* `level` controls the compression level (0-9 from none to best).
	* for `windowBits`, `memLevel` and `strategy` refer to the zlib manual.
	  * note that our `windowBits` is always in the positive range 8..15.

GZIP files -------------------------------------------------------------------

[try_]gzip_open(filename[, mode][, bufsize]) -> gzfile
gzfile:close()
gzfile:flush('none|partial|sync|full|finish|block|trees')
gzfile:read_tobuffer(buf, size) -> bytes_read
gzfile:read(size) -> s
gzfile:write(cdata, size) -> bytes_written
gzfile:write(s[, size]) -> bytes_written
gzfile:eof() -> true|false     NOTE: only true if trying to read *past* EOF!
gzfile:seek(['cur'|'set'], [offset])
	If the file is opened for reading, this function is emulated but can be
	extremely slow. If the file is opened for writing, only forward seeks are
	supported: `seek()` then compresses a sequence of zeroes up to the new
	starting position. If the file is opened for writing and the new starting
	position is before the current position, an error occurs.
	Returns the resulting offset location as measured in bytes from the
	beginning of the uncompressed stream.
gzfile:offset() -> n
	When reading, the offset does not include as yet unused buffered input.
	This information can be used for a progress indicator.

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

--zlib 1.2.7 from ufo
cdef[[
enum {
/* flush values*/
     Z_NO_FLUSH           = 0,
     Z_PARTIAL_FLUSH      = 1,
     Z_SYNC_FLUSH         = 2,
     Z_FULL_FLUSH         = 3,
     Z_FINISH             = 4,
     Z_BLOCK              = 5,
     Z_TREES              = 6,
/* return codes */
     Z_OK                 = 0,
     Z_STREAM_END         = 1,
     Z_NEED_DICT          = 2,
     Z_ERRNO              = -1,
     Z_STREAM_ERROR       = -2,
     Z_DATA_ERROR         = -3,
     Z_MEM_ERROR          = -4,
     Z_BUF_ERROR          = -5,
     Z_VERSION_ERROR      = -6,
/* compression values */
     Z_NO_COMPRESSION      =  0,
     Z_BEST_SPEED          =  1,
     Z_BEST_COMPRESSION    =  9,
     Z_DEFAULT_COMPRESSION = -1,
/* compression levels */
     Z_FILTERED            =  1,
     Z_HUFFMAN_ONLY        =  2,
     Z_RLE                 =  3,
     Z_FIXED               =  4,
     Z_DEFAULT_STRATEGY    =  0,
/* compression strategies */
     Z_BINARY              =  0,
     Z_TEXT                =  1,
     Z_ASCII               =  Z_TEXT,   /* for compatibility with 1.2.2 and earlier */
     Z_UNKNOWN             =  2,
/* Possible values of the data_type field (though see inflate()) */
     Z_DEFLATED            =  8,
/* The deflate compression method (the only one supported in this version) */
     Z_NULL                =  0,  /* for initializing zalloc, zfree, opaque */
     Z_MAX_WBITS           =  15 /* 32K LZ77 window */
};

typedef struct {int unused;} gzFile_s;
typedef gzFile_s* gzFile;

typedef void*    (* z_alloc_func)( void* opaque, unsigned items, unsigned size );
typedef void     (* z_free_func) ( void* opaque, void* address );
typedef unsigned (* z_in_func  )( void*, unsigned char*  * );
typedef int      (* z_out_func )( void*, unsigned char*, unsigned );

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

typedef struct gz_header_s {
    int           text;
    unsigned long time;
    int           xflags;
    int           os;
    char*         extra;
    unsigned      extra_len;
    unsigned      extra_max;
    char*         name;
    unsigned      name_max;
    char*         comment;
    unsigned      comm_max;
    int           hcrc;
    int           done;
} gz_header;

const char*   zlibVersion(           );
unsigned long zlibCompileFlags(      );
const char*   zError(               int );

int           inflate(              z_stream*, int flush );
int           inflateEnd(           z_stream*  );

int           inflateSetDictionary( z_stream*, const char *dictionary, unsigned dictLength);
int           inflateSync(          z_stream*  );
int           inflateCopy(          z_stream*, z_stream* source);
int           inflateReset(         z_stream*  );
int           inflateReset2(        z_stream*, int windowBits);
int           inflatePrime(         z_stream*, int bits, int value);
long          inflateMark(          z_stream*  );
int           inflateGetHeader(     z_stream*, gz_header* head);
int           inflateBack(          z_stream*, z_in_func  in,  void* in_desc,
				               z_out_func out, void* out_desc );
int           inflateBackEnd(       z_stream*  );
int           inflateInit_(         z_stream*, const char *version, int stream_size);
int           inflateInit2_(        z_stream*, int windowBits, const char* version, int stream_size);
int           inflateBackInit_(     z_stream*, int windowBits, unsigned char *window,
				               const char *version, int stream_size);
int           inflateSyncPoint(     z_stream*  );
int           inflateUndermine(     z_stream*, int );

int           deflate(              z_stream*, int flush );
int           deflateEnd(           z_stream*  );

int           deflateSetDictionary( z_stream*, const char *dictionary, unsigned dictLength );
int           deflateCopy(          z_stream*, z_stream* source );
int           deflateReset(         z_stream*  );
int           deflateParams(        z_stream*, int level, int strategy );
int           deflateTune(          z_stream*, int good_length, int max_lazy, int nice_length, int max_chain );
unsigned long deflateBound(         z_stream*, unsigned long sourceLen );
int           deflatePrime(         z_stream*, int bits, int value );
int           deflateSetHeader(     z_stream*, gz_header* head );
int           deflateInit_(         z_stream*, int level, const char *version, int stream_size);
int           deflateInit2_(        z_stream*, int level, int method, int windowBits, int memLevel,
				               int strategy, const char *version, int stream_size );

int           compress(             char *dest,   unsigned long *destLen,
			      const char *source, unsigned long sourceLen );
int           compress2(            char *dest,   unsigned long *destLen,
			      const char *source, unsigned long sourceLen, int level);
unsigned long compressBound(        unsigned long sourceLen );
int           uncompress(           char *dest,   unsigned long *destLen,
			      const char *source, unsigned long sourceLen );

gzFile        gzdopen(              int fd, const char *mode);
int           gzbuffer(             gzFile, unsigned size);
int           gzsetparams(          gzFile, int level, int strategy);
int           gzread(               gzFile, void* buf, unsigned len);
int           gzwrite(              gzFile, void const *buf, unsigned len);
int           gzprintf(             gzFile, const char *format, ...);
int           gzputs(               gzFile, const char *s);
char*         gzgets(               gzFile, char *buf, int len);
int           gzputc(               gzFile, int c);
int           gzgetc(               gzFile  );
int           gzungetc(      int c, gzFile  );
int           gzflush(              gzFile, int flush);
int           gzrewind(             gzFile  );
int           gzeof(                gzFile  );
int           gzdirect(             gzFile  );
int           gzclose(              gzFile  );
int           gzclose_r(            gzFile  );
int           gzclose_w(            gzFile  );
const char*   gzerror(              gzFile, int *errnum);
void          gzclearerr(           gzFile  );
gzFile        gzopen(               const char *, const char * );
long          gzseek(               gzFile, long, int );
long          gztell(               gzFile );
long          gzoffset(             gzFile );

unsigned long adler32(              unsigned long adler, const char *buf, unsigned len );
unsigned long crc32(                unsigned long crc,   const char *buf, unsigned len );
unsigned long adler32_combine(      unsigned long, unsigned long, long );
unsigned long crc32_combine(        unsigned long, unsigned long, long );

const unsigned long* get_crc_table( void );
]]

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

--gzip file access functions -------------------------------------------------

local function checkz(ret) assert(ret == 0) end
local function checkminus1(ret) assert(ret ~= -1); return ret end

local function gzclose(gzfile)
	checkz(C.gzclose(gzfile))
	gc(gzfile, nil)
end

function try_gzip_open(filename, mode, bufsize)
	local gzfile = C.gzopen(filename, mode or 'r')
	if gzfile == nil then
		return nil, string.format('errno %d', errno())
	end
	gc(gzfile, gzclose)
	if bufsize then C.gzbuffer(gzfile, bufsize) end
	return gzfile
end
function gzip_open(...)
	return assert(try_gzip_open(...))
end

local flush_enum = {
	none    = C.Z_NO_FLUSH,
	partial = C.Z_PARTIAL_FLUSH,
	sync    = C.Z_SYNC_FLUSH,
	full    = C.Z_FULL_FLUSH,
	finish  = C.Z_FINISH,
	block   = C.Z_BLOCK,
	trees   = C.Z_TREES,
}

local function gzflush(gzfile, flush)
	checkz(C.gzflush(gzfile, flush_enum[flush]))
end

local function gzread_tobuffer(gzfile, buf, sz)
	return checkminus1(C.gzread(gzfile, buf, sz))
end

local function gzread(gzfile, sz)
	local buf = u8a(sz)
	return str(buf, gzread_tobuffer(gzfile, buf, sz))
end

local function gzwrite(gzfile, data, sz)
	sz = C.gzwrite(gzfile, data, sz or #data)
	if sz == 0 then return nil,'error' end
	return sz
end

local function gzeof(gzfile)
	return C.gzeof(gzfile) == 1
end

local function gzseek(gzfile, ...)
	local narg = select('#',...)
	local whence, offset
	if narg == 0 then
		whence, offset = 'cur', 0
	elseif narg == 1 then
		if isstr((...)) then
			whence, offset = ..., 0
		else
			whence, offset = 'cur',...
		end
	else
		whence, offset = ...
	end
	whence = assert(whence == 'set' and 0 or whence == 'cur' and 1)
	return checkminus1(C.gzseek(gzfile, offset, whence))
end

local function gzoffset(gzfile)
	return checkminus1(C.gzoffset(gzfile))
end

metatype('gzFile_s', {__index = {
	close = gzclose,
	read = gzread,
	write = gzwrite,
	flush = gzflush,
	eof = gzeof,
	seek = gzseek,
	offset = gzoffset,
}})

--checksum functions ---------------------------------------------------------

function adler32(data, sz, adler)
	adler = adler or C.adler32(0, nil, 0)
	return tonumber(C.adler32(adler, data, sz or #data))
end

function crc32(data, sz, crc)
	crc = crc or C.crc32(0, nil, 0)
	return tonumber(C.crc32(crc, data, sz or #data))
end
