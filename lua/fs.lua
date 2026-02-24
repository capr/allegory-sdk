--[=[

	Filesystem API for Linux.
	Written by Cosmin Apreutesei. Public Domain.

FEATURES
  * utf8 filenames
  * symlinks and hard links
  * memory mapping
  * cdata buffer-based I/O

FILE OBJECTS
	[try_]open(opt | path,[mode],[quiet]) -> f    open file
	f:[try_]close()                               close file
	f:closed() -> true|false                      check if file is closed
	isfile(f [,'file'|'pipe']) -> true|false      check if f is a file or pipe
	f.fd -> fd                                    POSIX file descriptor
PIPES
	[try_]pipe([opt]) -> rf, wf                   create an anonymous pipe
	[try_]pipe(path|{path=,...}) -> pf            create/open a named pipe
	[try_]mkfifo(path|{path=,...}) -> true        create a named pipe (POSIX)
STDIO STREAMS
	f:stream(mode) -> fs                          open a FILE* object from a file
	fs:[try_]close()                              close the FILE* object
MEMORY STREAMS
	open_buffer(buf, [size], [mode]) -> f         create a memory stream
FILE I/O
	f:[try_]read(buf, len) -> readlen             read data from file
	f:[try_]readn(buf, n) -> buf, n               read exactly n bytes
	f:[try_]readall([ignore_file_size]) -> buf, len    read until EOF into a buffer
	f:[try_]write(s | buf,len) -> true            write data to file
	f:[try_]flush()                               flush buffers
	f:[try_]seek([whence] [, offset]) -> pos      get/set the file pointer
	f:[try_]skip(n) -> n                          skip bytes
	f:[try_]truncate([opt])                       truncate file to current file pointer
	f:[un]buffered_reader([bufsize]) -> read(buf, sz)   get read(buf, sz)
OPEN FILE ATTRIBUTES
	f:attr([attr]) -> val|t                       get/set attribute(s) of open file
	f:size() -> n                                 get file size
DIRECTORY LISTING
	ls(dir, [opt]) -> d, name, next               directory contents iterator
	  d:next() -> name, d                         call the iterator explicitly
	  d:[try_]close()                             close iterator
	  d:closed() -> true|false                    check if iterator is closed
	  d:name() -> s                               dir entry's name
	  d:dir() -> s                                dir that was passed to ls()
	  d:path() -> s                               full path of the dir entry
	  d:attr([attr, ][deref]) -> t|val            get/set dir entry attribute(s)
	  d:is(type, [deref]) -> t|f                  check if dir entry is of type
	scandir(path|{path1,...}, [dive]) -> iter() -> sc     recursive dir iterator
	  sc:close()
	  sc:closed() -> true|false
	  sc:name([depth]) -> s
	  sc:dir([depth]) -> s
	  sc:path([depth]) -> s
	  sc:relpath([depth]) -> s
	  sc:attr([attr, ][deref]) -> t|val
	  sc:depth([n]) -> n (from 1)
FILE ATTRIBUTES
	[try_]file_attr(path, [attr, ][deref]) -> t|val     get/set file attribute(s)
	file_is(path, [type], [deref]) -> t|f,['not_found'] check if file exists or is of a certain type
	exists                                      = file_is
	checkexists(path, [type], [deref])            assert that file exists
	[try_]mtime(path, [deref]) -> ts              get file's modification time
	[try_]chmod(path, perms, [quiet]) -> path     change a file or dir's permissions
FILESYSTEM OPS
	cwd() -> path                                 get current working directory
	abspath(path[, cwd]) -> path                  convert path to absolute path
	startcwd() -> path                            get the cwd that process started with
	[try_]chdir(path)                             set current working directory
	run_indir(dir, fn)                            run function in specified cwd
	[try_]mkdir(dir, [recursive], [perms], [quiet]) -> dir    make directory
	[try_]rm[dir|file](path, [quiet])             remove directory or file
	[try_]rm_rf(path, [quiet])                    like `rm -rf`
	[try_]mkdirs(file, [perms], [quiet]) -> file     make file's dir
	[try_]mv(old_path, new_path, [dst_dirs_perms], [quiet])   rename/move file or dir on the same filesystem
SYMLINKS & HARDLINKS
	[try_]mksymlink(symlink, path, [quiet])       create a symbolic link for a file or dir
	[try_]mkhardlink(hardlink, path, [quiet])     create a hard link for a file
	[try_]readlink(path) -> path                  dereference a symlink recursively
COMMON PATHS
	homedir() -> path                             get current user's home directory
	tmpdir() -> path                              get the temporary directory
	exepath() -> path                             get the full path of the running executable
	exedir() -> path                              get the directory of the running executable
	appdir([appname]) -> path                     get the current user's app data dir
	scriptdir() -> path                           get the directory of the main script
	vardir() -> path                              get script's private r/w directory
	varpath(...) -> path                          get vardir-relative path
LOW LEVEL
	file_wrap_fd(fd, [opt], ...) -> f             wrap opened file descriptor
	file_wrap_file(FILE*, [opt], ...) -> f        wrap opened FILE* object
	fileno(FILE*) -> fd                           get stream's file descriptor
MEMORY MAPPING
	mmap(...) -> map                              create a memory mapping
	f:map([offset],[size],[addr],[access]) -> map   create a memory mapping
	map.addr                                      a void* pointer to the mapped memory
	map.size                                      size of the mapped memory in bytes
	map:flush([async, ][addr, size])              flush (parts of) the mapping to disk
	map:free()                                    release the memory and associated resources
	unlink_mapfile(tagname)                       remove the shared memory file from disk
	map:unlink()
	mirror_buffer([size], [addr]) -> map          create a mirrored memory-mapped ring buffer
	pagesize() -> bytes                           get allocation granularity
	aligned_size(bytes[, dir]) -> bytes           next/prev page-aligned size
	aligned_addr(ptr[, dir]) -> ptr               next/prev page-aligned address
FILESYSTEM INFO
	fs_info(path) -> {size=, free=}               get free/total disk space for a path
HI-LEVEL APIs
	[try_]load[_tobuffer](path, [default], [ignore_fsize]) -> buf,len  read file to string or buffer
	[try_]save(path, s, [sz], [perms], [quiet])   atomic save value/buffer/array/read-results
	file_saver(path) -> f(v | buf,len | t | read) atomic save writer function
	touch(file, [mtime], [btime], [quiet])
	cp(src_file, dst_file)

The `deref` arg is true by default, meaning that by default, symlinks are
followed recursively and transparently where this option is available.

FILE ATTRIBUTES

 attr     | R/W | Description
 ---------+-----+--------------------------------
 type     | r   | file type (see below)
 size     | r   | file size
 atime    | rw  | last access time (seldom correct)
 mtime    | rw  | last contents-change time
 ctime    | r   | last metadata-or-contents-change time
 target   | r   | symlink's target (nil if not symlink)
 perms    | rw  | permissions
 uid      | rw  | user id or name
 gid      | rw  | group id or name
 dev      | r   | device id containing the file
 inode    | r   | inode number (int64_t)
 nlink    | r   | number of hard links
 rdev     | r   | device id (if special file)
 blksize  | r   | block size for I/O
 blocks   | r   | number of 512B blocks allocated

On the table above, `r` means that the attribute is read/only and `rw` means
that the attribute can be changed. Attributes can be queried and changed via
`f:attr()`, `file_attr()` and `d:attr()`.

NOTE: File sizes and offsets are Lua numbers not 64bit ints, so they can hold
at most 8KTB.

FILE TYPES

 name      | description
 ----------+---------------------------------
 file      | file is a regular file
 dir       | file is a directory
 symlink   | file is a symlink
 blockdev  | file is a block device
 chardev   | file is a character device
 pipe      | file is a pipe
 socket    | file is a socket
 unknown   | file type unknown

NORMALIZED ERROR MESSAGES

	not_found          file/dir/path not found
	io_error           I/O error
	access_denied      access denied
	already_exists     file/dir already exists
	is_dir             trying this on a directory
	not_empty          dir not empty (for remove())
	io_error           I/O error
	disk_full          no space left on device

File Objects -----------------------------------------------------------------

[try_]open(opt | path,[mode],[quiet]) -> f

Open/create a file for reading and/or writing. The second arg can be a string:

	'r'  : open; allow reading only (default)
	'r+' : open; allow reading and writing
	'w'  : open and truncate or create; allow writing only
	'w+' : open and truncate or create; allow reading and writing
	'a'  : open and seek to end or create; allow writing only
	'a+' : open and seek to end or create; allow reading and writing

	... or an options table with platform-specific options which represent
	OR-ed bitmask flags which must be given either as 'foo bar ...',
	{foo=true, bar=true} or {'foo', 'bar'}.
	All fields and flags are documented in the code.

 field       | reference                            | default
 ------------+--------------------------------------+----------
 flags       | open() / flags                       | 'rdonly'
 perms       | octal or symbolic perms              | '0666' / 'rwx'
 inheritable | all        | sub-processes inherit the fd/handle  | false

The `perms` arg is passed to unixperms_parse().

The `inheritable` flag is false by default on both files and pipes
to prevent leaking them to sub-processes.

Pipes ------------------------------------------------------------------------

[try_]pipe([opt]) -> rf, wf

	Create an anonymous (unnamed) pipe. Return two files corresponding to the
	read and write ends of the pipe.

	Options:
		* `inheritable`, `read_inheritable`, `write_inheritable`: make one
		or both pipes inheritable by sub-processes.

[try_]pipe(path|{path=,...}) -> pf

	Create and open a named pipe.

[try_]mkfifo(path, [perms], [quiet]) -> true[,'already_exists']

	Create a named pipe.

Stdio Streams ----------------------------------------------------------------

f:stream(mode) -> fs

	Open a `FILE*` object from a file. The file should not be used anymore while
	a stream is open on it and `fs:close()` should be called to close the file.

fs:[try_]close()

	Close the `FILE*` object and the underlying file object.

Memory Streams ---------------------------------------------------------------

open_buffer(buf, [size], [mode]) -> f

	Create a memory stream for reading and writing data from and into a buffer
	using the file API. Only opening modes 'r' and 'w' are supported.

File I/O ---------------------------------------------------------------------

f:[try_]read(buf, len) -> readlen

	Read data from file. Returns (and keeps returning) 0 on EOF or broken pipe.

f:[try_]readn(buf, len) -> buf, len

	Read data from file until `len` is read.
	Partial reads are signaled with `nil, err, readlen`.

f:[try_]readall() -> buf, len

	Read until EOF into a buffer.

f:[try_]write(s | buf,len) -> true

	Write data to file.
	Partial writes are signaled with `nil, err, writelen`.

f:[try_]flush()

	Flush buffers.

f:[try_]seek([whence] [, offset]) -> pos

	Get/set the file pointer. Same semantics as standard `io` module seek
	i.e. `whence` defaults to `'cur'` and `offset` defaults to `0`.

f:[try_]truncate(size, [opt])

	Truncate file to given `size` and move the current file pointer to `EOF`.
	This can be done both to shorten a file and thus free disk space, or to
	preallocate disk space to be subsequently filled (eg. when downloading a file).

	`opt` is an optional string which can contain any of the words
	`fallocate` (call `fallocate()`) and `fail` (do not call `ftruncate()`
	if `fallocate()` fails: return an error instead). The problem with calling
	`ftruncate()` if `fallocate()` fails is that on most filesystems, that
	creates a sparse file which doesn't help if what you want is to actually
	reserve space on the disk, hence the `fail` option. The default is
	`'fallocate fail'` which should never create a sparse file, but it can be
	slow on some file systems (when it's emulated) or it can just fail
	(like on virtual filesystems).

	Btw, seeking past EOF and writing something there will also create a sparse
	file, so there's no easy way out of this complexity.

f:[un]buffered_reader([bufsize]) -> read(buf, len)

	Returns a `read(buf, len) -> readlen` function which reads ahead from file
	in order to lower the number of syscalls. `bufsize` specifies the buffer's
	size (default is 64K). The unbuffered version doesn't use a buffer.

Open file attributes ---------------------------------------------------------

f:attr([attr]) -> val|t

	Get/set attribute(s) of open file. `attr` can be:
	* nothing/nil: get the values of all attributes in a table.
	* string: get the value of a single attribute.
	* table: set one or more attributes.

Directory listing ------------------------------------------------------------

ls([dir], [opt]) -> d, next

	Directory contents iterator. `dir` defaults to '.'.
	`opt` is a string that can include:
		* `..`   :  include `.` and `..` dir entries (excluded by default).

	USAGE

		for name, d in ls() do
			if not name then
				print('error: ', d)
				break
			end
			print(d:attr'type', name)
		end

	Always include the `if not name` condition when iterating. The iterator
	doesn't raise any errors. Instead it returns `false, err` as the
	last iteration when encountering an error. Initial errors from calling
	`ls()` (eg. `'not_found'`) are passed to the iterator also, so the
	iterator must be called at least once to see them.

	d:next() -> name, d | false, err | nil

		Call the iterator explicitly.

	d:close()

		Close the iterator. Always call `d:close()` before breaking the for loop
		except when it's an error (in which case `d` holds the error message).

	d:closed() -> true|false

		Check if the iterator is closed.

	d:name() -> s

		The name of the current file or directory being iterated.

	d:dir() -> s

		The directory that was passed to `ls()`.

	d:path() -> s

		The full path of the current dir entry (`d:dir()` combined with `d:name()`).

	d:attr([attr, ][deref]) -> t|val

		Get/set dir entry attribute(s).

		`deref` means return the attribute(s) of the symlink's target if the file is
		a symlink (`deref` defaults to `true`!). When `deref=true`, even the `'type'`
		attribute is the type of the target, so it will never be `'symlink'`.

		Some attributes for directory entries are free to get (but not for symlinks
		when `deref=true`) meaning that they don't require a system call for each
		file, notably `type`, `atime`, `mtime`, `size` and `inode`.

	d:is(type, [deref]) -> true|false

		Check if dir entry is of type.

scandir(path|{path1,...}, [dive]) -> iter() -> sc

	Recursive dir walker. All sc methods return `nil,err` if an error occured
	on the current dir entry, but the iteration otherwise continues, unless
	you call close() to stop it.
	* `depth` arg can be 0=sc:depth(), 1=first-level, -1=parent-level, etc.
	* `dive(sc) -> true` is an optional filter to skip from diving into dirs.

	sc:close()
	sc:closed() -> true|false
	sc:name([depth]) -> s
	sc:dir([depth]) -> s
	sc:path([depth]) -> s
	sc:relpath([depth]) -> s
	sc:attr([attr, ][deref]) -> t|val
	sc:depth([n]) -> n (from 1)

File attributes --------------------------------------------------------------

[try_]file_attr(path, [attr, ][deref]) -> t|val

	Get/set a file's attribute(s) given its path in utf8.

file_is(path, [type], [deref]) -> true|false, ['not_found']

	Check if file exists or if it is of a certain type.

Filesystem operations --------------------------------------------------------

mkdir(path, [recursive], [perms])

	Make directory. `perms` can be a number or a string passed to unixperms_parse().

	NOTE: In recursive mode, if the directory already exists this function
	returns `true, 'already_exists'`.

fileremove(path, [recursive])

	Remove a file or directory (recursively if `recursive=true`).

filemove(path, newpath, [opt])

	Rename/move a file on the same filesystem.

	This operation is atomic.

Symlinks & Hardlinks ---------------------------------------------------------

[try_]readlink(path) -> path

	Dereference a symlink recursively. The result can be an absolute or
	relative path which can be valid or not.

Memory Mapping ---------------------------------------------------------------

	FEATURES
	  * file-backed and pagefile-backed (anonymous) memory maps
	  * read-only, read/write and copy-on-write access modes plus executable flag
	  * name-tagged memory maps for sharing memory between processes
	  * mirrored memory maps for using with lock-free ring buffers.
	  * synchronous and asynchronous flushing

	LIMITATIONS
	  * I/O errors from accessing mmapped memory cause a crash (and there's
	  nothing that can be done about that with the current ffi), which makes
	  this API unsuitable for mapping files from removable media or recovering
	  from write failures in general. For all other uses it is fine.

[try_]mmap(args_t) -> map
[try_]mmap(path, [access], [size], [offset], [addr], [tagname], [perms]) -> map
f:[try_]map([offset], [size], [addr], [access])

	Create a memory map object. Args:

	* `path`: the file to map: optional; if nil, a portion of the system pagefile
	will be mapped instead.
	* `access`: can be either:
		* '' (read-only, default)
		* 'w' (read + write)
		* 'c' (read + copy-on-write)
		* 'x' (read + execute)
		* 'wx' (read + write + execute)
		* 'cx' (read + copy-on-write + execute)
	* `size`: the size of the memory segment (optional, defaults to file size).
		* if given it must be > 0 or an error is raised.
		* if not given, file size is assumed.
			* if the file size is zero the mapping fails with `'file_too_short'`.
		* if the file doesn't exist:
			* if write access is given, the file is created.
			* if write access is not given, the mapping fails with `'not_found'` error.
		* if the file is shorter than the required offset + size:
			* if write access is not given (or the file is the pagefile which
			can't be resized), the mapping fails with `'file_too_short'` error.
			* if write access is given, the file is extended.
				* if the disk is full, the mapping fails with `'disk_full'` error.
	* `offset`: offset in the file (optional, defaults to 0).
		* if given, must be >= 0 or an error is raised.
		* must be aligned to a page boundary or an error is raised.
		* ignored when mapping the pagefile.
	* `addr`: address to use (optional; an error is raised if zero).
		* it's best to provide an address that is above 4 GB to avoid starving
		LuaJIT which can only allocate in the lower 4 GB of the address space.
	* `tagname`: name of the memory map (optional; cannot be used with `file`;
		must not contain slashes or backslashes).
		* using the same name in two different processes (or in the same process)
		gives access to the same memory.

	Returns an object with the fields:

	* `addr` - a `void*` pointer to the mapped memory
	* `size` - the actual size of the memory block

	If the mapping fails, returns `nil,err` where `err` can be:

	* `'not_found'` - file not found.
	* `'file_too_short'` - the file is shorter than the required size.
	* `'disk_full'` - the file cannot be extended because the disk is full.
	* `'out_of_mem'` - size or address too large or specified address in use.
	* an OS-specific error message.

NOTES

	* when mapping or resizing a `FILE` that was written to, the write buffers
	should be flushed first.
	* after mapping an opened file handle of any kind, that file handle should
	not be used anymore except to close it after the mapping is freed.
	* attempting to write to a memory block that wasn't mapped with write
	or copy-on-write access results in a crash.
	* changes done externally to a mapped file may not be visible immediately
	(or at all) to the mapped memory.
	* access to shared memory from multiple processes must be synchronized.

map:free()

	Free the memory and all associated resources and close the file
	if it was opened by the `mmap()` call.

map:[try_]flush([async, ][addr, size]) -> true | nil,err

	Flush (part of) the memory to disk. If the address is not aligned,
	it will be automatically aligned to the left. If `async` is true,
	perform the operation asynchronously and return immediately.

unlink_mapfile(tagname)` <br> `map:unlink()

	Remove a (the) shared memory file from disk. When creating a shared memory
	mapping using `tagname`, a file is created on the filesystem. That file
	must be removed manually when it is no longer needed. This can be done
	anytime, even while mappings are open and will not affect said mappings.

mirror_buffer([size], [addr]) -> map

	Create a mirrored buffer to use with a lock-free ring buffer. Args:
	* `size`: the size of the memory segment (optional; one page size
	  by default. automatically aligned to the next page size).
	* `addr`: address to use (optional; can be anything convertible to `void*`).

	The result is a table with `addr` and `size` fields and all the mirror map
	objects in its array part (freeing the mirror will free all the maps).
	The memory block at `addr` is mirrored such that
	`(char*)addr[i] == (char*)addr[size+i]` for any `i` in `0..size-1`.

aligned_size(bytes[, dir]) -> bytes

	Get the next larger (dir = 'right', default) or smaller (dir = 'left') size
	that is aligned to a page boundary. It can be used to align offsets and sizes.

aligned_addr(ptr[, dir]) -> ptr

	Get the next (dir = 'right', default) or previous (dir = 'left') address that
	is aligned to a page boundary. It can be used to align pointers.

pagesize() -> bytes

	Get the current page size. Memory will always be allocated in multiples
	of this size and file offsets must be aligned to this size too.

Async I/O --------------------------------------------------------------------

Pipes are opened in async mode by default, which uses the sock scheduler
to multiplex the I/O which means that all I/O must be performed inside
sock threads.

Programming Notes ------------------------------------------------------------

### Filesystem operations are non-atomic

Most filesystem operations are non-atomic (unless otherwise specified) and
thus prone to race conditions. This library makes no attempt at fixing that
and in fact it ignores the issue entirely in order to provide a simpler API.
So never work on the (same part of the) filesystem from multiple processes
without proper locking (watch Niall Douglas's "Racing The File System"
presentation for more info).

### Flushing does not protect against power loss

Flushing does not protect against power loss on consumer hard drives because
they usually don't have non-volatile write caches (and disabling the write
cache is generally not possible nor feasible). The only way to ensure
durability after flush is to use drives with Power Loss Protection (PLP).

### File locking doesn't always work

File locking APIs only work right on disk mounts and are buggy or non-existent
on network mounts (NFS, Samba).

### Async disk I/O

Async disk I/O is a complete afterthought on all major Operating Systems.
If your app is disk-bound just bite the bullet and make a thread pool.
Read Arvid Norberg's article[1] for more info.

[1] https://blog.libtorrent.org/2012/10/asynchronous-disk-io/

]=]

if not ... then require'fs_test'; return end

require'glue'
require'path'
require'unixperms'

--POSIX does not define an ABI and platfoms have different cdefs thus we have
--to limit support to the platforms and architectures we actually tested for.
assert(Linux, 'platform not Linux')

local
	C, min, max, floor, ceil, ln, push, pop, istab, isstr =
	C, min, max, floor, ceil, ln, push, pop, istab, isstr

local
	cast, bor, band, bnot, shl, check, check_errno =
	cast, bor, band, bnot, shl, check, check_errno

local file = {}; file.__index = file --file object methods
local stream = {}; stream.__index = stream --FILE methods
local dir = {}; dir.__index = dir --dir listing object methods

--types, consts, utils -------------------------------------------------------

cdef[[
typedef size_t ssize_t; // for older luajit
typedef unsigned int mode_t;
typedef unsigned int uid_t;
typedef unsigned int gid_t;
typedef size_t time_t;
typedef int64_t off64_t;
]]

cdef'int fcntl(int fd, int cmd, ...);' --fallocate, set_inheritable

cdef'long syscall(int number, ...);' --stat, fstat, lstat

local cbuf = buffer'char[?]'

local default_file_perms = tonumber('644', 8)
local default_dir_perms  = tonumber('755', 8)

local function parse_perms(s, base)
	if isstr(s) then
		return unixperms_parse(s, base)
	else --pass-through
		return s or default_file_perms, false
	end
end

--open/close -----------------------------------------------------------------

cdef[[
int open(const char *pathname, int flags, mode_t mode);
int close(int fd);
]]

local o_bits = {
	rdonly    = 0x000000, --access: read only
	wronly    = 0x000001, --access: write only
	rdwr      = 0x000002, --access: read + write
	accmode   = 0x000003, --access: ioctl() only
	append    = 0x000400, --append mode: write() at eof
	trunc     = 0x000200, --truncate the file on opening
	creat     = 0x000040, --create if not exist
	excl      = 0x000080, --create or fail (needs 'creat')
	nofollow  = 0x020000, --fail if file is a symlink
	directory = 0x010000, --open if directory or fail
	async     = 0x002000, --enable signal-driven I/O
	sync      = 0x101000, --enable _file_ sync
	fsync     = 0x101000, --'sync'
	dsync     = 0x001000, --enable _data_ sync
	noctty    = 0x000100, --prevent becoming ctty
	direct    = 0x004000, --don't cache writes
	noatime   = 0x040000, --don't update atime
	rsync     = 0x101000, --'sync'
	path      = 0x200000, --open only for fd-level ops
   tmpfile   = 0x410000, --create anon temp file (Linux 3.11+)
}

local open_mode_opt = {
	['r' ] = {flags = 'rdonly'},
	['r+'] = {flags = 'rdwr'},
	['w' ] = {flags = 'creat wronly trunc'},
	['w+'] = {flags = 'creat rdwr'},
	['a' ] = {flags = 'creat wronly', seek_end = true},
	['a+'] = {flags = 'creat rdwr', seek_end = true},
}

local F_GETFL     = 3
local F_SETFL     = 4
local O_NONBLOCK  = 0x000800 --async I/O
local O_CLOEXEC   = 0x080000 --close-on-exec

local F_GETFD = 1
local F_SETFD = 2
local FD_CLOEXEC = 1

local function fcntl_set_flags_func(GET, SET)
	return function(f, mask, bits)
		local cur_bits = C.fcntl(f.fd, GET)
		local bits = setbits(cur_bits, mask, bits)
		assert(check_errno(C.fcntl(f.fd, SET, cast('int', bits)) == 0))
	end
end
local fcntl_set_fl_flags = fcntl_set_flags_func(F_GETFL, F_SETFL)
local fcntl_set_fd_flags = fcntl_set_flags_func(F_GETFD, F_SETFD)

function file_wrap_fd(fd, opt, async, file_type, path, quiet, debug_prefix)

	file_type = file_type or 'file'

	--make `if f.seek then` the idiom for checking if a file is seekable.
	local seek; if file_type ~= 'file' or async then seek = false end

	local f = object(file, {
		fd = fd,
		s = fd, --for async use with sock
		type = file_type,
		seek = seek,
		debug_prefix = debug_prefix
			or file_type == 'file' and 'F'
			or file_type == 'pipe' and 'P'
			or file_type == 'pidfile' and 'D',
		w = 0, r = 0,
		quiet = repl(quiet, nil, file_type == 'pipe' or nil), --pipes are quiet
		path = path,
		async = async,
	}, opt)
	live(f, f.path or '')

	if f.async then
		fcntl_set_fl_flags(f, O_NONBLOCK, O_NONBLOCK)
		local ok, err = _sock_register(f)
		if not ok then
			assert(f:close())
			return nil, err
		end
	end

	return f
end

local function _open(path, opt, quiet, file_type)
	local async = opt.async --files are sync by defualt
	local flags = bitflags(opt.flags or 'rdonly', o_bits)
	flags = bor(flags, async and O_NONBLOCK or 0)
	if not opt.inheritable then
		flags = bor(flags, O_CLOEXEC)
	end
	local r = band(flags, o_bits.rdonly) == o_bits.rdonly
	local w = band(flags, o_bits.wronly) == o_bits.wronly
	quiet = repl(quiet, nil, not w or nil) --r/o opens are quiet
	local perms = parse_perms(opt.perms)
	local open = opt.open or C.open
	local fd = open(path, flags, perms)
	if fd == -1 then
		return check_errno()
	end
	local f, err = file_wrap_fd(fd, opt, async, file_type, path, quiet)
	if not f then
		return nil, err
	end
	log(f.quiet and '' or 'note', 'fs', 'open',
		'%-4s %s%s %s fd=%d', f, r and 'r' or '', w and 'w' or '', path, fd)

	if opt.seek_end then
		local pos, err = f:seek('end', 0)
		if not pos then
			assert(f:close())
			return nil, err
		end
	end

	return f
end

function file.closed(f)
	return f.fd == -1
end

function file.try_close(f)
	if f:closed() then return true end
	if f.async then
		_sock_unregister(f)
	end
	local ok, err = check_errno(C.close(f.fd) == 0)
	f.fd = -1 --fd is gone no matter the error.
	if f._after_close then
		f:_after_close()
	end
	_sock_cancel_wait_io(f)
	if not ok then return ok, err end
	log(f.quiet and '' or 'note', 'fs', 'closed', '%-4s r:%d w:%d', f, f.r, f.w)
	live(f, nil)
	return true
end

function file:onclose(fn)
	after(self, '_after_close', fn)
end

cdef[[
int fileno(struct FILE *stream);
]]

function fileno(file)
	local fd = C.fileno(file)
	if fd == -1 then return check_errno() end
	return fd
end

function file_wrap_file(file, opt)
	local fd = C.fileno(file)
	if fd == -1 then return check_errno() end
	return file_wrap_fd(fd, opt)
end

function file.set_inheritable(file, inheritable)
	fcntl_set_fd_flags(file, FD_CLOEXEC, inheritable and 0 or FD_CLOEXEC)
end

--file objects ---------------------------------------------------------------

function isfile(f, type)
	local mt = getmetatable(f)
	return istab(mt) and rawget(mt, '__index') == file and (not type or f.type == type)
end

function try_open(path, mode, quiet)
	local opt
	if istab(path) then --try_open{path=,...}
		opt = path
		path = opt.path
		mode = opt.mode
		quiet = opt.quiet
	end
	assert(isstr(path), 'path required')
	mode = repl(mode, nil, 'r') --use `false` for no mode.
	if mode then
		local mode_opt = assertf(open_mode_opt[mode], 'invalid open mode: %s', mode)
		if opt then
			merge(opt, mode_opt)
		else
			opt = mode_opt
		end
	end
	local f, err = _open(path, opt or empty, quiet)
	if not f then return nil, err end
	return f
end

function open(arg1, ...)
	local f, err = try_open(arg1, ...)
	local path = isstr(arg1) and arg1 or arg1.path
	return check('fs', 'open', f, '%s: %s', path, err)
end

file.check_io = check_io
file.checkp   = checkp

function file:try_skip(n)
	local i, err = f:try_seek('cur', 0); if not i then return nil, err end
	local j, err = f:try_seek('cur', n); if not i then return nil, err end
	return j - i
end

function file.unbuffered_reader(f)
	return function(buf, sz)
		if not buf then --skip bytes (libjpeg semantics)
			return f:skip(sz)
		else
			return f:read(buf, sz)
		end
	end
end

function file.buffered_reader(f, bufsize)
	local ptr_ct = u8p
	local buf_ct = u8a
	local o1, err = f:size()
	local o0, err = f:try_seek'cur'
	if not (o0 and o1) then
		return function() return nil, err end
	end
	local bufsize = min(bufsize or 64 * 1024, o1 - o0)
	local buf = buf_ct(bufsize)
	local ofs, len = 0, 0
	local eof = false
	return function(dst, sz)
		if not dst then --skip bytes (libjpeg semantics)
			return f:skip(sz)
		end
		local rsz = 0
		while sz > 0 do
			if len == 0 then
				if eof then
					return 0
				end
				ofs = 0
				local len1, err = f:read(buf, bufsize)
				if not len1 then return nil, err end
				len = len1
				if len == 0 then
					eof = true
					return rsz
				end
			end
			--TODO: benchmark: read less instead of copying.
			local n = min(sz, len)
			copy(cast(ptr_ct, dst) + rsz, buf + ofs, n)
			ofs = ofs + n
			len = len - n
			rsz = rsz + n
			sz = sz - n
		end
		return rsz
	end
end

--pipes ----------------------------------------------------------------------

cdef[[
int pipe2(int[2], int flags);
int mkfifo(const char *pathname, mode_t mode);
]]

function try_mkfifo(path, perms, quiet)
	perms = parse_perms(perms)
	local ok, err = check_errno(C.mkfifo(path, perms) == 0)
	if not ok and err ~= 'already_exists' then return nil, err end
	log(quiet and '' or 'note', 'fs', 'mkfifo', '%s %o', path, perms)
	if err == 'already_exists' then return true, err end
	return ok
end

function mkfifo(path, perms)
	perms = parse_perms(perms)
	local ok, err = try_mkfifo(path, perms)
	check('fs', 'mkfifo', ok, '%s %o', path, perms)
	if err then return ok, err end
	return ok
end

local function _pipe(path, opt)
	local async = repl(opt.async, nil, true) --pipes are async by default
	if path then --named pipe
		local ok, err = try_mkfifo(path, perms, opt.quiet)
		if not ok then return nil, err end
		return _open(path, update({
			async = async,
		}, opt), true, 'pipe')
	else --unnamed pipe
		local fds = new'int[2]'
		local flags = not opt.inheritable and O_CLOEXEC or 0
		local ok = C.pipe2(fds, flags) == 0
		if not ok then return check_errno() end
		local r_async = repl(opt.async_read , nil, async)
		local w_async = repl(opt.async_write, nil, async)
		local rf, err1 = file_wrap_fd(fds[0], opt, r_async, 'pipe', 'pipe.r')
		local wf, err2 = file_wrap_fd(fds[1], opt, w_async, 'pipe', 'pipe.w')
		if not (rf and wf) then
			if rf then assert(rf:close()) end
			if wf then assert(wf:close()) end
			return nil, err1 or err2
		end
		if not opt.inheritable then
			if opt. read_inheritable then rf:set_inheritable(true) end
			if opt.write_inheritable then wf:set_inheritable(true) end
		end
		log(rf.quiet and '' or 'note',
			'fs', 'pipe', 'r=%s%s w=%s%s rfd=%d wfd=%d',
			rf, rf.async and '' or ',blocking',
			wf, wf.async and '' or ',blocking', rf.fd, wf.fd)
		return rf, wf
	end
end

local function pipe_args(path_opt)
	if istab(path_opt) then
		return path_opt.path, path_opt
	else
		return path_opt, empty
	end
end

function try_pipe(...)
	return _pipe(pipe_args(...))
end

function pipe(...)
	local path, opt = pipe_args(...)
	local ret, err = _pipe(path, opt)
	check('fs', 'pipe', ret, '%s: %s', path or '', err)
	if not path then return ret, err end --actually rf, wf
	return ret --pf
end

--stdio streams --------------------------------------------------------------

cdef[[
typedef struct FILE FILE;
FILE *fdopen(int fd, const char *mode);
int fclose(FILE*);
]]

stream_ct = ctype'struct FILE'

function stream.try_close(fs)
	local ok = C.fclose(fs) == 0
	if not ok then return check_errno(false) end
	return true
end
stream.close = unprotect_io(stream.try_close)

function file.stream(f, mode)
	local fs = C.fdopen(f.fd, mode)
	if fs == nil then return check_errno() end
	return fs
end

--i/o ------------------------------------------------------------------------

cdef[[
ssize_t read(int fd, void *buf, size_t count);
ssize_t write(int fd, const void *buf, size_t count);
int fsync(int fd);
int64_t lseek(int fd, int64_t offset, int whence) asm("lseek64");
]]

--NOTE: always ask for more than 0 bytes from a pipe or you'll not see EOF.
function file.try_read(f, buf, sz)
	if sz == 0 then return 0 end --masked for compat.
	if f.async then
		return _file_async_read(f, buf, sz)
	else
		local n = C.read(f.fd, buf, sz)
		if n == -1 then return check_errno() end
		n = tonumber(n)
		f.r = f.r + n
		return n
	end
end

function file._write(f, buf, sz)
	if f.async then
		return _file_async_write(f, buf, sz)
	else
		local n = C.write(f.fd, buf, sz or #buf)
		if n == -1 then return check_errno() end
		n = tonumber(n)
		f.w = f.w + n
		return n
	end
end

function file.try_flush(f)
	return check_errno(C.fsync(f.fd) == 0)
end

function file:setexpires(rw, expires)
	if not isstr(rw) then rw, expires = nil, rw end
	local r = rw == 'r' or not rw
	local w = rw == 'w' or not rw
	if r then self.recv_expires = expires end
	if w then self.send_expires = expires end
end
function file:settimeout(s, rw)
	self:setexpires(s and clock() + s, rw)
end

local whences = {set = 0, cur = 1, ['end'] = 2} --FILE_*
function file:try_seek(whence, offset)
	if tonumber(whence) and not offset then --middle arg missing
		whence, offset = 'cur', tonumber(whence)
	end
	whence = whence or 'cur'
	offset = tonumber(offset or 0)
	whence = assertf(whences[whence], 'invalid whence: "%s"', whence)
	local offs = C.lseek(self.fd, offset, whence)
	if offs == -1 then return check_errno() end
	return tonumber(offs)
end

function file:try_write(buf, sz)
	sz = sz or #buf
	if sz == 0 then return true end --mask out null writes
	local sz0 = sz
	while true do
		local len, err = self:_write(buf, sz)
		if len == sz then
			break
		elseif not len then --short write
			return nil, err, sz0 - sz
		end
		assert(len > 0)
		if isstr(buf) then --only make pointer on the rare second iteration.
			buf = cast(u8p, buf)
		end
		buf = buf + len
		sz  = sz  - len
	end
	return true
end

function file:try_readn(buf, sz)
	local buf0, sz0 = buf, sz
	local buf = cast(u8p, buf)
	while sz > 0 do
		local len, err = self:try_read(buf, sz)
		if not len then --short read
			return nil, err, sz0 - sz
		elseif len == 0 then --eof
			return nil, 'eof', sz0 - sz
		end
		buf = buf + len
		sz  = sz  - len
	end
	return buf0, sz0
end

function file:try_readall(ignore_file_size)
	if self.type == 'pipe' or ignore_file_size then
		return readall(self.try_read, self)
	end
	assert(self.type == 'file')
	local size, err = self:try_attr'size'; if not size then return nil, err end
	local offset, err = self:try_seek(); if not offset then return nil, err end
	local sz = size - offset
	local buf = u8a(sz)
	local n, err = self:try_read(buf, sz)
	if not n then return nil, err end
	if n < sz then return nil, 'partial', buf, n end
	return buf, n
end

--truncate -------------------------------------------------------------------

cdef[[
int ftruncate(int fd, int64_t length);
int fallocate64(int fd, int mode, off64_t offset, off64_t len);
]]

--NOTE: ftruncate() creates a sparse file (and so would seeking to size-1
--and writing '\0' there), so we need to call fallocate() to actually reserve
--any disk space. OTOH, fallocate() is only efficient on some file systems.

local function fallocate(fd, size)
	return check_errno(C.fallocate64(fd, 0, 0, size) == 0)
end

--NOTE: lseek() is not defined for shm_open()'ed fds, that's why we ask
--for a `size` arg. The seek() behavior is just for compat with Windows.
function file.try_truncate(f, size, opt)
	assert(isnum(size), 'size expected')
	if not f.shm then
		local pos, err = f:seek('set', size)
		if not pos then return nil, err end
	end
	if not f.shm then
		opt = opt or 'fallocate fail' --emulate Windows behavior.
		if opt:find'fallocate' then
			local cursize, err = f:try_attr'size'
			if not cursize then return nil, err end
			local ok, err = fallocate(f.fd, size)
			if not ok then
				if err == 'disk_full' then
					--when fallocate() fails because disk is full, a file is still
					--created filling up the entire disk, so shrink back the file
					--to its original size. this is courtesy: we don't check to see
					--if this fails or not, and we return the original error code.
					C.ftruncate(f.fd, cursize)
				end
				if opt:find'fail' then
					return nil, err
				end
			end
		end
	end
	return check_errno(C.ftruncate(f.fd, size) == 0)
end

--filesystem operations ------------------------------------------------------

cdef[[
int mkdir(const char *pathname, mode_t mode);
int rmdir(const char *pathname);
int chdir(const char *path);
char *getcwd(char *buf, size_t size);
int unlink(const char *pathname);
int rename(const char *oldpath, const char *newpath);
]]

local ERANGE = 34

function cwd()
	while true do
		local buf, sz = cbuf(256)
		if C.getcwd(buf, sz) == nil then
			if errno() ~= ERANGE or buf >= 2048 then
				return assert(check_errno())
			else
				buf, sz = cbuf(sz * 2)
			end
		end
		return str(buf)
	end
end
startcwd = memoize(cwd)

function try_chdir(dir)
	startcwd()
	local ok, err = check_errno(C.chdir(dir) == 0)
	if not ok then return false, err end
	log('', 'fs', 'chdir', '%s', dir)
	return true
end

local function _try_mkdir(path, perms, quiet)
	perms = parse_perms(perms) or default_dir_perms
	local ok, err = check_errno(C.mkdir(path, perms) == 0)
	if not ok then
		if err == 'already_exists' then return true, err end
		return false, err
	end
	log(quiet and '' or 'note', 'fs', 'mkdir', '%s%s%s',
		path, perms and ' ' or '', perms or '')
	return true
end

function try_mkdir(dir, recursive, perms, quiet)
	if recursive then
		dir = path_normalize(dir, true, true) --avoid creating `dir` in `dir/..` sequences
		if not dir or dir == '.' or dir == '/' then
			return nil, 'invalid path'
		end
		local t = {}
		while true do
			local ok, err = _try_mkdir(dir, perms, quiet)
			if ok then break end
			if err ~= 'not_found' then --other problem
				return ok, err
			end
			push(t, dir)
			dir = dirname(dir)
			if not dir or dir == '.' or dir == '/' then --reached root
				return ok, err
			end
		end
		while #t > 0 do
			local dir = pop(t)
			local ok, err = _try_mkdir(dir, perms, quiet)
			if not ok then return ok, err end
		end
		return true
	else
		return _try_mkdir(dir, perms, quiet)
	end
end

function try_mkdirs(file, perms, quiet)
	local dir = dirname(file)
	if dir and dir ~= '.' and dir ~= '/' then
		local ok, err = try_mkdir(dir, true, perms, quiet)
		if not ok then return nil, err end
	end
	return file
end

function try_rmdir(dir, quiet)
	local ok, err, errcode = check_errno(C.rmdir(dir) == 0)
	if not ok then
		if err == 'not_found' then return true, err end
		return false, err
	end
	log(quiet and '' or 'note', 'fs', 'rmdir', '%s', dir)
	return true
end

function try_rmfile(file, quiet)
	local ok, err = check_errno(C.unlink(file) == 0)
	if not ok then
		if err == 'not_found' then return true, err end
		return false, err
	end
	log(quiet and '' or 'note', 'fs', 'rmfile', '%s', file)
	return ok, err
end

local function try_rm(path, quiet)
	local type, err = try_file_attr(path, 'type', false)
	if not type and err == 'not_found' then
		return true, err
	end
	if type == 'dir' then
		return try_rmdir(path, quiet)
	else
		return try_rmfile(path, quiet)
	end
end

local function try_rmdir_recursive(dir, quiet)
	for file, d in ls(dir) do
		if not file then
			if d == 'not_found' then return true, d end
			return file, d
		end
		local filepath = indir(dir, file)
		local ok, err
		local realtype, err = d:attr('type', false)
		if realtype == 'dir' then
			ok, err = try_rmdir_recursive(filepath, quiet)
		elseif realtype then
			ok, err = try_rmfile(filepath, quiet)
		end
		if not ok then
			d:close()
			return ok, err
		end
	end
	return try_rmdir(dir, quiet)
end
local function try_rm_rf(path, quiet)
	--not recursing if the dir is a symlink, unless it has an endsep!
	if not path:ends'/' then
		local type, err = try_file_attr(path, 'type', false)
		if not type then
			if err == 'not_found' then return true, err end
			return nil, err
		end
		if type == 'symlink' then
			return try_rmfile(path, quiet)
		end
	end
	return try_rmdir_recursive(path, quiet)
end

function try_mv(old_path, new_path, dst_dirs_perms, quiet)
	if dst_dirs_perms ~= false then
		local ok, err = try_mkdirs(new_path, dst_dirs_perms, quiet)
		if not ok then return false, err end
	end
	local ok, err = check_errno(C.rename(old_path, new_path) == 0)
	if not ok then return false, err end
	log(quiet and '' or 'note', 'fs', 'mv', 'old: %s\nnew: %s', old_path, new_path)
	return true
end

function try_mksymlink(link_path, target_path, quiet, replace)
	local ok, err = check_errno(C.symlink(target_path, link_path) == 0)
	if not ok then
		if err == 'already_exists' then
			local file_type, symlink_type = try_file_attr(link_path, 'type')
			if file_type == 'symlink'
				and (symlink_type == 'dir') == false
			then
				if try_readlink(link_path) == target_path then
					return true, err
				elseif replace ~= false then
					local ok, err = try_rmfile(link_path)
					if not ok then return false, err end
					local ok, err = check_errno(C.symlink(target_path, link_path) == 0)
					if not ok then return false, err end
					return true, 'replaced'
				end
			end
		end
		return false, err
	end
	log('', 'fs', 'mkslink', 'link:   %s\ntarget:  %s', link_path, target_path)
	return true
end

function try_mkhardlink(link_path, target_path, quiet)
	local ok, err = check_errno(C.link(target_path, link_path) == 0)
	if not ok then
		if err == 'already_exists' then
			local i1 = try_file_attr(target_path, 'inode')
			if not i1 then goto fuggetit end
			local i2 = try_file_attr(link_path, 'inode')
			if not i2 then goto fuggetit end
			if i1 == i2 then return true, err end
		end
		::fuggetit::
		return false, err
	end
	log('', 'fs', 'mkhlink', 'link:   %s\ntarget:  %s', link_path, target_path)
	return true
end

--raising versions

function chdir(dir)
	local ok, err = try_chdir(dir)
	if ok then return dir, err end
	check('fs', 'chdir', ok, '%s: %s', dir, err)
end

function mkdir(dir, perms, quiet)
	local ok, err = try_mkdir(dir, true, perms, quiet)
	if ok then return dir, err end
	check('fs', 'mkdir', ok, '%s%s%s: %s', dir, perms and ' ' or '', perms or '', err)
end

function mkdirs(file)
	mkdir(assert(dirname(file)))
	return file
end

function rmdir(dir, quiet)
	local ok, err = try_rmdir(dir, quiet)
	if ok then return dir, err end
	check('fs', 'rmdir', ok, '%s: %s', dir, err)
end

function rmfile(path, quiet)
	local ok, err = try_rmfile(path, quiet)
	if ok then return path, err end
	check('fs', 'rmfile', ok, '%s: %s', path, err)
end

function rm(path, quiet)
	local ok, err = try_rm(path, quiet)
	if ok then return path, err end
	check('fs', 'rm', ok, '%s: %s', path, err)
end

function rm_rf(path, quiet)
	local ok, err = try_rm_rf(path, quiet)
	if ok then return path, err end
	check('fs', 'rm_rf', ok, '%s: %s', path, err)
end

function mv(old_path, new_path, perms, quiet)
	local ok, err = try_mv(old_path, new_path, perms, quiet)
	if ok then return new_path, err end
	check('fs', 'mv', ok, 'old: %s\nnew: %s\nerror: %s',
		old_path, new_path, err)
end

function mksymlink(link_path, target_path, quiet)
	local ok, err = try_mksymlink(link_path, target_path)
	if ok then return dir, err end
	check('fs', 'mkslink', ok, '%s: %s', dir, err)
end

function mkhardlink(link_path, target_path, quiet)
	local ok, err = try_mkhardlink(link_path, target_path)
	if ok then return dir, err end
	check('fs', 'mkhlink', ok, '%s%s%s: %s', dir, perms and ' ' or '', perms or '', err)
end

--hardlinks & symlinks -------------------------------------------------------

cdef[[
int link(const char *oldpath, const char *newpath);
int symlink(const char *oldpath, const char *newpath);
ssize_t readlink(const char *path, char *buf, size_t bufsize);
]]

local EINVAL = 22

function try_readlink(link, maxdepth)
	maxdepth = maxdepth or 32
	if not file_is(link, 'symlink') then
		return link
	end
	if maxdepth == 0 then
		return nil, 'not_found'
	end
	local buf, sz = cbuf(256)
	::again::
	local len = C.readlink(link, buf, sz)
	if len == -1 then
		if errno() == EINVAL then --make it legit: no symlink, no target
			return nil
		end
		return check_errno()
	end
	if len >= sz then --we don't know if sz was enough
		buf, sz = cbuf(sz * 2)
		goto again
	end
	local target = str(buf, len)
	if target:starts'/' then
		link = target
	else --relative symlinks are relative to their own dir
		local link_dir = dirname(link)
		if not link_dir then
			return nil, 'not_found'
		elseif link_dir == '.' then
			link = target
		else
			link = indir(link_dir, target)
		end
	end
	return try_readlink(link, maxdepth - 1)
end

function readlink(link, maxdepth)
	local target, err = try_readlink(link, maxdepth)
	local ok = target ~= nil or err == 'not_found'
	check('fs', 'readlink', ok, '%s: %s', link, err)
	if target == nil then return target, err end
	return target
end

--common paths ---------------------------------------------------------------

function homedir()
	return os.getenv'HOME'
end

function tmpdir()
	return os.getenv'TMPDIR' or '/tmp'
end

function appdir(appname)
	local dir = homedir()
	return dir and format('%s/.%s', dir, appname)
end

function exepath()
	return readlink'/proc/self/exe'
end
exepath = memoize(exepath)

function abspath(path, pwd)
	if path:starts'/' then
		return path
	end
	return indir(pwd or cwd(), path)
end

function run_indir(dir, fn, ...)
	local cwd = cwd()
	chdir(dir)
	local function pass(ok, ...)
		chdir(cwd)
		if ok then return ... end
		error(..., 2)
	end
	pass(pcall(fn, ...))
end

exedir = memoize(function()
	return assert(dirname(exepath()))
end)

scriptdir = memoize(function()
	local s = rel_scriptdir:starts'/' and rel_scriptdir or indir(startcwd(), rel_scriptdir)
	return path_normalize(s)
end)

vardir = memoize(function()
	return config'vardir' or indir(scriptdir(), 'var')
end)

function varpath(...)
	return indir(vardir(), ...)
end

--file attributes ------------------------------------------------------------

cdef[[
struct stat {
	uint64_t st_dev;
	uint64_t st_ino;
	uint64_t st_nlink;
	uint32_t st_mode;
	uint32_t st_uid;
	uint32_t st_gid;
	uint32_t __pad0;
	uint64_t st_rdev;
	int64_t  st_size;
	int64_t  st_blksize;
	int64_t  st_blocks;
	uint64_t st_atime;
	uint64_t st_atime_nsec;
	uint64_t st_mtime;
	uint64_t st_mtime_nsec;
	uint64_t st_ctime;
	uint64_t st_ctime_nsec;
	int64_t  __unused[3];
};
]]

local file_types = {
	[0xc000] = 'socket',
	[0xa000] = 'symlink',
	[0x8000] = 'file',
	[0x6000] = 'blockdev',
	[0x2000] = 'chardev',
	[0x4000] = 'dir',
	[0x1000] = 'pipe',
}
local function st_type(mode)
	local type = band(mode, 0xf000)
	return file_types[type]
end

local function st_perms(mode)
	return band(mode, bnot(0xf000))
end

local function st_time(s, ns)
	return tonumber(s) + tonumber(ns) * 1e-9
end

local stat_getters = {
	type    = function(st) return st_type(st.st_mode) end,
	dev     = function(st) return tonumber(st.st_dev) end,
	inode   = function(st) return st.st_ino end, --unfortunately, 64bit inode
	nlink   = function(st) return tonumber(st.st_nlink) end,
	perms   = function(st) return st_perms(st.st_mode) end,
	uid     = function(st) return st.st_uid end,
	gid     = function(st) return st.st_gid end,
	rdev    = function(st) return tonumber(st.st_rdev) end,
	size    = function(st) return tonumber(st.st_size) end,
	blksize = function(st) return tonumber(st.st_blksize) end,
	blocks  = function(st) return tonumber(st.st_blocks) end,
	atime   = function(st) return st_time(st.st_atime, st.st_atime_nsec) end,
	mtime   = function(st) return st_time(st.st_mtime, st.st_mtime_nsec) end,
	ctime   = function(st) return st_time(st.st_ctime, st.st_ctime_nsec) end,
}

local stat_ct = ctype'struct stat'
local st
local function wrap(stat_func)
	return function(arg, attr)
		st = st or stat_ct()
		local ok = stat_func(arg, st) == 0
		if not ok then return check_errno() end
		if attr then
			local get = stat_getters[attr]
			return get and get(st)
		else
			local t = {}
			for k, get in pairs(stat_getters) do
				t[k] = get(st)
			end
			return t
		end
	end
end

local int = ctype'int'
local fstat = wrap(function(f, st)
	return C.syscall(5, cast(int, f.fd), cast(voidp, st))
end)
local stat = wrap(function(path, st)
	return C.syscall(4, cast(voidp, path), cast(voidp, st))
end)
local lstat = wrap(function(path, st)
	return C.syscall(6, cast(voidp, path), cast(voidp, st))
end)

cdef[[
struct timespec {
	time_t tv_sec;
	long   tv_nsec;
};
int futimens(int fd, const struct timespec times[2]);
int utimensat(int dirfd, const char *path, const struct timespec times[2], int flags);
]]

local UTIME_OMIT = shl(1,30)-2

local function set_timespec(ts, t)
	if ts then
		t.tv_sec = ts
		t.tv_nsec = (ts - floor(ts)) * 1e9
	else
		t.tv_sec = 0
		t.tv_nsec = UTIME_OMIT
	end
end

local AT_FDCWD = -100

local ts_ct = ctype'struct timespec[2]'
local ts
local function futimes(f, atime, mtime)
	ts = ts or ts_ct()
	set_timespec(atime, ts[0])
	set_timespec(mtime, ts[1])
	return check_errno(C.futimens(f.fd, ts) == 0)
end

local function utimes(path, atime, mtime)
	ts = ts or ts_ct()
	set_timespec(atime, ts[0])
	set_timespec(mtime, ts[1])
	return check_errno(C.utimensat(AT_FDCWD, path, ts, 0) == 0)
end

local AT_SYMLINK_NOFOLLOW = 0x100

local function lutimes(path, atime, mtime)
	ts = ts or ts_ct()
	set_timespec(atime, ts[0])
	set_timespec(mtime, ts[1])
	return check_errno(C.utimensat(AT_FDCWD, path, ts, AT_SYMLINK_NOFOLLOW) == 0)
end

cdef[[
int fchmod(int fd,           mode_t mode);
int  chmod(const char *path, mode_t mode);
int lchmod(const char *path, mode_t mode);
]]

local function wrap(chmod_func, stat_func)
	return function(f, perms)
		local cur_perms
		local _, is_rel = parse_perms(perms)
		if is_rel then
			local cur_perms, err = stat_func(f, 'perms')
			if not cur_perms then return nil, err end
		end
		local mode = parse_perms(perms, cur_perms)
		return chmod_func(f, mode) == 0
	end
end
local fchmod = wrap(function(f, mode) return C.fchmod(f.fd, mode) end, fstat)
local chmod = wrap(C.chmod, stat)
local lchmod = wrap(C.lchmod, lstat)

cdef[[
int fchown(int fd,           uid_t owner, gid_t group);
int  chown(const char *path, uid_t owner, gid_t group);
int lchown(const char *path, uid_t owner, gid_t group);
typedef unsigned int uid_t;
typedef unsigned int gid_t;
struct passwd {
    char   *pw_name;    // Username
    char   *pw_passwd;  // User password (usually "x" or "*")
    uid_t   pw_uid;     // User ID
    gid_t   pw_gid;     // Group ID
    char   *pw_gecos;   // Real name or comment field
    char   *pw_dir;     // Home directory
    char   *pw_shell;   // Login shell
};
struct group {
    char   *gr_name;    // Group name
    char   *gr_passwd;  // Group password (usually "x" or "*")
    gid_t   gr_gid;     // Group ID
    char  **gr_mem;     // Null-terminated list of group members
};
struct passwd *getpwnam(const char *name);
struct group *getgrnam(const char *name);
]]
local function get_uid(s)
	if not s or isnum(s) then return s end
	local p = ptr(C.getpwnam(s))
	return p and p.pw_uid
end
local function get_gid(s)
	if not s or isnum(s) then return s end
	local p = ptr(C.getgrnam(s))
	return p and p.gr_gid
end

local function wrap(chown_func)
	return function(arg, uid, gid)
		return chown_func(arg, get_uid(uid) or -1, get_gid(gid) or -1) == 0
	end
end
local fchown = wrap(function(f, uid, gid) return C.fchown(f.fd, uid, gid) end)
local chown = wrap(C.chown)
local lchown = wrap(C.lchown)

local function _fs_attr_get(path, attr, deref)
	local stat = deref and stat or lstat
	return stat(path, attr)
end

local function wrap(chmod_func, chown_func, utimes_func)
	return function(arg, t)
		local ok, err
		if t.perms then
			ok, err = chmod_func(arg, t.perms)
			if not ok then return nil, err end
		end
		if t.uid or t.gid then
			ok, err = chown_func(arg, t.uid, t.gid)
			if not ok then return nil, err end
		end
		if t.atime or t.mtime then
			ok, err = utimes_func(arg, t.atime, t.mtime)
			if not ok then return nil, err end
		end
		return ok --returns nil without err if no attr was set
	end
end

local _file_attr_set = wrap(fchmod, fchown, futimes)

local set_deref   = wrap( chmod,  chown,  utimes)
local set_symlink = wrap(lchmod, lchown, lutimes)
local function _fs_attr_set(path, t, deref)
	local set = deref and set_deref or set_symlink
	return set(path, t)
end

function file.try_attr(f, attr)
	if istab(attr) then
		return _file_attr_set(f, attr)
	else
		return fstat(f, attr)
	end
end

function file.attr(f, attr)
	local ret, err = f:try_attr(attr)
	local ok = ret ~= nil or err == nil or err == 'not_found'
	check('fs', 'attr', ok, '%s: %s', f.path, err)
	if err ~= nil then return ret, err end
	return ret
end

function file.size(f)
	return f:attr'size'
end

local function attr_args(attr, deref)
	if isbool(attr) then --middle arg missing
		attr, deref = nil, attr
	end
	if deref == nil then
		deref = true --deref by default
	end
	return attr, deref
end

function try_file_attr(path, ...)
	local attr, deref = attr_args(...)
	if attr == 'target' then
		return try_readlink(path)
	end
	if istab(attr) then
		return _fs_attr_set(path, attr, deref)
	else
		return _fs_attr_get(path, attr, deref)
	end
end
function file_attr(path, ...)
	local ret, err = try_file_attr(path, ...)
	local ok = ret ~= nil or err == nil or err == 'not_found'
	check('fs', 'attr', ok, '%s: %s', path, err)
	if err ~= nil then return ret, err end
	return ret
end

function try_mtime(file, deref)
	return try_file_attr(file, 'mtime', deref)
end
function mtime(file, deref)
	return file_attr(file, 'mtime', deref)
end

function try_chmod(path, perms, quiet)
	local ok, err = try_file_attr(path, {perms = perms})
	if not ok then return false, err end
	log(quiet and '' or 'note', 'fs', 'chmod', '%s', path)
	return path
end
function chmod(path, perms, quiet)
	local ok, err = try_chmod(path, perms, quiet)
	check('fs', 'chmod', ok, '%s: %s', path, err)
	return path
end

function file_is(path, type, deref)
	if type == 'symlink' then
		deref = false
	end
	local ftype, err = try_file_attr(path, 'type', deref)
	if not ftype and err == 'not_found' then
		return false, 'not_found'
	elseif not type and ftype then
		return true
	elseif not ftype then
		check('fs', 'file_is', nil, '%s: %s', path, err)
	else
		return ftype == type
	end
end
exists = file_is

function checkexists(file, type, deref)
	check('fs', 'exists', exists(file, type, deref), '%s', file)
end

--directory listing ----------------------------------------------------------

cdef[[
struct dirent { // NOTE: 64bit version
	uint64_t        d_ino;
	int64_t         d_off;
	unsigned short  d_reclen;
	unsigned char   d_type;
	char            d_name[256];
};
typedef struct DIR DIR;
DIR *opendir(const char *name);
struct dirent *readdir(DIR *dirp) asm("readdir64");
int closedir(DIR *dirp);
]]

dir_ct = ctype[[
	struct {
		DIR *_dirp;
		struct dirent* _dentry;
		int  _errno;
		int  _dirlen;
		char _skip_dot_dirs;
		char _dir[?];
	}
]]

function dir.try_close(dir)
	if dir:closed() then return true end
	local ok = C.closedir(dir._dirp) == 0
	if not ok then return check_errno(false) end
	dir._dirp = nil
	return true
end

function dir.close(dir)
	assert(dir:try_close())
end

function dir_ready(dir)
	return dir._dentry ~= nil
end

function dir.closed(dir)
	return dir._dirp == nil
end

function dir_name(dir)
	return str(dir._dentry.d_name)
end

function dir.dir(dir)
	return str(dir._dir, dir._dirlen)
end

function dir.next(dir)
	if dir:closed() then
		if dir._errno ~= 0 then
			local errno = dir._errno
			dir._errno = 0
			return check_errno(false, errno)
		end
		return nil
	end
	errno(0)
	dir._dentry = C.readdir(dir._dirp)
	if dir._dentry ~= nil then
		local name = dir:name()
		if dir._skip_dot_dirs == 1 and (name == '.' or name == '..') then
			return dir.next(dir)
		end
		return name, dir
	else
		local errno = errno()
		dir:close()
		if errno == 0 then
			return nil
		end
		return check_errno(false, errno)
	end
end

--dirent.d_type consts
local DT_UNKNOWN = 0
local DT_FIFO    = 1
local DT_CHR     = 2
local DT_DIR     = 4
local DT_BLK     = 6
local DT_REG     = 8
local DT_LNK     = 10
local DT_SOCK    = 12

local dt_types = {
	dir      = DT_DIR,
	file     = DT_REG,
	symlink  = DT_LNK,
	blockdev = DT_BLK,
	chardev  = DT_CHR,
	pipe     = DT_FIFO,
	socket   = DT_SOCK,
	unknown  = DT_UNKNOWN,
}

local dt_names = {
	[DT_DIR]  = 'dir',
	[DT_REG]  = 'file',
	[DT_LNK]  = 'symlink',
	[DT_BLK]  = 'blockdev',
	[DT_CHR]  = 'chardev',
	[DT_FIFO] = 'pipe',
	[DT_SOCK] = 'socket',
	[DT_UNKNOWN] = 'unknown',
}

local function _dir_attr_get(dir, attr)
	if attr == 'type' and dir._dentry.d_type == DT_UNKNOWN then
		--some filesystems (eg. VFAT) require this extra call to get the type.
		local type, err = lstat(dir:path(), 'type')
		if not type then
			return false, nil, err
		end
		local dt = dt_types[type]
		dir._dentry.d_type = dt --cache it
	end
	if attr == 'type' then
		return dt_names[dir._dentry.d_type]
	elseif attr == 'inode' then
		return dir._dentry.d_ino
	else
		return nil, false
	end
end

local function dir_check(dir)
	assert(not dir:closed(), 'dir closed')
	assert(dir_ready(dir), 'dir not ready') --must call next() at least once.
end

function ls(p, opt)
	local skip_dot_dirs = not (opt and opt:find('..', 1, true))
	p = p or '.'
	local dir = dir_ct(#p)
	dir._dirlen = #p
	copy(dir._dir, p, #p)
	dir._skip_dot_dirs = skip_dot_dirs and 1 or 0
	dir._dirp = C.opendir(p)
	if dir._dirp == nil then
		dir._errno = errno()
	end
	return dir.next, dir
end

function dir.path(dir)
	return indir(dir:dir(), dir:name())
end

function dir.name(dir)
	dir_check(dir)
	return dir_name(dir)
end

local function dir_is_symlink(dir)
	return _dir_attr_get(dir, 'type', false) == 'symlink'
end

function dir.attr(dir, ...)
	dir_check(dir)
	local attr, deref = attr_args(...)
	if attr == 'target' then
		if dir_is_symlink(dir) then
			return try_readlink(dir:path())
		else
			return nil --no error for non-symlink files
		end
	end
	if istab(attr) then
		return fs_attr_set(dir:path(), attr, deref)
	elseif not attr or (deref and dir_is_symlink(dir)) then
		return _fs_attr_get(dir:path(), attr, deref)
	else
		local val, found = _dir_attr_get(dir, attr)
		if found == false then --attr not found in state
			return _fs_attr_get(dir:path(), attr)
		else
			return val
		end
	end
end

function dir.size(dir)
	return dir:attr'size'
end

function dir.is(dir, type, deref)
	if type == 'symlink' then
		deref = false
	end
	return dir:attr('type', deref) == type
end

local function scandir1(path, dive)
	local pds = {}
	local next, d = ls(path)
	local name, err
	local sc = {}
	setmetatable(sc, sc)
	function sc:close()
		repeat
			local ok, err = d:close()
			if not ok then return nil, err end
			d = pop(pds)
		until not d
		name, err = nil, 'closed'
	end
	function sc:depth(n)
		n = n or 0
		local maxdepth = #pds + 1
		return n > 0 and min(maxdepth, n) or max(1, maxdepth + n)
	end
	function sc:relpath(n)
		return relpath(sc:path(n), path)
	end
	function sc:__index(k) --forward other method calls to a dir object.
		local f
		function f(self, depth, ...)
			if not name then return nil, err end
			if not isnum(depth) then
				return f(self, 0, depth, ...)
			end
			local d = d
		 	if depth ~= 0 then
				depth = self:depth(depth)
				d = pds[depth] or d
			end
			return d[k](d, ...)
		end
		self[k] = f
		return f
	end
	local function iter()
		if not d then return nil end --closed
		if name and d:is('dir', false) then
			if not dive or dive(d) then
				local next1, d1 = ls(d:path())
				assert(next1 == next)
				push(pds, d)
				d = d1
			end
		end
		--TODO: error reporting!
		name, err = next(d)
		if name == nil then
			d = pop(pds)
			return iter()
		end
		return sc
	end
	return iter
end
function scandir(arg)
	if isstr(arg) then
		return scandir1(arg)
	elseif istab(arg) then
		local i, n = 1, arg.n or #arg
		local iter = scandir1(arg[i])
		return function()
			::again::
			local sc = iter()
			if not sc then
				if i == n then return nil end
				i = i + 1
				iter = scandir1(arg[i])
				goto again
			end
			return sc
		end
	else
		assertf(false, 'string or table expected, got: %s', type(arg))
	end
end

--memory mapping -------------------------------------------------------------

local librt = C
do --for shm_open()
	local ok, rt = pcall(ffi.load, 'rt')
	if ok then librt = rt end
end

cdef'int __getpagesize();'
local getpagesize = C.__getpagesize
pagesize = memoize(function() return getpagesize() end)

cdef[[
int shm_open(const char *name, int oflag, mode_t mode);
int shm_unlink(const char *name);

void* mmap(void *addr, size_t length, int prot, int flags,
	int fd, off64_t offset) asm("mmap64");
int munmap(void *addr, size_t length);
int msync(void *addr, size_t length, int flags);
int mprotect(void *addr, size_t len, int prot);
]]

local PROT_READ  = 1
local PROT_WRITE = 2
local PROT_EXEC  = 4

local function protect_bits(write, exec, copy)
	return bor(PROT_READ,
		(write or copy) and PROT_WRITE or 0,
		exec and PROT_EXEC or 0)
end

local function C_mmap(...)
	local addr = C.mmap(...)
	local ok, err = check_errno(cast('intptr_t', addr) ~= -1)
	if not ok then return nil, err end
	return addr
end

function check_tagname(tagname)
	assert(not tagname:find'[/\\]', 'tagname cannot contain `/` or `\\`')
	return tagname
end

local MAP_SHARED  = 1
local MAP_PRIVATE = 2 --copy-on-write
local MAP_FIXED   = 0x0010
local MAP_ANON    = 0x0020

--TODO: merge this into mmap()
local function _mmap(path, access, size, offset, addr, tagname, perms)

	local write, exec, copy = parse_access(access or '')

	path = path or tagname and check_tagname(tagname)

	--open the file, if any.

	local file
	local function exit(err)
		if file then file:try_close() end
		return nil, err
	end

	if isstr(path) then
		local flags = write and 'rdwr creat' or 'rdonly'
		local perms = perms and parse_perms(perms)
			or tonumber('400', 8) +
				(write and tonumber('200', 8) or 0) +
				(exec  and tonumber('100', 8) or 0)
		local err
		file, err = _open(path, {
				flags = flags, perms = perms,
				open = tagname and librt.shm_open,
				shm = tagname and true or nil,
			})
		if not file then
			return nil, err
		end
	end

	--emulate Windows behavior for missing size and size mismatches.

	if file then
		if not size then --if size not given, assume entire file.
			local filesize, err = file:try_attr'size'
			if not filesize then
				return exit(err)
			end
			size = filesize - offset
		elseif write then --if writable file too short, extend it.
			local filesize, err = file:try_attr'size'
			if not filesize then
				return exit(err)
			end
			if filesize < offset + size then
				local ok, err = file:try_truncate(offset + size)
				if not ok then
					return exit(err)
				end
			end
		else --if read/only file too short.
			local filesize, err = file:try_attr'size'
			if not filesize then
				return exit(err)
			end
			if filesize < offset + size then
				return exit'file_too_short'
			end
		end
	end

	--mmap the file.

	local protect = protect_bits(write, exec, copy)

	local flags = bor(
		copy and MAP_PRIVATE or MAP_SHARED,
		file and 0 or MAP_ANON,
		addr and MAP_FIXED or 0)

	local addr, err = C_mmap(addr, size, protect, flags, file and file.fd or -1, offset)
	if not addr then return exit(err) end

	--create the map object.

	local MS_ASYNC      = 1
	local MS_INVALIDATE = 2
	local MS_SYNC       = 4

	local function flush(self, async, addr, sz)
		if not isbool(async) then --async arg is optional
			async, addr, sz = false, async, addr
		end
		local addr = aligned_addr(addr or self.addr, 'left')
		local flags = bor(async and MS_ASYNC or MS_SYNC, MS_INVALIDATE)
		local ok = C.msync(addr, sz or self.size, flags) == 0
		if not ok then return check_errno(false) end
		return true
	end

	local function free()
		C.munmap(addr, size)
		exit()
	end

	local function unlink()
		return unlink_mapfile(tagname)
	end

	return {addr = addr, size = size, free = free,
		flush = flush, unlink = unlink, access = access}

end

function unlink_mapfile(tagname)
	local ok, err = check_errno(librt.shm_unlink(check_tagname(tagname)) == 0)
	if ok or err == 'not_found' then return true end
	return nil, err
end

function mprotect(addr, size, access)
	local protect = protect_bits(parse_access(access or 'x'))
	return check_errno(C.mprotect(addr, size, protect) == 0)
end

local split_uint64, join_uint64
do
local m = new[[
	union {
		struct { uint32_t lo; uint32_t hi; };
		uint64_t x;
	}
]]
function split_uint64(x)
	m.x = x
	return m.hi, m.lo
end
function join_uint64(hi, lo)
	m.hi, m.lo = hi, lo
	return m.x
end
end

function aligned_size(size, dir) --dir can be 'l' or 'r' (default: 'r')
	if isctype(u64, size) then --an uintptr_t on x64
		local pagesize = pagesize()
		local hi, lo = split_uint64(size)
		local lo = aligned_size(lo, dir)
		return join_uint64(hi, lo)
	else
		local pagesize = pagesize()
		if not (dir and dir:find'^l') then --align to the right
			size = size + pagesize - 1
		end
		return band(size, bnot(pagesize - 1))
	end
end

function aligned_addr(addr, dir)
	return cast(voidp, aligned_size(cast(uintptr, addr), dir))
end

function parse_access(s)
	assert(not s:find'[^rwcx]', 'invalid access flags')
	local write = s:find'w' and true or false
	local exec  = s:find'x' and true or false
	local copy  = s:find'c' and true or false
	assert(not (write and copy), 'invalid access flags')
	return write, exec, copy
end

function file.try_mmap(f, ...)
	local access, size, offset, addr
	if istab(t) then
		access, size, offset, addr = t.access, t.size, t.offset, t.addr
	else
		offset, size, addr, access = ...
	end
	return mmap(f, access or f.access, size, offset, addr)
end
function file:mmap(...)
	self:check_io(self:try_mmap(...))
end

function try_mmap(t,...)
	local file, access, size, offset, addr, tagname, perms
	if istab(t) then
		file, access, size, offset, addr, tagname, perms =
			t.file, t.access, t.size, t.offset, t.addr, t.tagname, t.perms
	else
		file, access, size, offset, addr, tagname, perms = t, ...
	end
	assert(not file or isstr(file) or isfile(file), 'invalid file argument')
	assert(file or size, 'file and/or size expected')
	assert(not size or size > 0, 'size must be > 0')
	local offset = file and offset or 0
	assert(offset >= 0, 'offset must be >= 0')
	assert(offset == aligned_size(offset), 'offset not page-aligned')
	local addr = addr and cast(voidp, addr)
	assert(not addr or addr ~= nil, 'addr can\'t be zero')
	assert(not addr or addr == aligned_addr(addr), 'addr not page-aligned')
	assert(not (file and tagname), 'cannot have both file and tagname')
	assert(not tagname or not tagname:find'\\', 'tagname cannot contain `\\`')
	return _mmap(file, access, size, offset, addr, tagname, perms)
end
function mmap(...)
	return check_io(nil, try_mmap(...))
end

--mirror buffer --------------------------------------------------------------

cdef'int memfd_create(const char *name, unsigned int flags);'
local MFD_CLOEXEC = 0x0001

function mirror_buffer(size, addr)

	local size = aligned_size(size or 1)

	local fd = C.memfd_create('mirror_buffer', MFD_CLOEXEC)
	if fd == -1 then return check_errno() end

	local addr1, addr2

	local function free()
		if addr1 then C.munmap(addr1, size) end
		if addr2 then C.munmap(addr2, size) end
		if fd then C.close(fd) end
	end

	local ok, err = check_errno(C.ftruncate(fd, size) == 0)
	if not ok then
		free()
		return nil, err
	end

	for i = 1, 100 do

		local addr = cast('void*', addr)
		local flags = bor(MAP_PRIVATE, MAP_ANON, addr ~= nil and MAP_FIXED or 0)
		local addr0, err = mmap(addr, size * 2, 0, flags, 0, 0)
		if not addr0 then
			free()
			return nil, err
		end

		C.munmap(addr0, size * 2)

		local protect = bor(PROT_READ, PROT_WRITE)
		local flags = bor(MAP_SHARED, MAP_FIXED)

		addr1, err = mmap(addr0, size, protect, flags, fd, 0)
		if not addr1 then
			goto skip
		end

		addr2 = cast('uint8_t*', addr1) + size
		addr2, err = mmap(addr2, size, protect, flags, fd, 0)
		if not addr2 then
			C.munmap(addr1, size)
			goto skip
		end

		C.close(fd)
		fd = nil

		do return {addr = addr1, size = size, free = free} end

		::skip::
	end

	free()
	return nil, 'max_tries'

end

--memory streams -------------------------------------------------------------

local vfile = {}

vfile.check_io = check_io
vfile.checkp   = checkp

function open_buffer(buf, sz, mode)
	sz = sz or #buf
	mode = mode or 'r'
	assertf(mode == 'r' or mode == 'w', 'invalid mode: "%s"', mode)
	local f = {
		b = string_buffer():set(buf, sz),
		offset = 0,
		mode = mode,
		w = 0,
		r = 0,
		__index = vfile,
	}
	return setmetatable(f, f)
end

function vfile.closed(f)
	return not f.b
end

function vfile.try_close(f)
	if f.b then
		f.b:free()
		f.b = nil
	end
	return true
end

vfile.onclose = file.onclose

function vfile:try_attr(attr)
	assert(not istab(attr))
	if attr == 'size' then
		return #self.b
	else
		assert(false)
	end
end

function vfile.try_flush(f)
	if not f.b then
		return nil, 'access_denied'
	end
	return true
end

function vfile:flush()
	self:check_io(self:try_flush())
end

function vfile.try_read(f, buf, sz)
	if not f.b then
		return nil, 'access_denied'
	end
	sz = min(max(0, sz), max(0, #f.b - f.offset))
	copy(buf, f.b:ref() + f.offset, sz)
	f.offset = f.offset + sz
	f.r = f.r + sz
	return sz
end

vfile.try_readn   = file.try_readn
vfile.try_readall = file.try_readall

function vfile.try_write(f, buf, sz)
	if not f.b then
		return nil, 'access_denied'
	end
	if f.mode ~= 'w' then
		return nil, 'access_denied'
	end
	sz = max(0, sz)
	local sz0 = #f.b
	local sz1 = f.offset + sz
	local grow = sz1 - sz0
	if grow > 0 then
		f.b:reserve(grow)
		f.b:commit(grow)
	end
	copy(f.b:ref() + f.offset, buf, sz)
	f.offset = f.offset + sz
	f.w = f.w + sz
	return sz
end

function vfile._seek(f, whence, offset)
	if whence == 1 then --cur
		offset = f.offset + offset
	elseif whence == 2 then --end
		offset = #f.b + offset
	end
	offset = max(offset, 0)
	f.offset = offset
	return offset
end
vfile.try_seek = file.try_seek
vfile.try_skip = file.try_skip

function vfile:try_truncate(n)
	local pos, err = f:try_seek(n)
	if not pos then return nil, err end
	local n0 = #f.b
	if n == 0 then
		b:reset()
	elseif n > n0 then
		local n = n - n0
		local p = b:reserve(n)
		fill(b, n)
		b:commit(n)
	elseif n < n0 then
		return nil, 'NYI'
	end
	return true
end

vfile.unbuffered_reader = file.unbuffered_reader
vfile  .buffered_reader = file  .buffered_reader

vfile.close    = unprotect_io(vfile.try_close)
vfile.read     = unprotect_io(vfile.try_read)
vfile.write    = unprotect_io(vfile.try_write)
vfile.readn    = unprotect_io(vfile.try_readn)
vfile.readall  = unprotect_io(vfile.try_readall)
vfile.flush    = unprotect_io(vfile.try_flush)
vfile.truncate = unprotect_io(vfile.try_truncate)
vfile.seek     = unprotect_io(vfile.try_seek)
vfile.skip     = unprotect_io(vfile.try_skip)

--hi-level APIs --------------------------------------------------------------

function file:pbuffer()
	return pbuffer{f = self}
end

ABORT = {} --error signal to pass to save()'s reader function.

function try_load_tobuffer(file, default_buf, default_len, ignore_file_size)
	local f, err = try_open(file)
	if not f then
		if err == 'not_found' and default_buf ~= nil then
			return default_buf, default_len
		end
		return nil, err
	end
	local buf, len = f:readall(ignore_file_size)
	f:close()
	return buf, len
end

function try_load(file, default, ignore_file_size)
	local buf, len = try_load_tobuffer(file, default, nil, ignore_file_size)
	if not buf then return nil, len end
	return str(buf, len)
end

function load_tobuffer(file, default_buf, default_len, ignore_file_size)
	local buf, len = try_load_tobuffer(file, default_buf, default_len, ignore_file_size)
	check('fs', 'load', buf ~= nil, '%s: %s', file, len)
	return buf, len
end

function load(file, default, ignore_file_size) --load a file into a string.
	local buf, len = load_tobuffer(file, default, nil, ignore_file_size)
	if buf == default then return default end
	return str(buf, len)
end

--write a Lua value, array of values or function results to a file atomically.
--TODO: make a file_saver() out of this without coroutines and use it
--in resize_image()!
local function _save(file, s, sz, perms)

	local tmpfile = file..'.tmp'

	local dir = assert(dirname(tmpfile))
	local ok, err = try_mkdir(dir, true, perms)
	if not ok then
		return false, _('could not create dir %s: %s', dir, err)
	end

	local f, err = try_open{path = tmpfile, mode = 'w', perms = perms, quiet = true}
	if not f then
		return false, _('could not open file %s: %s', tmpfile, err)
	end

	local ok, err = true
	if istab(s) then --array of stringables
		for i = 1, #s do
			ok, err = f:try_write(tostring(s[i]))
			if not ok then break end
		end
	elseif isfunc(s) then --reader of buffers or stringables
		local read = s
		while true do
			local s, sz
			ok, s, sz = pcall(read, true)
			if not ok then err = s; break end
			if sz == 0 then break end --eof
			if s == nil then
				if sz ~= nil then ok = false end --error, not eof
				break
			end
			if not iscdata(s) then
				s = tostring(s)
			end
			ok, err = f:try_write(s, sz)
			if not ok then break end
		end
	elseif s ~= nil and s ~= '' then --buffer or stringable
		if not iscdata(s) then
			s = tostring(s)
		end
		ok, err = f:try_write(s, sz)
	end
	local close_ok, close_err = f:try_close()
	if ok then --I/O errors can also be reported by close().
		ok, err = close_ok, close_err
	end

	if not ok then
		local err_msg = 'could not write to file %s: %s'
		local ok, rm_err = try_rmfile(tmpfile, true)
		if not ok then
			err_msg = err_msg..'\nremoving it also failed: %s'
		end
		return false, _(err_msg, tmpfile, err, rm_err)
	end

	local ok, err = try_mv(tmpfile, file, nil, true)
	if not ok then
		local err_msg = 'could not move file %s -> %s: %s'
		local ok, rm_err = try_rmfile(tmpfile, true)
		if not ok then
			err_msg = err_msg..'\nremoving it also failed: %s'
		end
		return false, _(err_msg, tmpfile, file, err, rm_err)
	end

	return true
end

function try_save(file, s, sz, perms, quiet)
	local ok, err = _save(file, s, sz, perms)
	if not ok then return false, err end
	local sz = sz or isstr(s) and #s
	local ssz = sz and _(' (%s)', kbytes(sz)) or ''
	log(quiet and '' or 'note', 'fs', 'save', '%s%s', file, ssz)
	return true
end

function save(file, s, sz, perms, quiet)
	local ok, err = try_save(file, s, sz, perms, quiet)
	check('fs', 'save', ok, '%s: %s', file, err)
end

--return a `try_write(v | buf,len | t | nil) -> true | false,err` function.
function file_saver(file, thread_name)
	require'sock'
	local write = cowrap(function(yield)
		return try_save(file, yield)
	end, thread_name or 'file-saver %s', file)
	local ok, err = write()
	if not ok then return nil, err end
	return write
end

--TODO: try_cp()
function cp(src_file, dst_file, quiet)
	log(quiet and '' or 'note', 'fs', 'cp', 'src: %s ->\ndst: %s', src_file, dst_file)
	--TODO: buffered read for large files.
	save(dst_file, load(src_file))
end

function try_touch(file, mtime, btime, quiet) --create file or update its mtime.
	local create = not exists(file)
	if create then
		local ok, err = try_save(file, '', quiet)
		if not ok then return false, err end
	end
	if not (create and not (mtime or btime)) then
		local ok, err = try_file_attr(file, {
			mtime = mtime or time(),
			btime = btime or nil,
		})
		if not ok then return false, err end
	end
	if not quiet then
		log('', 'fs', 'touch', '%s to %s%s', file,
			date('%d-%m-%Y %H:%M', mtime) or 'now',
			btime and ', btime '..date('%d-%m-%Y %H:%M', btime) or '')
	end
	return true
end

function touch(file, mtime, btime, quiet)
	local ok, err = try_touch(file, mtime, btime, quiet)
	return check('fs', 'touch', ok and file, '%s: %s', file, err)
end

--TODO: remove this or incorporate into ls() ?
function ls_dir(path, patt, min_mtime, create, order_by, recursive)
	if istab(path) then
		local t = path
		path, patt, min_mtime, create, order_by, recursive =
			t.path, t.find, t.min_mtime, t.create, t.order_by, t.recursive
	end
	local t = {}
	local create = create or function(file) return {} end
	if recursive then
		for sc in scandir(path) do
			local file, err = sc:name()
			if not file and err == 'not_found' then break end
			check('fs', 'dir', file, 'dir listing failed for %s: %s', sc:path(-1), err)
			if     (not min_mtime or sc:attr'mtime' >= min_mtime)
				and (not patt or file:find(patt))
			then
				local f = create(file, sc)
				if f then
					f.name    = file
					f.path    = sc:path()
					f.relpath = sc:relpath()
					f.type    = sc:attr'type'
					f.mtime   = sc:attr'mtime'
					f.btime   = sc:attr'btime'
					t[#t+1] = f
				end
			end
		end
	else
		for file, d in ls(path) do
			if not file and d == 'not_found' then break end
			check('fs', 'dir', file, 'dir listing failed for %s: %s', path, d)
			if     (not min_mtime or d:attr'mtime' >= min_mtime)
				and (not patt or file:find(patt))
			then
				local f = create(file, d)
				if f then
					f.name    = file
					f.path    = d:path()
					f.relpath = file
					f.type    = sc:attr'type'
					f.mtime   = d:attr'mtime'
					f.btime   = d:attr'btime'
					t[#t+1] = f
				end
			end
		end
	end
	sort(t, cmp(order_by or 'mtime path'))
	log('', 'fs', 'dir', '%-20s %5d files%s%s', path,
		#t,
		patt and '\n  match: '..patt or '',
		min_mtime and '\n  mtime >= '..date('%d-%m-%Y %H:%M', min_mtime) or '')
	local i = 0
	return function()
		i = i + 1
		return t[i]
	end
end

local function toid(s, field) --validate id minimally.
	local n = tonumber(s)
	if n and n >= 0 and floor(n) == n then return n end
 	return nil, '%s invalid: %s', field or 'field', s
end
function gen_id(name, start, quiet)
	local next_id_file = varpath'next_'..name
	if not exists(next_id_file) then
		save(next_id_file, tostring(start or 1), nil, quiet)
	else
		touch(next_id_file, nil, nil, quiet)
	end
	local n = tonumber(load(next_id_file))
	check('fs', 'gen_id', toid(n, next_id_file))
	save(next_id_file, tostring(n + 1), nil, quiet)
	log('note', 'fs', 'gen_id', '%s: %d', name, n)
	return n
end

--free space reporting -------------------------------------------------------

cdef[[
int statfs(const char *path, struct statfs *buf);
typedef long int __fsword_t;
typedef unsigned long int fsblkcnt_t;
typedef struct { int __val[2]; } fsid_t;
typedef unsigned long int fsfilcnt_t;
struct statfs {
	__fsword_t f_type;    /* Type of filesystem (see below) */
	__fsword_t f_bsize;   /* Optimal transfer block size */
	fsblkcnt_t f_blocks;  /* Total data blocks in filesystem */
	fsblkcnt_t f_bfree;   /* Free blocks in filesystem */
	fsblkcnt_t f_bavail;  /* Free blocks available to
									 unprivileged user */
	fsfilcnt_t f_files;   /* Total inodes in filesystem */
	fsfilcnt_t f_ffree;   /* Free inodes in filesystem */
	fsid_t     f_fsid;    /* Filesystem ID */
	__fsword_t f_namelen; /* Maximum length of filenames */
	__fsword_t f_frsize;  /* Fragment size (since Linux 2.6) */
	__fsword_t f_flags;   /* Mount flags of filesystem (since Linux 2.6.36) */
	__fsword_t f_spare[4]; /* Padding bytes reserved for future use */
};
]]
local statfs_ct = ctype'struct statfs'
local statfs_buf
local function statfs(path)
	statfs_buf = statfs_buf or statfs_ct()
	local ok, err = check_errno(C.statfs(path, statfs_buf) == 0)
	if not ok then return nil, err end
	return statfs_buf
end

function fs_info(path)
	local buf, err = statfs(path)
	if not buf then return nil, err end
	local t = {}
	t.size = tonumber(buf.f_blocks * buf.f_bsize)
	t.free = tonumber(buf.f_bfree  * buf.f_bsize)
	return t
end

--pollable pid files ---------------------------------------------------------

--NOTE: Linux 5.3+ feature, not used yet. Intended to replace polling
--for process status change in proc_posix.lua.

local PIDFD_NONBLOCK = 0x000800

function pidfd_open(pid, opt, quiet)
	local async = not (opt and opt.async == false)
	local flags = async and PIDFD_NONBLOCK or 0
	local fd = syscall(434, pid, flags)
	if fd == -1 then
		return check_errno()
	end
	local f, err = file_wrap_fd(fd, opt, async, 'pidfile', nil, quiet)
	if not f then
		return nil, err
	end
	return f
end

--metatypes ------------------------------------------------------------------

file.close    = unprotect_io(file.try_close)
file.read     = unprotect_io(file.try_read)
file.write    = unprotect_io(file.try_write)
file.readn    = unprotect_io(file.try_readn)
file.readall  = unprotect_io(file.try_readall)
file.flush    = unprotect_io(file.try_flush)
file.truncate = unprotect_io(file.try_truncate)
file.seek     = unprotect_io(file.try_seek)
file.skip     = unprotect_io(file.try_skip)

metatype(stream_ct, stream)
metatype(dir_ct, dir)
