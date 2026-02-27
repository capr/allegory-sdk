--[[

	fileno(FILE*) -> fd                           get stream's file descriptor
	file_wrap_file(FILE*, ...) -> f               wrap opened FILE* object

STDIO STREAMS
	f:stream(mode) -> fs                          open a FILE* object from a file
	fs:[try_]close()                              close the FILE* object

]]

cdef[[
int fileno(struct FILE *stream);
]]

function fileno(F)
	local fd = C.fileno(F)
	if fd == -1 then return check_errno() end
	return fd
end

function file_wrap_file(F, opt)
	local fd = C.fileno(F)
	if fd == -1 then return check_errno() end
	return file_wrap_fd(fd, opt, 'file')
end

--stdio streams --------------------------------------------------------------

cdef[[
typedef struct FILE FILE;
FILE *fdopen(int fd, const char *mode);
int fclose(FILE*);
]]

stream_ct = ctype'struct FILE'

function stream.close(fs)
	return check_errno(C.fclose(fs) == 0)
end

function file.stream(f, mode)
	local fs = C.fdopen(f.fd, mode)
	if fs == nil then return check_errno() end
	return fs
end

metatype(stream_ct, stream)
