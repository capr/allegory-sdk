--[[

	fileno(FILE*) -> fd                           get stream's file descriptor
	file_wrap_file(FILE*, ...) -> f               wrap opened FILE* object

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
