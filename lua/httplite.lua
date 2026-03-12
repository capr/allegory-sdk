--[=[

	HTTP 1.1 light client & server protocol
	Written by Cosmin Apreutesei. Public Domain.



]=]

if not ... then require'httplite_test'; return end

require'glue'
require'pbuffer'
require'gzip'
require'sock'

--http server ----------------------------------------------------------------

local server = {}
local server_req = {}

function http_server(opt)
	local self = update(server, {
		--
	}, opt)

	if self.debug and self.debug.tracebacks then
		self.f.tracebacks = true --for check_io()
	end
	if not (self.debug and self.debug.protocol) then
		self.dp = noop
	end
	if self.debug and self.debug.stream then
		self.f:debug'http'
	end

	self.recv_buffer_size = self.recv_buffer_size or self.f:getopt'so_rcvbuf'

	self.b = pbuffer{
		f = self.f,
		readahead = self.recv_buffer_size,
		lineterm = '\r\n',
	} --for reading only

	return self
end

function server:read_request()
	local req = object(server_req, {
		server = self,
		headers = {},
	})
	self.start_time = clock()

	--read request line
	local line = self.b:needline()
	req.method, req.uri, req.http_version =
		line:match'^([%u]+)%s+([^%s]+)%s+HTTP/(%d+%.%d+)'
	self:dp('<=', '%s %s HTTP/%s', req.method, req.uri, req.http_version)
	self.f:checkp(req.method and req.http_version == '1.1', 'invalid request line')

	--read request headers
	while 1 do
		local line = self.b:needline()
		if line == '' then break end --headers end up with a blank line
		local name, value = line:match'^([^:]+):%s*(.*)'
		self.f:checkp(name, 'invalid header')
		name = name:lower() --header names are case-insensitive
		value = value:gsub('%s+', ' ') --multiple spaces equal one space.
		value = value:gsub('%s*$', '') --around-spaces are meaningless.
		self:dp('<-', '%-17s %s', name, value)
		local prev_value = req.headers[name]
		if prev_value then --duplicate header: fold.
			req.headers[name] = prev_value .. ',' .. value
		else
			req.headers[name] = value
		end
	end

	--parse request headers

	req.close = req.headers['connection'] and req.headers['connection'].close
	return req

end

--http client ----------------------------------------------------------------

local client = {}

