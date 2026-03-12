--[=[

	HTTP 1.1 light client & server protocol
	Written by Cosmin Apreutesei. Public Domain.

	TODO: bearssl SNI

http_server(opt1,...) -> server   Create a http server merging multiple options tables
	opt.listen                     {{addr=,...}, {addr=,...}}
		host                        Host header match
		addr                        IP address to listen to
		port                        TCP port to listen to
		tls                         use TLS on this socket
		tls_options                 TLS options, see sock_bearssl.lua
		unix_socket                 unix socket file to listen to
		unix_socket_perms           set perms on socket file after bind()
		unix_socket_user            set user  on socket file after bind()
		unix_socket_group           set group on socket file after bind()
	opt.respond(req)               respond callack
		req:respond(opt) -> out
			opt.headers <- {k=v}     override response headers
			opt.want_out_function    have respond() return an out() function
		req:onfinish(f)             add code to run when request finishes
		req.thread                  the thread that handled the request
	opt.debug                      nil ('protocol tracebacks stream')

CONFIG

	host                           'localhost'
	http_addr                      '0.0.0.0'
	http_port                      80
	http_unix_socket
	http_unix_socket_perms
	http_unix_socket_user
	http_unix_socket_group
	https_addr                     '0.0.0.0' (set to false to disable)
	https_port                     443
	https_crt_file                 var/HOST.crt or ../tests/localhost.crt
	https_key_file                 var/HOST.key or ../tests/localhost.key
	http_compress                  nil, means enabled (set to false to disable)
	http_debug                     nil (set to true to enable)

]=]

if not ... then require'httplite_test'; return end

require'glue'
require'pbuffer'
require'gzip'
require'sock'
require'http_date'

--http server ----------------------------------------------------------------

local server = {}
local server_req = {}

server.compressed_mime_types = index{
	'image/gif',
	'image/jpeg',
	'image/png',
	'image/x-icon',
	'font/woff',
	'font/woff2',
	'application/pdf',
	'application/zip',
	'application/x-gzip',
	'application/x-xz',
	'application/x-bz2',
	'audio/mpeg',
	'text/event-stream',
}

local header_parse = {}
local header_format = {}

local function format_date(t)
	return http_date_format(t, 'rfc1123')
end

function server_req:onfinish(f)
	after(self, 'finish', f)
end

function server_req:read_body(headers, write, from_server, close, state)
	if write == 'string' or write == 'buffer' then
		local to_string = write == 'string'
		local write, collect = dynarray_pump()
		self:read_body_to_writer(headers, write, from_server, close, state)
		local buf, sz = collect()
		if to_string then
			return ffi.string(buf, sz)
		else
			return buf, sz
		end
	elseif write == 'reader' then
		--don't read the body, but return a reader function for it instead.
		return (cowrap(function(yield)
			self:read_body_to_writer(headers, yield, from_server, close, state)
			--not returning anything here signals eof.
		end, 'http-read-body %s', self.f))
	else --function or nil
		self:read_body_to_writer(headers, write, from_server, close, state)
		if write then write() end --signal eof to writer.
		return true --signal that content was read.
	end
end

function http_server(...)

	local self = object(server, {}, ...)

	if not self.listen then
		self.listen = {}
		local host = config('host', 'localhost')
		if config'http_addr' ~= false then
			add(self.listen, {
				host = host,
				addr = config('http_addr', '0.0.0.0'),
				port = config'http_port',
				unix_socket = config'http_unix_socket',
				unix_socket_perms = config'http_unix_socket_perms',
				unix_socket_user  = config'http_unix_socket_user',
				unix_socket_group = config'http_unix_socket_group',
			})
		end
		if config'https_addr' ~= false then
			local crt_file = config'https_crt_file' or varpath(host..'.crt')
			local key_file = config'https_key_file' or varpath(host..'.key')
			if host == 'localhost'
				and not config'https_crt_file'
				and not config'https_key_file'
				and not exists(crt_file)
				and not exists(key_file)
			then --use bundled-in self-signed certs for localhost
				crt_file = exedir()..'/../tests/localhost.crt'
				key_file = exedir()..'/../tests/localhost.key'
			end
			add(self.listen, {
				host = host,
				addr = config('https_addr', '0.0.0.0'),
				port = config'https_port',
				tls = true,
				tls_options = {
					cert_file = crt_file,
					key_file  = key_file,
				},
			})
		end
	end
	assert(self.listen and #self.listen > 0, 'listen option is missing or empty')

	self.debug = index(collect(words(self.debug or config'http_debug' or '')))
	if not self.debug.protocol then
		self.dp = noop
	end

	local next_request_id = 1

	local function handle_request(ctcp)
		local req = object(server_req, {
			tcp = ctcp,
			server = self,
			headers = {},
			start_time = clock(),
			thread = currentthread(),
		})

		--read request line
		local line = ctcp.b:needline()
		local method, uri, http_version = line:match'^([%u]+)%s+([^%s]+)%s+HTTP/(%d+%.%d+)'
		self:dp('<=', '%s %s HTTP/%s', method, uri, http_version)
		ctcp:checkp(method and http_version == '1.1', 'invalid request line')
		req.method = method
		req.uri = uri

		--read request headers
		for i = 1, 100 do
			local line = ctcp.b:needline()
			if line == '' then break end --headers end with a blank line
			local name, value = line:match'^([^:]+):%s*(.*)'
			ctcp:checkp(name, 'invalid header')
			name = name:lower() --header names are case-insensitive
			value = value:trim()
			self:dp('<-', '%-17s %s', name, value)
			local prev_value = req.headers[name]
			if prev_value then --duplicate header: fold.
				req.headers[name] = prev_value .. ',' .. value
			else
				req.headers[name] = value
			end
		end

		--parse request headers
		for k,v in pairs(req.headers) do
			local parse = header_parse[k]
			if parse then
				req.headers[k] = parse(v)
			end
		end

		--read request body and send a response back.
		ownthreadenv().http_request = req
		req.request_id = next_request_id
		next_request_id = next_request_id + 1

		local close, out, out_thread, send_started, send_finished

		local function send_response(opt)
			send_started = true

			local headers = {}

			close = opt.close
				or (req.headers['connection'] and req.headers['connection']:has'close')
			if close then
				headers['connection'] = 'close'
			end

			res.headers['date'] = format_date(time())

			local content, content_size =
				self:encode_content(opt.content or '', opt.content_size, content_encoding)

			if isstr(content) then
				assert(not content_size, 'content_size would be ignored')
				headers['content-length'] = #content
			elseif iscdata(content) then
				headers['content-length'] = assert(content_size, 'content_size missing')
			elseif isfunc(content) then
				if content_size then
					headers['content-length'] = content_size
				elseif not close then
					headers['transfer-encoding'] = 'chunked'
				end
			else
				assertf(false, 'invalid content: %s', type(content))
			end
			update(headers, opt.headers)

			--send status line
			local status = opt.status or 200
			assert(status >= 100 and status <= 999, 'invalid status code')
			local s = _('HTTP/1.1 %d\r\n', status)
			self:dp('=>', '%s', status)
			req.tcp:send(s)

			--send response headers.
			--header names are case-insensitive and can't contain newlines.
			--passing a table as value will generate duplicate headers for each value
			--set-cookie will come like that because it's not safe to send it folded.
			for k,v in sortedpairs(headers) do
				local hformat = header_format[k]
				if istab(v) then --must be sent unfolded.
					for i,v in ipairs(v) do
						if hformat then v = hformat(v) end
						self:dp('->', '%-17s %s', k, v)
						req.tcp:send(_('%s: %s\r\n', k, v))
					end
				else
					if hformat then v = hformat(v) end
					self:dp('->', '%-17s %s', k, v)
					req.tcp:send(_('%s: %s\r\n', k, v))
				end
			end
			req.tcp:send'\r\n'

			--

			send_finished = true
		end --send_response()

		--NOTE: both req:respond() and out() raise on I/O errors breaking
		--user's code, so use req:onfinish() to free resources.
		function req.respond(req, opt)
			if opt.want_out_function then
				out, out_thread = cowrap(function(yield)
					opt.content = yield
					send_response(opt)
				end, 'http-server-out %s %s', ctcp, req.uri)
				out()
				return out
			else
				send_response(opt)
			end
		end

		--self.respond(req) needs to call req:respond(opt) or it's a 404.
		local ok, err = pcall(self.respond, req)
		if req.finish then
			req:finish(ok, err)
		end

		if not ok then
			if not send_started then
				if iserror(err, 'http_response') then
					req:respond(err)
				else
					self:check(ctcp, false, 'respond', '%s', err)
					req:respond{status = 500}
				end
			else --status line already sent, too late to send HTTP 500.
				if out_thread and threadstatus(out_thread) ~= 'dead' then
					--Signal eof so that the out() thread finishes. We could
					--abandon the thread and it will be collected without leaks
					--but we want it to be removed from logging.live immediately.
					--NOTE: we're checking that out_thread is really suspended
					--because we also get here on I/O errors which kill it.
					out()
				end
				error(err)
			end
		elseif not send_finished then
			if out then --out() thread waiting for eof
				out() --signal eof
			else --respond() not called
				req:respond{status = 404}
			end
		end

		--the request must be entirely read before we can read the next request.
		if req.body_was_read == nil then
			req:read_body()
		end

		--close connection if asked for.
		if close then
			--this is the "http graceful close" you hear about: we send a FIN to
			--the client then we wait for it to close the connection in response
			--to our FIN, and only after that we can close our end.
			--if we'd just call close() that would send a RST to the client which
			--would cut short the client's pending input stream (it's how TCP works).
			--TODO: limit how much traffic we absorb for this.
			ctcp:shutdown'w'
			ctcp.b:readall_to(noop)
			ctcp:close()
		end
	end --handle_request()

	local function handle_connection(ctcp)
		while not ctcp:closed() do
			handle_request(ctcp)
		end
	end

	self.sockets = {}

	for _,listen_opt in ipairs(self.listen) do

		local addr =
			listen_opt.unix_socket and 'unix:'..listen_opt.unix_socket
			or (listen_opt.addr or '0.0.0.0')..':'..
				(listen_opt.port or (listen_opt.tls and 443 or 80))

		local tcp = listen(addr)

		if listen_opt.unix_socket then
			if listen_opt.unix_socket_perms or
				listen_opt.unix_socket_user  or
				listen_opt.unix_socket_group
			then
				file_attr(listen_opt.unix_socket, {
					perms = listen_opt.unix_socket_perms,
					uid   = listen_opt.unix_socket_user,
					gid   = listen_opt.unix_socket_group,
				})
			end
		end

		local tls = listen_opt.tls
		if tls then
			local opt = update({}, self.tls_options, listen_opt.tls_options)
			local stcp = server_stcp(tcp, opt)
			liveadd(stcp, 'listen=%s', tcp.bound_addr)
			tcp = stcp
		end

		if self.debug.tracebacks then
			tcp.tracebacks = true --for check_io()
		end
		if self.debug.stream then
			tcp:debug'http'
		end

		push(self.sockets, tcp)

		local function accept_connection()
			local ctcp, err, retry = tcp:try_accept()
			if not ctcp then
				if err == 'closed' then return end --stop() called
				self:check(tcp, false, 'accept', '%s', err)
				if retry then
					--temporary network error. let it retry but pause a little
					--to avoid killing the CPU while the error persists.
					wait(.2)
				else
					self:stop()
				end
				return
			end
			if self.debug.tracebacks then
				ctcp.tracebacks = true --for check_io()
			end
			if self.debug.stream then
				ctcp:debug'http'
			end
			local recv_buffer_size = ctcp:getopt'so_rcvbuf' --usually 128k
			resume(thread(function()
				ctcp.b = pbuffer{
					f = ctcp,
					readahead = recv_buffer_size,
					lineterm = '\r\n',
					linesize = 8192,
				} --for reading only
				local ok, err = pcall(handle_connection, ctcp)
				ctcp.b:free()
				ctcp.b = nil
				self:check(ctcp, ok or iserror(err, 'io'), 'handler', '%s', err)
			end, 'http-accept %s', ctcp))
		end

		resume(thread(function()
			while not tcp:closed() do
				accept_connection()
			end
		end, 'http-listen %s', tcp))

	end --for in listen

	return self
end

function server:stop()
	self:log(tcp, 'note', 'htsrv', 'kill-all', '%s',
		cat(sort(imap(keys(self.sockets), logarg)), ' '))
	for _,s in ipairs(self.sockets) do
		s:close()
	end
end

function server:log(tcp, severity, module, event, fmt, ...)
	if logging.filter[severity] then return end
	local s = isstr(fmt) and _(fmt, logargs(...)) or fmt or ''
	log(severity, module, event, '%-4s %s', tcp, s)
end

function server:check(tcp, ret, ...)
	if ret then return ret end
	self:log(tcp, 'ERROR', 'htsrv', ...)
end

--http client ----------------------------------------------------------------

local client = {}
