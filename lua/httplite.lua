--[=[

	HTTP 1.1 light client & server protocol
	Written by Cosmin Apreutesei. Public Domain.

	http_server(opt1,...) -> server   Create a http server merging multiple options tables
CONFIG
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
	opt.respond <- fn(req)         request handler
	opt.debug <- flags             debug flags: 'protocol tracebacks stream'
REQUEST
	req.headers -> {k=v}	          request headers (in lowercase)
	req.body_size -> n             request upload size in bytes
	req:read_body() -> buf,size    read whole body in a buffer
	req:read_body_chunk() -> buf,size,size_left  read body in chunks
	req:onfinish(fn)               run fn when request finishes, even if it raises
	req.thread                     the thread that handled the request
RESPONSE
	req.status <- n                set response status (default: 200)
	req.response_headers <- {k=v}  set response headers (in lowercase!)	
	req.response_size <- n         set response size (otherwise it's chunked TE)
	req.compress <- true           enable gzip compresion
	req:send_headers() -> req      send status line and headers
	req:send_chunk(s | buf,len) -> req    send body chunk
	req:end()                      end of response
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
require'sock_bearssl'
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

local function logerror(tcp, action, ...)
	if logging.filter.ERROR then return end
	log('ERROR', 'htsrv', action, '%-4s %s', tcp, _(...))
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
			request_id = next_request_id,
			response_status = 200,
			response_headers = {}, --put them in lowercase!
		})
		ownthreadenv().http_request = req
		next_request_id = next_request_id + 1

		--read request line
		local line = ctcp.rb:needline()
		local method, uri, http_version = line:match'^([%u]+)%s+([^%s]+)%s+HTTP/(%d+%.%d+)'
		self:dp('<=', '%s %s HTTP/%s', method, uri, http_version)
		ctcp:checkp(method and http_version == '1.1', 'invalid request line')
		req.method = method
		req.uri = uri

		--read request headers
		for i = 1, 100 do
			local line = ctcp.rb:needline()
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

		--parse relevant request headers into req fields.
		req.body_size = tonumber(req.headers['content-length']) or 0
		local cc = req.headers['connection']
		req.close = cc and cc:has'close'

		--make req methods for reading the request body and for responding.

		function req.onfinish(req, fn)
			after(req, 'finish', fn)
		end

		local rb_needs_reset = false
		local body_unread_len = req.body_size
		function req.read_body_chunk(req)
			if body_unread_len == 0 then
				return nil, 'eof', 0
			end
			if rb_needs_reset then
				ctcp.rb:reset()
				rb_needs_reset = false
			end
			ctcp.rb:need(1)
			local buf, len = ctcp.rb:ref()
			body_unread_len = body_unread_len - len
			rb_needs_reset = true
			return buf, len, body_unread_len
		end

		function req.read_body(req)
			ctcp.rb:need(body_unread_len)
			body_unread_len = 0
			return ctcp.rb:ref()
		end

		local headers_sent
		local out, out_thread
		function req.send_headers(req)
			headers_sent = true
			
			req.response_headers['date'] = http_date_format(time(), 'rfc1123')
			if req.close then
				req.response_headers['connection'] = 'close' 
			end
			if req.response_size then
				req.response_headers['content-length'] = req.response_size
			else
				req.response_headers['transfer-encoding'] = 'chunked'
			end

			--send status line
			assert(req.status >= 100 and req.status <= 999, 'invalid status code')
			self:dp('=>', '%s', req.status)
			ctcp.wb:putf('HTTP/1.1 %d\r\n', req.status)

			--send response headers.
			--header names are case-insensitive and can't contain newlines.
			--passing a table as value will generate duplicate headers for each value
			--set-cookie will be like that because it's not safe to send it folded.
			local t = {}
			for k,v in pairs(req.response_headers) do
				if not istab(v) then t[1] = v; v = t end
				for _,v in ipairs(v) do
					assert(not v:has'\n' and not v:has'\r')
					self:dp('->', '%-17s %s', k, v)
					ctcp.wb:putf('%s: %s\r\n', k, v)
				end
			end
			ctcp.wb:putf'\r\n'
			ctcp.wb:flush()

			if req.compress then
				--NOTE: on error, the gzip thread is left in suspended state (either
				--not yet started or waiting on write), and we could just abandon it
				--and it will get gc'ed along with the zlib object. The reason we go
				--the extra mile to make sure it always finishes is so it gets removed
				--from the logging.live list immediately.
				local content, gzip_thread = cowrap(function(yield, s)
					if s == false then return end --abort on entry
					local ok, err = deflate(content, yield, 64 * 1024, 'gzip')
					assert(ok or err == 'abort', err)
				end, 'http-gzip-encode %s', req)
				function req:onfinish() --called on errors too.
					if threadstatus(gzip_thread) ~= 'dead' then
						content(false, 'abort')
					end
				end
				req.gzip_thread = gzip_thread
			end

			return req
		end

		function req.send_chunk(req, chunk, len)
			len = len or #chunk
			self:dp('>>', '%7d bytes', len)
			if req.response_size then
				ctcp.wb:put(chunk, len)
				ctcp.wb:flush()
			else --chunked
				ctcp.wb:putf('%X\r\n', len)
				ctcp.wb:put(chunk, len)
				ctcp.wb:put'\r\n'
				ctcp.wb:flush()
			end
			return req
		end

		local body_sent
		function req.end(req)
			self:dp('>>', 'end')
			if not req.response_size then
				ctcp.wb:put'0\r\n\r\n'
				ctcp.wb:flush()
			end
			body_sent = true
			return req
		end

		function req.out_function(req)
			out, out_thread = cowrap(function(yield)
				error'here error'
				pr'11'
				opt.content = yield
				pr'11'
				req:respond(opt)
				pr'22'
			end, 'http-server-out %s %s', ctcp, req.uri)
			pr'called'
			error'just'
			out()
			return out
		end

		--self.respond(req) needs to call req:respond(opt) or it's a 404.
		local ok, err = pcall(self.respond, req)

		if req.finish then
			req:finish(ok, err)
		end

		if not ok then
			if not headers_sent then
				if iserror(err, 'http_response') then
					req:respond(err)
				else
					logerror(ctcp, 'respond', '%s', err)
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
		elseif not body_sent then
			if out then --out() thread waiting for eof
				out() --signal eof
			else --respond() not called
				req:respond{status = 404}
			end
		end

		--the request must be entirely read before we can read the next request.
		while req:read_body_chunk() do end

		--close connection if asked.
		if req.close then
			--this is the "http graceful close" you hear about: we send a FIN to
			--the client then we wait for it to close the connection in response
			--to our FIN, and only after that we can close our end.
			--if we'd just call close() that would send a RST to the client which
			--would cut short the client's pending input stream (it's how TCP works).
			ctcp:shutdown'w'
			while ctcp.rb:have(1) do ctcp.rb:reset() end --read until peer closes.
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
				logerror(tcp, 'accept', '%s', err)
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
				ctcp.rb = pbuffer{
					f = ctcp,
					readahead = recv_buffer_size,
					lineterm = '\r\n',
					linesize = 8192,
					tracebacks = self.debug.tracebacks,
				} --read buffer
				ctcp.wb = pbuffer{
					f = ctcp,
					tracebacks = self.debug.tracebacks,
				} --write buffer
				local ok, err = pcall(handle_connection, ctcp)
				ctcp.rb:free(); ctcp.rb = nil
				ctcp.wb:free(); ctcp.wb = nil
				if not ok or not iserror(err, 'io') then
					logerror(ctcp, 'handler', '%s', err)
				end
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
	log('note', 'htsrv', 'kill-all', '%-4s %s', tcp,
		cat(sort(imap(keys(self.sockets), logarg)), ' '))
	for _,s in ipairs(self.sockets) do
		s:close()
	end
end
