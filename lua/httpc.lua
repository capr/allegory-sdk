--[=[

	Async http(s) downloader.
	Written by Cosmin Apreutesei. Public Domain.

	Features https, gzip compression, persistent connections, pipelining,
	multiple client IPs, resource limits, auto-redirects, auto-retries,
	cookie jars, multi-level debugging, caching, cdata-buffer-based I/O.

CLIENT
	http_client(opt1,...) -> client   create a client object
	client:request(opt) -> req        make a HTTP request

]=]

require'glue'
require'json'
require'url'
require'sock'
require'sock_bearssl'
require'gzip'
require'fs'
require'resolver'
require'http_date'

--http connection object -----------------------------------------------------

local htcp = {}

function http_conn(tcp, opt)

	local htcp = tcp

	local rb = pbuffer{
		f = tcp,
		readahead = recv_buffer_size,
		lineterm = '\r\n',
		linesize = 8192,
	} --read buffer

	local wb = pbuffer{
		f = ctcp,
	} --write buffer

	local req
	local headers_sent
	local body_sent

	function htcp:start_request(opt, cookies)
		assert(not req)
		req = update({
			tcp = self,
			method = 'GET',
			uri = '/',
			headers = {},
		}, opt)
		self.req = req
	end

	function htcp:send_headers()
		assert(req)
		assert(not headers_sent)
		assert(not body_sent)

		assert(req.host, 'host missing') --required by http
		local default_port = self.https and 443 or 80
		local port = self.port ~= default_port and self.port or nil
		req.headers['host'] = req.host..(port and ':'..port or '')

		if req.close then
			req.headers['connection'] = 'close'
		end

		if repl(opt.compress, nil, self.compress) ~= false then
			req.headers['accept-encoding'] = 'gzip'
		end

		req.headers['cookie'] = cookies

		local upload_len = req.headers['content-length'] or 0
		--if upload_len > 0 then

		local dt = req.request_timeout
		self.start_time = clock()
		self.tcp:setexpires(dt and self.start_time + dt or nil, 'w')

		--send request line
		assert(req.method and req.method == method:upper())
		assert(req.uri)
		self:dp('=>', '%s %s HTTP/1.1', req.method, req.uri)
		wb:putf('%s %s HTTP/1.1\r\n', req.method, req.uri)

		--send request headers.
		--header names are case-insensitive and can't contain newlines.
		for k,v in pairs(req.headers) do
			assert(not v:has'\n' and not v:has'\r')
			req:dp('->', '%-17s %s', k, v)
			wb:putf('%s: %s\r\n', k, v)
		end
		wb:putf'\r\n'
		wb:flush()
	end
	htcp.try_send_headers = protect_io(htcp.send_headers)

	function htcp:send_body_chunk(chunk, len)
		assert(headers_sent)
		function send_body_chunk(chunk, len)
			assert(headers_sent)
			assert(not body_sent)
			if not (chunk == nil and len == 'eof') then
				len = len or #chunk
				req:dp('>>', '%7d bytes', len)
				if req.response_headers['content-length'] then
					wb:putdata(chunk, len)
					wb:flush()
				else --chunked
					wb:putf('%X\r\n', len)
					wb:putdata(chunk, len)
					wb:put'\r\n'
					wb:flush()
				end
			else
				body_sent = true
				req:dp('>>', '%7d end', 0)
				if not req.response_headers['content-length'] then
					wb:put'0\r\n\r\n'
					wb:flush()
				end
			end
		end
	end
	--self:send_body(req.content, req.content_size, req.headers['transfer-encoding'])

	function htcp:read_headers()
		assert(headers_sent)
		for i = 1, 101 do
			local line = rb:needline()
			if line == '' then break end --headers end with a blank line
			ctcp:checkp(i <= 100, 'too many headers')
			local name, value = line:match'^([^:]+):%s*(.*)'
			self:checkp(name, 'invalid header')
			name = name:lower() --header names are case-insensitive
			value = value:trim()
			req:dp('<-', '%-17s %s', name, value)
			local prev_value = req.headers[name]
			if prev_value then --duplicate header: append value.
				if name == 'set-cookie' then
					add(req.headers[name], value)
				else
					req.headers[name] = prev_value .. ',' .. value
				end
			else
				req.headers[name] = value
			end
		end
	end

	function htcp:read_body_chunk()
		--
	end

	return tcp
end

--http client object ---------------------------------------------------------

local client = {
	type = 'http_client',
	max_conn = 50,
	max_conn_per_target = 20,
	max_pipelined_requests = 10,
	client_ips = {},
	max_redirects = 10,
	max_cookie_length = 8192,
	max_cookies = 1e6,
	max_cookies_per_host = 1000,
}

function client:dp(target, event, fmt, ...)
	if logging.filter[''] then return end
	local s = fmt and _(fmt, logargs(...)) or ''
	return log('', 'htcl', event, '%-4s %s %s', target or '', s)
end

--self-test ------------------------------------------------------------------

if not ... then

run(function()
	local tcp = connect('google.com:80')
	local tcp = http_conn(tcp, {
		host = 'google.com',
	})

end)

end
