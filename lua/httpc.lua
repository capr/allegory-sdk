--[[

	Async http(s) downloader.
	Written by Cosmin Apreutesei. Public Domain.

	Features https, gzip compression, persistent connections,
	multiple client IPs, resource limits, auto-redirects, auto-retries,
	cookie jars, multi-level debugging, caching, cdata-buffer-based I/O.

]]

require'glue'
require'json'
require'url'
require'sock'
require'sock_bearssl'
require'pbuffer'
require'gzip'
require'fs'
require'resolver'
require'http_date'

--http connection object -----------------------------------------------------

local http = {type = 'http_connection', debug_prefix = 'H'}

function http_conn(opt)
	local rb = pbuffer{
		f = opt.tcp,
		readahead = recv_buffer_size,
		lineterm = '\r\n',
		linesize = 8192,
	} --read buffer
	local wb = pbuffer{
		f = opt.tcp,
	} --write buffer
	return object(http, {
		rb = rb,
		wb = wb,
	}, opt)
end

function http:send_request_headers(req)
	assert(req.headers['host'], 'host missing') --required by http
	if repl(req.compress, nil, self.compress) ~= false then
		req.headers['accept-encoding'] = 'gzip'
	end
	local cookies = req.cookies
	if istab(cookies) then
		local t = {}
		for k,v in sortedpairs(cookies) do
			assert(not k:has'=')
			assert(not k:has';')
			assert(not v:has'=')
			assert(not v:has';')
			append(t, k, '=', v)
		end
		req.headers['cookie'] = cat(t, ';')
	end
	req.request_body_len = req.headers['content-length'] or 0
	req.request_body_unsent_len = req.request_body_len
	req.wb = self.wb

	self.tcp:settimeout(req.request_timeout, 'w')
	local wb = self.wb

	--send request line
	assert(req.method and req.method == req.method:upper())
	assert(req.uri)
	req:dp('=>', '%s %s HTTP/1.1', req.method, req.uri)
	wb:putf('%s %s HTTP/1.1\r\n', req.method, req.uri)

	--send request headers.
	--header names are case-insensitive and can't contain newlines.
	local t = {}
	for k,v in pairs(req.headers) do
		if not istab(v) then t[1] = v; v = t end --must be 'cookie'
		for _,v in ipairs(v) do
			assert(not v:has'\n' and not v:has'\r')
			req:dp('->', '%-17s %s', k, v)
			wb:putf('%s: %s\r\n', k, v)
		end
	end
	wb:putf'\r\n'
	wb:flush()

	return true
end



	local headers_sent
	function htcp:send_headers(req)
		assert(not headers_sent)
		assert()
		headers_sent = true
		headers_read = false
		body_read_state = nil

		assert(req.headers, 'headers required')
		assert(req.headers['host'], 'host header required')

		--local default_port = self.istlssocket and 443 or 80
		--local port = self.port ~= default_port and self.port or nil
		--..(port and ':'..port or '')
		--headers['host'] = host
		if req.close then
			req.headers['connection'] = 'close'
		end

		req.headers['accept-encoding'] = 'gzip'

		if req.cookies then
			local t = {}
			for k,v in sortedpairs(req.cookies) do
				assert(not k:has'=')
				assert(not k:has';')
				assert(not v:has'=')
				assert(not v:has';')
				append(t, k, '=', v)
			end
			req.headers['cookie'] = cat(t, ';')
		end

		local dt = req.request_timeout
		req.start_time = clock()
		self:setexpires(dt and req.start_time + dt or nil, 'w')

		--send request line
		assert(req.method == req.method:upper())
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

		local body_unsent_len = req.headers['content-length'] or 0

		function htcp:send_body_chunk(chunk, len)
			len = len or #chunk
			body_unsent_len = body_unsent_len - len
			self:checkp(body_unsent_len >= 0, 'upload size mismatch')
			req:dp('>>', '%7d bytes, left %7d', len, body_unsent_len)
			wb:putdata(chunk, len)
			wb:flush()
			return body_unsent_len
		end
		function htcp:send_body(body, len)
			local unsent = self:send_body_chunk(body, len)
			self:checkp(unsent == 0, 'upload size mismatch')
		end

		function htcp:read_response_headers()
			headers_read = true
			req.response_headers = {}
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
			chunked_te = req.response_headers['transfer-encoding'] == 'chunked'
			body_unread_len = req.response_headers['content-length']

			return req.response_headers
		end

	end
	htcp.try_send_headers = protect_io(htcp.send_headers)

	local rb_needs_reset
	function htcp:read_body_chunk(req)
		assert(headers_read)
		if body_read_state == 'eof' then
			return nil, 'eof', 0
		end
		if rb_needs_reset then
			rb:reset()
			rb_needs_reset = false
		end
		if body_read_state == 'chunked' then
			local line = self.b:needline()
			local len = tonumber(line:gsub(';.*', ''), 16) --len[; extension]
			self.f:checkp(len, 'invalid chunk size')
			self:dp('<<', '%7d bytes', len)
			if len > 0 then
				rb:reset()
				rb:need(len)
				rb_needs_reset = true
				return rb:ref()
			else --last chunk
				body_read_state = 'eof'
			end
			rb:needline()
		elseif body_
			rb:need(1)
			rb_needs_reset = true
			body_read_state = body_read_state - len
			local buf, len = rb:ref()
			self:dp('<<', '%7d bytes', len)
		end
		return buf, len, body_unread_len
	end

	function req.read_body(req)
		rb:need(body_unread_len)
		body_unread_len = 0
		return rb:ref()
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
