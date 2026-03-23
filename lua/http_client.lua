--[=[

	Async http(s) downloader.
	Written by Cosmin Apreutesei. Public Domain.

	Features https, gzip compression, persistent connections, pipelining,
	multiple client IPs, resource limits, auto-redirects, auto-retries,
	cookie jars, multi-level debugging, caching, cdata-buffer-based I/O.

CLIENT
	http_client(opt1,...) -> client   create a client object
	  max_conn                        limit the number of total connections
	  max_conn_per_target             limit the number of connections per target
	  max_pipelined_requests          limit the number of pipelined requests
	  client_ips <- {ip1,...}         a list of client IPs to assign to requests
	  max_retries                     number of retries before giving up
	  max_redirects                   number of redirects before giving up
	  tls_options                     options to pass to sock_bearssl
	  debug <- flags                  debug flags: 'protocol tracebacks stream sched'
	client:request(opt) -> req        make a HTTP request
		TODO: document all options
		client_ip                      client ip to bind to (optional)
		connect_timeout                connect timeout (optional)
		request_timeout                timeout for the request part (optional)
		response_timeout               timeout for the response part (optional)
	req:upload_chunk(s | buf,sz)
	req:
	client:fetch(opt | url) -> s, ht  make a request and get response body and headers
	client:close_all()                close all connections
CONFIG
	http_debug                        nil (set to true to enable)
FETCH
	fetch(opt | url, [body]) -> content, req

NOTES

	A target is a combination of (vhost, client_ip) on which
	one or more HTTP connections can be created subject to per-target limits.

	client:request() must be called from a scheduled socket thread.

	client:close_all() must be called after the socket loop finishes.

Pipelined requests

	A pipelined request is a request that is sent in advance of receiving the
	response for the previous request on the same connection. Most HTTP servers
	accept these but in a limited number. Browsers don't have them though so if
	you use them you'll look like the robot that you really are to the servers.

	Spawning a new connection for a new request has a lot more initial latency
	than pipelining the request on an existing connection. On the other hand,
	pipelined responses need to come back in the same order as the requests
	and so the server might decide not to start processing pipelined requests
	as soon as they arrive because it would have to buffer the results before
	it can start sending them.

]=]

if not ... then require'http_client_test'; return end

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

function http:send_request(req, cookies)

	assert(req.host, 'host missing') --required by http
	req.headers['host'] = req.host

	if req.close then
		req.headers['connection'] = 'close'
	end

	if repl(req.compress, nil, self.client.compress) ~= false then
		req.headers['accept-encoding'] = 'gzip'
	end

	req.headers['cookie'] = cookies

	local upload_len = req.headers['content-length'] or 0
	if upload_len > 0 then
	end

	local dt = req.request_timeout
	self.start_time = clock()
	self.tcp:setexpires(dt and self.start_time + dt or nil, 'w')

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
http.try_send_request = protect_io(http.send_request)

function http:should_have_response_body(method, status)
	if method == 'HEAD' then return false end
	if status == 204 or status == 304 then return false end
	if status >= 100 and status < 200 then return false end
	return true
end

function http:should_redirect(req)
	local status = req.status
	return req.response_headers['location']
		and (status == 301 or status == 302 or status == 303 or status == 307)
end

function http:read_response_headers(req)
	local tcp = self.tcp
	local rb = self.rb
	tcp:settimeout(req.response_timeout, 'r')

	--read status line
	local line, err = rb:needline()
	if not line then return nil, err end
	local http_version, status, status_message
		= line:match'^HTTP/(%d+%.%d+)%s+(%d%d%d)%s*(.*)'
	req:dp('<=', '%s %s %s', status, status_message, http_version)
	status = tonumber(status)
	tcp:checkp(http_version and status, 'invalid status line: %s', line)
	tcp:checkp(http_version == '1.1', 'invalid http version: %s', http_version)
	tcp:checkp(status >= 200 and status <= 999, 'invalid status: %d', status)
	req.status = status
	req.status_message = status_message

	--read response headers
	req.response_headers = {}
	for i = 1, 101 do
		local line = rb:needline()
		if line == '' then break end --headers end with a blank line
		tcp:checkp(i <= 100, 'too many headers')
		local name, value = line:match'^([^:]+):%s*(.*)'
		tcp:checkp(name, 'invalid header')
		name = name:lower() --header names are case-insensitive
		value = value:trim()
		req:dp('<-', '%-17s %s', name, value)
		local prev_value = req.response_headers[name]
		if name == 'set-cookie' then --this header is not safe to combine.
			add(attr(req.response_headers, name), value)
		elseif prev_value then --duplicate header: append value.
			req.response_headers[name] = prev_value .. ',' .. value
		else
			req.response_headers[name] = value
		end
	end

	local hconn = req.response_headers['connection']
	req.close = req.close or (hconn and hconn:has'close')

	local receive_content = req.receive_content
	if self:should_redirect(req) then
		receive_content = nil --ignore the body (it's not the body we want)
		req.redirect_location = tcp:checkp(req.response_headers['location'], 'no location')
	end

	if not self:should_have_response_body(req.method, req.status) then
		function http:read_response_body_chunk(req)
			return true
		end

		return true
	end

	local rb_needs_reset, body_read_state
	local chunked_te = req.response_headers['transfer-encoding'] == 'chunked'
	local body_unread_len = req.response_headers['content-length']

	function http:read_response_body_chunk(req)

		if body_read_state == 'eof' then
			return nil, 'eof', 0
		end
		if rb_needs_reset then
			rb:reset()
			rb_needs_reset = false
		end
		if body_read_state == 'chunked' then
			local line = rb:needline()
			local len = tonumber(line:gsub(';.*', ''), 16) --len[; extension]
			tcp:checkp(len, 'invalid chunk size')
			req:dp('<<', '%7d bytes', len)
			if len > 0 then
				rb:reset()
				rb:need(len)
				self.rb_needs_reset = true
				return rb:ref()
			else --last chunk
				body_read_state = 'eof'
			end
			rb:needline()
		elseif body_
			rb:need(1)
			self.rb_needs_reset = true
			body_read_state = body_read_state - len
			local buf, len = rb:ref()
			self:dp('<<', '%7d bytes', len)
		end

		return buf, len, body_unread_len
	end


	return req
end

function http:cookie_domain_matches_request_host(domain, host)
	return not domain or domain == host
		or (host:ends('.'..domain) and not (is_ipv4(host) or is_ipv6(host)))
end

function http:cookie_default_path(uri)
	return '/' --TODO
end

--cookie path matches request path exactly, or
--cookie path ends in `/` and is a prefix of the request path, or
--cookie path is a prefix of the request path, and the first
--character of the request path that is not included in the cookie path is `/`.
function http:cookie_path_matches_request_path(cpath, path)
	if cpath == rpath then
		return true
	elseif cpath == rpath:sub(1, #cpath) then
		if cpath:sub(-1, -1) == '/' then
			return true
		elseif rpath:sub(#cpath + 1, #cpath + 1) == '/' then
			return true
		end
	end
	return false
end

--http client object ---------------------------------------------------------

local function pull(t)
	return remove(t, 1)
end

local client = {
	type = 'http_client',
	max_conn = 50,
	max_conn_per_target = 20,
	max_pipelined_requests = 10,
	client_ips = {},
	max_redirects = 20,
	max_cookie_length = 8192,
	max_cookies = 1e6,
	max_cookies_per_host = 1000,
}

function client:dp(target, event, fmt, ...)
	if logging.filter[''] then return end
	local s = fmt and _(fmt, logargs(...)) or ''
	return log('', 'htcl', event, '%-4s %s %s', target or '', s)
end

--targets --------------------------------------------------------------------

--A target is a combination of (vhost, client_ip) on which one or more
--HTTP connections can be created subject to per-target limits.

function client:assign_client_ip(host)
	if #self.client_ips == 0 then return end
	local i = (self.last_client_ip_index[host] or 0) + 1
	if i > #self.client_ips then i = 1 end
	self.last_client_ip_index[host] = i
	return self.client_ips[i]
end

function client:target(opt) --opt is request options
	local host = assert(opt.host, 'host missing'):lower()
	local https = opt.https and true or false
	local client_ip = opt.client_ip or self:assign_client_ip(host)
	local target_key = host .. (client_ip and ' '..client_ip or '')
	local target = attr(self.targets, target_key)
	if not target.type then --just created
		target.type = 'http_target'
		target.debug_prefix = '@'
		target.host = host
		target.client_ip = client_ip
		target.ready = {} --ready conn FIFO
		target.conn_count = 0
		--NOTE: these are set once so we assume they are static per target.
		target.connect_timeout = opt.connect_timeout
		target.max_pipelined_requests = opt.max_pipelined_requests
		target.max_conn = opt.max_conn_per_target
		target.max_redirects = opt.max_redirects
	end
	return target
end

--connection pool ------------------------------------------------------------

function client:adjust_conn_count(target, n)
	self.conn_count = self.conn_count + n
	target.conn_count = target.conn_count + n
	self:dp(target, (n > 0 and '+' or '-')..'CO', '=%d, total=%d',
		target.conn_count, self.conn_count)
end

function client:push_ready_conn(target, http)
	push(target.ready, http)
	self:dp(target, '+READY', '%s', http)
end

function client:pull_ready_conn(target)
	local http = target.ready and pull(target.ready)
	if not http then return end
	self:dp(target, '-READY', '%s', http)
	return http
end

function client:push_wait_conn_thread(thread, target)
	local queue = self.wait_conn_queue
	push(queue, {thread, target})
	self:dp(target, '+WAIT_CO', '%s %s Q: %d', thread, target, #queue)
end

function client:pull_wait_conn_thread()
	local queue = self.wait_conn_queue
	local t = queue and pull(queue)
	if not t then return end
	local thread, target = t[1], t[2]
	self:dp(target, '-WAIT_CO', '%s Q: %d', thread, #queue)
	return thread, target
end

function client:pull_matching_wait_conn_thread(target)
	local queue = self.wait_conn_queue
	if not queue then return end
	for i,t in ipairs(queue) do
		if t[2] == target then
			remove(queue, i)
			local thread = t[1]
			self:dp(target, '-WAIT_CO', '%s: %s Q: %d', target, thread, #queue)
			return thread
		end
	end
end

function client:can_connect_now(target)
	local can = self.conn_count < self.max_conn
	if can and target then
		can = target.conn_count < (target.max_conn or self.max_conn_per_target)
	end
	self:dp(target, '?CAN_CO', '%s', can)
	return can
end

function client:connect_now(target)
	local host = target.host
	local client_ip = target.client_ip
	local tcp = tcp()
	if target.debug and target.debug.stream then
		tcp:debug'http'
	end
	if client_ip then
		local ok, err = tcp:try_bind(client_ip)
		if not ok then return nil, err end
	end
	self:adjust_conn_count(target, 1)
	local ok, err = try_connect(tcp, host, target.https and 443 or 80, target.connect_timeout)
	self:dp(target, '+CO', '%s %s', tcp, err or '')
	if not ok then
		self:adjust_conn_count(target, -1)
		return nil, err
	end
	if target.https then
		local stcp, err = client_stcp(tcp, host, self.tls_options)
		self:dp(target, ' TLS', '%s %s', stcp, err or '')
		if not stcp then
			return nil, err
		end
		tcp = stcp
	end
	tcp:onclose(function(tcp)
		self:dp(target, '-CO', '%s', tcp)
		self:adjust_conn_count(target, -1)
		self:resume_next_wait_conn_thread()
	end)
	local rb = pbuffer{
		f = tcp,
		readahead = recv_buffer_size,
		lineterm = '\r\n',
		linesize = 8192,
	} --read buffer
	local wb = pbuffer{
		f = tcp,
	} --write buffer
	local http = object(http, {
		client = client,
		tcp = tcp,
		rb = rb,
		wb = wb,
	})
	self:dp(target, ' BIND', '%s %s', tcp, http)
	return http
end

function client:wait_conn(target)
	local thread = currentthread()
	self:push_wait_conn_thread(thread, target)
	local http = suspend()
	if http == 'connect' then
		return self:connect_now(target)
	else
		return http
	end
end

function client:get_conn(target)
	local http, err = self:pull_ready_conn(target)
	if http then return http end
	if self:can_connect_now(target) then
		return self:connect_now(target)
	else
		return self:wait_conn(target)
	end
end

function client:resume_next_wait_conn_thread()
	local thread, target = self:pull_wait_conn_thread()
	if not thread then return end
	self:dp(target, '^WAIT_CO', '%s', thread)
	resume(thread, 'connect')
end

function client:resume_matching_wait_conn_thread(target, http)
	local thread = self:pull_matching_wait_conn_thread(target)
	if not thread then return end
	self:dp(target, '^WAIT_CO', '%s < %s', thread, http)
	resume(thread, http)
	return true
end

function client:can_pipeline_new_requests(http, target, req)
	local close = req.close
	local pr_count = http.wait_response_threads and #http.wait_response_threads or 0
	local max_pr = target.max_pipelined_requests or self.max_pipelined_requests
	local can = not close and pr_count < max_pr
	self:dp(target, '?CAN_PIPE', '%s (wait: %d, close: %s)', can, pr_count, close)
	return can
end

--pipelining -----------------------------------------------------------------

function client:push_wait_response_thread(http, thread, target)
	push(attr(http, 'wait_response_threads'), thread)
	self:dp(target, '+WAIT_RS', 'wait: %d', #http.wait_response_threads)
end

function client:pull_wait_response_thread(http, target)
	local queue = http.wait_response_threads
	local thread = queue and pull(queue)
	if not thread then return end
	self:dp(target, '-WAIT_RS', 'wait: %d', #queue)
	return thread
end

function client:read_response_headers(http, req)
	http.reading_response = true
	self:dp(http.target, '+READ_RS', '%s.%s.%s', http.target, http, req)
	return http:read_response_headers(req)
end

function client:read_response_body_chunk(http, req)
	local chunk, len, left = http:read_response_body_chunk(req)
	if chunk and left > 0 then return chunk, len, left end
	self:dp(http.target, '-READ_RS', '%s.%s.%s', http.target, http, req)
	http.reading_response = false
	return chunk, len
end

function client:read_response_body(http, req)
	local ok, err = http:read_response_body(req)
	self:dp(http.target, '-READ_RS', '%s.%s.%s %s', http.target, http, req, err or '')
	http.reading_response = false
	return ok, err
end

--redirects ------------------------------------------------------------------

function client:redirect_request_args(t, req, res)
	local location = assert(req.redirect_location, 'no location')
	local loc = url_parse(location)
	local uri = url_format{
		path = loc.path,
		query = loc.query,
		fragment = loc.fragment,
	}
	local https = loc.scheme == 'https' or nil
	local port = loc.port or (not loc.host and t.port) or nil
	local host = loc.host or t.host
	if port then host = host..':'..port end
	return {
		method = 'GET',
		close = t.close,
		host = host,
		https = https,
		uri = uri,
		compress = t.compress,
		headers = merge({['content-type'] = false}, t.headers),
		receive_content = req.receive_content,
		redirect_count = (t.redirect_count or 0) + 1,
		connect_timeout = t.connect_timeout,
		request_timeout = t.request_timeout,
		respone_timeout = t.response_timeout,
		debug = t.debug or self.debug,
	}
end

--cookie management ----------------------------------------------------------

function client:accept_cookie(cookie, host, http)
	return http:cookie_domain_matches_request_host(cookie.domain, host)
end

function client:cookie_jar(ip)
	return attr(attr(self, 'cookies'), ip or '*')
end

function client:remove_cookie(jar, domain, path, name)
	--
end

function client:clear_cookies(client_ip, host)
	--
end

function client:store_cookies(target, req, res)
	local cookies = req.response_headers['set-cookie']
	if not cookies then return end
	local time = time()
	local client_jar = self:cookie_jar(target.client_ip)
	local host = target.host
	for _,cookie in ipairs(cookies) do
		if self:accept_cookie(cookie, host, req.http) then
			local expires
			if cookie.expires then
				expires = cookie.expires
			elseif cookie['max-age'] then
				expires = time + cookie['max-age']
			end
			local domain = cookie.domain or host
			local path = cookie.path or http:cookie_default_path(req.uri)
			if expires and expires < time then --expired: remove from jar.
				self:remove_cookie(client_jar, domain, path, cookie.name)
			else
				local sc = attr(attr(attr(client_jar, domain), path), cookie.name)
				sc.wildcard = cookie.domain and true or false
				sc.secure = cookie.secure
				sc.expires = expires
				sc.value = cookie.value
			end
		end
	end
end

function client:get_cookies(client_ip, host, uri, https)
	local client_jar = self:cookie_jar(client_ip)
	if not client_jar then return end
	local path = uri:match'^[^%?#]+'
	local time = time()
	local cookies = {}
	local names = {}
	for s in host:gmatch'[^%.]+' do
		push(names, s)
	end
	local domain = names[#names]
	for i = #names-1, 1, -1 do
		domain = names[i] .. '.' .. domain
		local domain_jar = client_jar[domain]
		if domain_jar then
			for cpath, path_jar in pairs(domain_jar) do
				if http:cookie_path_matches_request_path(cpath, path) then
					for name, sc in pairs(path_jar) do
						if sc.expires and sc.expires < time then --expired: auto-clean.
							self:remove_cookie(client_jar, domain, cpath, sc.name)
						elseif https or not sc.secure then --allow
							cookies[name] = sc.value
						end
					end
				end
			end
		end
	end
	return cookies
end

function client:save_cookies(file)
	return save(file, pp(self.cookies))
end

function client:load_cookies(file)
	local s, err = try_load(file)
	if not s then return nil, err end
	local t, err = try_eval(s)
	if not t then return nil, err end
	self.cookies = t
end

--request call ---------------------------------------------------------------

local function req_dp(req, event, fmt, ...)
	if logging.filter[''] then return end
	local dt = clock() - req.start_time
	local s = fmt and _(fmt, logargs(...)) or ''
	log('', 'htcl', event, '%-4s %4dms %s', req.tcp, dt * 1000, s)
end

function client:request(opt)

	local target = self:target(opt)

	self:dp(target, '+RQ')

	local http, err = self:get_conn(target)
	if not http then return nil, err end

	local req = update({
		http = http,
		host = target.host,
		method = 'GET',
		uri = '/',
		headers = {},
		dp = self.debug.protocol and req_dp or noop,
	}, opt)

	local cookies = self:get_cookies(target.client_ip, target.host,
		req.uri, target.https)

	self:dp(target, '+SEND_RQ', '%s.%s.%s %s %s',
		target, http, req, req.method, req.uri)

	local ok, err = http:try_send_request(req, cookies)
	if not ok then return nil, err end

	self:dp(target, '-SEND_RQ', '%s.%s.%s', target, http, req)

	local waiting_response
	if http.reading_response then
		self:push_wait_response_thread(http, currentthread(), target)
		waiting_response = true
	else
		http.reading_response = true
	end

	local taken
	if self:can_pipeline_new_requests(http, target, req) then
		taken = true
		if not self:resume_matching_wait_conn_thread(target, http) then
			self:push_ready_conn(target, http)
		end
	end

	if waiting_response then
		suspend()
	end

	local ok, err = self:read_response_headers(http, req)
	if not ok then return nil, err, req end

	self:store_cookies(target, req)

	if not taken and not http.tcp:closed() then
		if not self:resume_matching_wait_conn_thread(target, http) then
			self:push_ready_conn(target, http)
		end
	end

	if not http.tcp:closed() then
		local thread = self:pull_wait_response_thread(http, target)
		if thread then
			resume(thread)
		end
	end

	self:dp(target, '-RQ', '%s.%s body: %d bytes', http, req,
		res and isstr(res.content) and #res.content or 0)

	if req and req.redirect_location then
		local t = self:redirect_request_args(t, req)
		local max_redirects = target.max_redirects or self.max_redirects
		if t.redirect_count >= max_redirects then
			return nil, 'too many redirects', req
		end
		return self:request(t)
	end

	return req, true
end

--hi-level API: fetch --------------------------------------------------------

--opt | url,[body]
function client:fetch(arg1, body)

	local opt = istab(arg1) and arg1 or empty
	body = body or opt.body

	local headers = {}

	if body ~= nil and not isstr(body) then
		body = json_encode(body)
		headers['content-type'] = 'application/json'
	end
	if body then
		headers['content-length'] = #body
	end

	local url = isstr(arg1) and arg1 or opt.url
	local u = url and url_parse(url)

	local opt = update({
		host = u and u.host,
		uri = u and u.path,
		https = u and u.scheme == 'https' or not u and opt.https ~= false,
		method = body and 'POST',
		body = body,
	}, opt)
	opt.headers = update(headers, opt.headers)

	local req, err = self:request(opt)

	if not req then
		return nil, err
	end

	local ct = req.response_headers['content-type']
	if ct and ct.media_type == 'application/json' then
		req.response = json_decode(req.response)
		--if the entire resonse is the json value "null", then return null
		--because nil is for errors.
		req.response = repl(req.response, nil, null)
	end

	return req.response, req
end

--instantiation --------------------------------------------------------------

function http_client(...)

	local self = object(client, {}, ...)

	self.debug = self.debug or config'http_debug' or ''
	if isstr(self.debug) then
		self.debug = index(collect(words(self.debug)))
	end

	self.wait_conn_queue = {} -- {{thread, target}, ...}
	self.last_client_ip_index = {} -- host -> index
	self.targets = {} -- 'HOST CLIENT_IP' -> target
	self.cookies = {}
	self.conn_count = 0

	if self.debug and self.debug.sched then
		local function pass(target, rc, ...)
			self:dp(target, '', ('<'):rep(1+rc)..('-'):rep(30-rc))
			return ...
		end
		override(self, 'request', function(inherited, self, t, ...)
			local rc = t.redirect_count or 0
			local target = self:target(t)
			self:dp(target, '', ('>'):rep(1+rc)..('-'):rep(30-rc))
			return pass(target, rc, inherited(self, t, ...))
		end)
	else
		self.dp = noop
	end

	return self
end

--global fetch ---------------------------------------------------------------

local cl
function fetch(...)
	cl = cl or http_client{
		max_conn               = config'fetch_max_conn',
		max_conn_per_target    = config'fetch_max_conn_per_target',
		max_pipelined_requests = config'fetch_max_pipelined_requests',
		client_ips             = config'fetch_client_ips',
		max_redirects          = config'fetch_max_redirects',
	}
	return cl:fetch(...)
end
