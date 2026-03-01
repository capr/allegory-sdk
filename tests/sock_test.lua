--go@ plink d10 -t -batch sdk/bin/linux/luajit sdk/tests/sock_test.lua

io.stdout:setvbuf'no'
io.stderr:setvbuf'no'

require'glue'
require'sock'

local function test_addr()
	local function dump(...)
		for ai in getaddrinfo(...):addrs() do
			pr(ai:tostring(), ai:type(), ai:family(), ai:protocol(), ai:name())
		end
	end
	dump('1234:2345:3456:4567:5678:6789:7890:8901', 0, 'tcp', 'inet6')
	dump('123.124.125.126', 1234, 'tcp', 'inet', nil, {canonname = true})
	dump('*', 0)
end

local function test_sockopt()
	local s = tcp()
	for _,k in ipairs{
		'so_type              ',
		'so_error             ',
		'so_reuseaddr         ',
	} do
		if k then
			local sk, k = k, trim(k)
			local v, err = s:try_getopt(k)
			pr('getopt', sk, v, err)
		end
	end

	pr''

	for _,k in ipairs{
		'so_reuseaddr          ',
		'so_broadcast          ',
		'so_sndbuf             ',
		'so_rcvbuf             ',
		'so_keepalive          ',
		'so_priority           ',
		'so_linger             ',
		'so_reuseport          ',
		'tcp_nodelay           ',
		'tcp_maxseg            ',
		'tcp_cork              ',
		'tcp_keepidle          ',
		'tcp_keepintvl         ',
		'tcp_keepcnt           ',
		'tcp_defer_accept      ',
		'tcp_quickack          ',
		'tcp_user_timeout      ',
		'tcp_fastopen          ',
		'tcp_fastopen_connect  ',
		'ip_tos                ',
		'ip_ttl                ',
		'ip_multicast_ttl      ',
		'ip_multicast_loop     ',
		'ip_freebind           ',
		'ipv6_v6only           ',
		'udp_cork              ',
	} do
		local sk, k = k, trim(k)
		local canget, v, err = pcall(s.try_getopt, s, k)
		if canget and v ~= nil then
			pr('setopt', k, s:try_setopt(k, v))
		end
	end
end

local function test_tcp()
	pr'tcp'
	run(function()
		local server = listen('127.0.0.1', 8091)
		resume(thread(function()
			local cs = server:accept()
			pr('accepted', cs,
				cs.remote_addr, cs.remote_port,
				cs. local_addr, cs. local_port)
			local buf = ffi.new'char[256]'
			local n = cs:recv(buf, 256)
			pr('server recv', n, ffi.string(buf, n))
			cs:close()
			server:close()
		end))
		resume(thread(function()
			local s = connect('127.0.0.1', 8091)
			pr('client connected', s)
			s:send'hello'
			s:close()
		end))
	end)
end

local function test_http()
	thread(function()
		local s = tcp()
		pr('connect', s:connect('127.0.0.1', 80))
		pr('send', s:send'GET / HTTP/1.0\r\n\r\n')
		local buf = ffi.new'char[4096]'
		local n, err, ec = s:recv(buf, 4096)
		if n then
			pr('recv', n, ffi.string(buf, n))
		else
			pr(n, err, ec)
		end
		s:close()
	end)
	pr('start', start())
end

local function test_timers()
	run(function()
		local i = 1
		local job = runevery(.1, function()
			pr(i); i = i + 1
		end)
		runafter(1, function()
			pr'canceling'
			job:cancel()
			pr'done'
		end)
	end)
end

test_timers()
test_addr()
test_sockopt()
test_tcp()
--test_http()
