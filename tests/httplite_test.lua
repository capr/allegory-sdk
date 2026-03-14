--go@ plink d10 -t -batch sdk/bin/linux/luajit sdk/tests/httplite_test.lua
require'glue'
require'httplite'
logging.debug = true

if os.getenv'AUTO' then return end

local server = http_server{
	listen = {
		{
			host = 'localhost',
			--addr = '127.0.0.1',
			port = 80,
		},
		{
			host = 'localhost',
			--addr = '127.0.0.1',
			port = 443,
			tls = true,
			tls_options = {
				cert_file = exedir()..'/../tests/localhost.crt',
				key_file  = exedir()..'/../tests/localhost.key',
			},
		},
	},
	debug = {
		protocol = true,
		--stream = true,
		tracebacks = true,
		errors = true,
	},
	respond = function(req, thread)
		while true do
			local buf, sz = req:read_body_chunk()
			pr(buf, sz)
			if buf == nil and sz == 0 then break end --eof
			local s = str(buf, sz)
			print(s)
		end
		if req.uri == '/favicon.ico' then
			raise('http_response', {status = 404})
		end
		local out = req:out_function()
		--out(('hello '):rep(1000))
		--raise{status = 404, content = 'Dude, no page here'}
	end,
	--respond = webb_respond,
}

start()
server:stop()

