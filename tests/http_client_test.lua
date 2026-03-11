require'glue'
require'http_client'
logging.verbose = true
logging.debug = true
--config('getpage_debug', 'protocol')
--config('getpage_debug', 'protocol stream')
--logging.filter.tls = true

function test_getpage(url, n)
	local b = 0
	for i=1,n do
		resume(thread(function()
			local s, err = getpage{url = url}
			b = b + (s and #s or 0)
			printf('%-10s %s', s and kbytes(#s) or err, url)
			assert(s:has'GET / HTTP/1.1')
			assert(s:has'X-Forwarded-Proto: https')
		end, 'P'..i))
	end
	local t0 = clock()
	start()
	t1 = clock()
	print(kbytes(b / (t1 - t0))..'/s')
end

test_getpage('https://echo.websocket.org/', 1)
