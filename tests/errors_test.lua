require'glue'

local caught
local function test_errors()
	local e1 = errortype'e1'
	local e2 = errortype('e2', 'e1')
	local e3 = errortype'e3'
	local ok, e = catch('e2 e3', function()
		local ok, e = catch('e1', function()
			raise('e2', 'imma e2')
		end)
		print'should not get here'
	end)
	if not ok then
		caught = e
	end
	raise(e)
end
assert(not pcall(test_errors))
assert(caught.errortype == 'e2')
assert(caught.message == 'imma e2')

--newerror / iserror
local e = newerror('io', 'test error')
assert(iserror(e))
assert(iserror(e, 'io'))
assert(not iserror(e, 'protocol'))
assert(not iserror('string'))
assert(not iserror(nil))
assert(not iserror(42))

--protect
local f = protect(function() raise('io', 'fail') end)
local v, e = f()
assert(v == nil)
assert(iserror(e, 'io'))

local f = protect(function() return 42 end)
assert(f() == 42)
do --protect with oncaught callback
	local caught_err
	local f = protect(function() raise('io', 'boom') end, function(e) caught_err = e end)
	local v, e = f()
	assert(v == nil)
	assert(iserror(caught_err, 'io'))
end
do --protect with class filter
	local f = protect('io', function() raise('io', 'io fail') end)
	local v, e = f()
	assert(v == nil)
	assert(iserror(e, 'io'))
end
do --catch: class filter lets unmatched errors through
	local ok, err = pcall(function()
		catch('protocol', function() raise('io', 'io error') end)
	end)
	assert(not ok)
end
do --newerror: pass-through for existing error object
	local e = newerror('io', 'test')
	assert(newerror(e) == e)
end
do --errortype: get same class twice returns same object
	local e1 = errortype'_test_et'
	assert(errortype'_test_et' == e1)
end
do --errortype: inheritance
	local base = errortype'_test_base'
	local child = errortype('_test_child', '_test_base')
	local e = newerror('_test_child', 'msg')
	assert(iserror(e, '_test_child'))
end

pr'errors tests passed'
