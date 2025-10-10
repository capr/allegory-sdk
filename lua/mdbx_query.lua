--[[


	get('val_col1 ...', key_val1, ...) -> val_col1_val, ...
	list()

	atomic(['w', ]f, ...) -> f(tx, ...)

]]

require'mdbx_schema'

function dbname(ns)
	return pconfig(ns, 'db_name')
		or pconfig(ns, 'db_name', scriptname..(ns and '_'..ns or ''))
end

do
local _db
function db()
	if not _db then
		assert(config('db_engine', 'mdbx') == 'mdbx')
		_db = mdbx_open(indir(config('db_dir', vardir()), config('db_name', scriptname)..'.mdbx'))
		_db.schema = schema
	end
	return _db
end
end

do
local function abort(tx, ...)
	tx:abort()
	return ...
end
function get(...)
	local tx = db():tx()
	abort(tx, tx:get(...))
end
end

do
local function finish(tx, ok, ...)
	if ok then
		tx:commit()
		return ...
	else
		tx:abort()
		error(...)
	end
end
function atomic(mode, f, ...)
	if isfunc(mode) then mode, f = 'r', mode end
	local tx = db():tx(mode)
	return finish(tx, xpcall(f, traceback, tx, ...))
end
end
