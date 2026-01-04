--go@ plink -t root@m1 sdk/bin/debian12/luajit sdk/tests/mdbx_schema_test.lua

logging.debug = true

local function iscfunc(v)
	if typeof(v) ~= 'cdata' then return false end
	local ti = ffi.typeinfo(tonumber(ctype(v)))
	local CT_code = shr(ti.info, 28)
	return CT_code == 6
end

local prf = {}
local prf_after = {}
local addrs = {}
local dbi_names = {}
local function faddr(typ, p)
	local s = tostring(p):sub(-12)
	local t = attr(addrs, typ)
	local i = t[s]
	if not i then t.i = (t.i or 0) + 1; i = t.i; t[s] = i; end
	return i
end
local function ftxn(txn)
	return 'txn:'..faddr('txn', txn)
end
local function fdbi(dbi)
	return 'dbi:'..dbi..':'..(dbi_names[dbi] or '?')
end
local function fcur(cur)
	return 'cur:'..faddr('cur', cur)
end
local function fflags(flags)
	flags = flags or 0
	return flags==0 and '' or 'flags='..tohex(flags)
end
local function fbin(v)
	if not v then return 'nil' end
	return faddr('v', v.data)..':'..num(v.size)..' '..((str(v.data, v.size) or ''):gsub('[%z\1-\31]', '.'))
end
local function fk(k) return 'k:'..fbin(k) end
local function fv(v) return 'v:'..fbin(v) end
function prf_after.mdbx_txn_begin_ex(fn, rc, env, parent, flags, txnp)
	local txn = txnp[0]
	pr('TX-BEG', rc, ftxn(txn), parent and 'p'..ftxn(parent) or '', fflags(flags))
end
function prf_after.mdbx_txn_commit_ex(fn, rc, txn)
	pr('TX-COM', rc, ftxn(txn))
end
function prf_after.mdbx_txn_abort(fn, rc, txn)
	pr('TX-ABO', rc, ftxn(txn))
end
function prf_after.mdbx_txn_reset(fn, rc, txn)
	pr('TX-RST', rc, ftxn(txn))
end
function prf_after.mdbx_dbi_open(fn, rc, txn, name, flags, dbip)
	local dbi = dbip[0]
	if rc == 0 then
		dbi_names[dbi] = name or '<main>'
	end
	pr(' OPEN', rc, ftxn(txn), name, fdbi(dbi), fflags(flags))
end
function prf_after.mdbx_dbi_close(fn, rc, env, dbi)
	pr(' CLOSE', rc, fdbi(dbi))
	if rc == 0 then dbi_names[dbi] = nil end
end
function prf_after.mdbx_put(fn, rc, txn, dbi, k, v, flags)
	pr('  put', rc, ftxn(txn), fdbi(dbi), fk(k), fv(v), fflags(flags))
end
function prf_after.mdbx_get(fn, rc, txn, dbi, k, v)
	pr('  get', rc, ftxn(txn), fdbi(dbi), fk(k), rc == 0 and fv(v) or '')
end
function prf_after.mdbx_del(fn, rc, txn, dbi, k, v)
	pr('  del', rc, ftxn(txn), fdbi(dbi), fk(k), fv(v))
end
function prf_after.mdbx_cursor_open(fn, rc, txn, dbi, curp)
	pr(' Copen', rc, ftxn(txn), fdbi(dbi), fcur(curp[0]))
end
function prf_after.mdbx_cursor_get(fn, rc, cur, k, v, op)
	pr('  Cget', rc, fcur(cur), fk(k), rc == 0 and fv(v) or '', fflags(op))
end
function prf_after.mdbx_cursor_unbind(fn, rc, cur)
	pr(' Cunb', rc, fcur(cur))
end
function prf.mdbx_env_get_maxkeysize_ex() end
local _C = mdbx
mdbx = setmetatable({}, {__index = function(t, k)
	local v = _C[k]
	if iscfunc(v) then
		t[k] = function(...)
			local prf = prf[k]
			local prf_after = prf_after[k]
			if not prf and not prf_after then prf = pr end
			if prf then prf(k, ...) end
			local rc = v(...)
			if prf_after then prf_after(k, rc, ...) end
			return rc
		end
		return t[k]
	else
		t[k] = v
		return v
	end
end
})
