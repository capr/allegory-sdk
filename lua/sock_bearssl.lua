--[=[

	Secure async TCP sockets based on BearSSL (push-pull engine API).
	Written by Cosmin Apreutesei. Public Domain.

API

	client_stcp(tcp, servername, opt) -> cstcp   create a secure client socket
	server_stcp(tcp, opt) -> sstcp               create a secure server socket
	cstcp:[try_]recv(buf, sz) -> n               receive decrypted bytes
	cstcp:[try_]send(buf, sz) -> true            send bytes (encrypted)
	cstcp:[try_]close()                          close with SSL shutdown
	sstcp:[try_]accept() -> cstcp                accept a TLS client connection
	cstcp:[try_]shutdown('r'|'w'|'rw')           shutdown underlying TCP

Config options (opt table)

	ca                     CA certificate PEM data (string) for server verification
	cert                   certificate PEM data (for server or mutual TLS)
	key                    private key PEM data (for server or mutual TLS)
	cert_issuer_rsa        hint: server EC cert was issued by RSA CA (default: EC)
	insecure_noverifycert  skip server certificate verification (client only)

]=]

require'glue'
require'sock'
require'bearssl_ssl_h'

local C = ffi.load(bearssl_libname or 'bearssl')

ffi.cdef[[
typedef struct {
	struct { uint32_t *dp; uint32_t *rp; const unsigned char *ip; } cpu;
	uint32_t dp_stack[32];
	uint32_t rp_stack[32];
	int err;
	const unsigned char *hbuf;
	size_t hlen;
	void (*dest)(void *dest_ctx, const void *src, size_t len);
	void *dest_ctx;
	unsigned char event;
	char name[128];
	unsigned char buf[255];
	size_t ptr;
} br_pem_decoder_context;
void br_pem_decoder_init(br_pem_decoder_context *ctx);
size_t br_pem_decoder_push(br_pem_decoder_context *ctx,
	const void *data, size_t len);
int br_pem_decoder_event(br_pem_decoder_context *ctx);
]]

local BR_PEM_BEGIN_OBJ = 1
local BR_PEM_END_OBJ   = 2
local BR_PEM_ERROR     = 3

local BR_SSL_CLOSED  = 0x0001
local BR_SSL_SENDREC = 0x0002
local BR_SSL_RECVREC = 0x0004
local BR_SSL_SENDAPP = 0x0008
local BR_SSL_RECVAPP = 0x0010

local BR_KEYTYPE_RSA  = 1
local BR_KEYTYPE_EC   = 2
local BR_KEYTYPE_KEYX = 0x10
local BR_KEYTYPE_SIGN = 0x20
local BR_X509_TA_CA   = 0x0001

local BR_SSL_BUFSIZE_BIDI = (16384 + 325) + (16384 + 85)

local min = math.min

--PEM parsing ----------------------------------------------------------------

local _pem_acc     -- uint8_t* write head
local _pem_acc_pos -- current fill count
local _pem_maxsz   -- allocated size

local _pem_dest_cb = cast('void(*)(void*, const void*, size_t)',
	function(ctx, src, len)
		len = tonumber(len)
		if _pem_acc_pos + len > _pem_maxsz then return end
		copy(_pem_acc + _pem_acc_pos, src, len)
		_pem_acc_pos = _pem_acc_pos + len
	end)

local _dn_acc = new('uint8_t[4096]') -- reused DN accumulator
local _dn_pos = 0

local _dn_cb = cast('void(*)(void*, const void*, size_t)',
	function(ctx, src, len)
		len = tonumber(len)
		if _dn_pos + len > 4096 then return end
		copy(_dn_acc + _dn_pos, src, len)
		_dn_pos = _dn_pos + len
	end)

-- Parse PEM string; returns list of {name=, data=uint8_t[], len=n}.
local function parse_pem(s)
	local data_len = #s
	local acc = new('uint8_t[?]', data_len)
	local pdc = new('br_pem_decoder_context')
	C.br_pem_decoder_init(pdc)
	local sp = cast('const uint8_t*', s)
	local i = 0
	local objs = {}
	local cur_name

	_pem_acc     = acc
	_pem_maxsz   = data_len
	_pem_acc_pos = 0

	while i < data_len do
		local consumed = tonumber(C.br_pem_decoder_push(pdc, sp + i, data_len - i))
		i = i + consumed
		local ev = C.br_pem_decoder_event(pdc)
		if ev == BR_PEM_BEGIN_OBJ then
			cur_name     = ffi.string(pdc.name)
			_pem_acc_pos = 0
			pdc.dest     = _pem_dest_cb
			pdc.dest_ctx = nil
		elseif ev == BR_PEM_END_OBJ then
			if cur_name then
				local n    = _pem_acc_pos
				local data = new('uint8_t[?]', n)
				copy(data, acc, n)
				objs[#objs + 1] = {name = cur_name, data = data, len = n}
				cur_name = nil
				pdc.dest = nil
			end
		elseif ev == BR_PEM_ERROR then
			return nil, 'pem_decode_error'
		end
	end
	return objs
end

--Key struct helpers ---------------------------------------------------------

-- Copy br_rsa_private_key fields into a new [1] array (pointer-compatible).
-- The actual key bytes still live in skdc.key_data; skdc must stay alive.
local function copy_rsa_sk(src, keepalive)
	local sk = new('br_rsa_private_key[1]')
	sk[0].n_bitlen = src.n_bitlen
	sk[0].p     = src.p;  sk[0].plen  = src.plen
	sk[0].q     = src.q;  sk[0].qlen  = src.qlen
	sk[0].dp    = src.dp; sk[0].dplen = src.dplen
	sk[0].dq    = src.dq; sk[0].dqlen = src.dqlen
	sk[0].iq    = src.iq; sk[0].iqlen = src.iqlen
	keepalive[#keepalive + 1] = sk
	return sk
end

local function copy_ec_sk(src, keepalive)
	local sk = new('br_ec_private_key[1]')
	sk[0].curve = src.curve
	sk[0].x     = src.x; sk[0].xlen = src.xlen
	keepalive[#keepalive + 1] = sk
	return sk
end

--Trust anchors --------------------------------------------------------------

-- Build br_x509_trust_anchor[] from CA PEM.
-- Returns (ta_array, ta_count, keepalive) or (nil, err).
local function load_trust_anchors(ca_pem)
	local objs, err = parse_pem(ca_pem)
	if not objs then return nil, err end

	local certs = {}
	for _, o in ipairs(objs) do
		if o.name == 'CERTIFICATE' then certs[#certs + 1] = o end
	end
	if #certs == 0 then return nil, 'no_certificates_in_ca' end

	local n        = #certs
	local ta_array = new('br_x509_trust_anchor[?]', n)
	local keepalive = {ta_array}

	for i, cert in ipairs(certs) do
		local dc = new('br_x509_decoder_context')
		_dn_pos = 0
		C.br_x509_decoder_init(dc, _dn_cb, nil)
		C.br_x509_decoder_push(dc, cert.data, cert.len)

		if tonumber(dc.err) ~= 0 or tonumber(dc.decoded) == 0 then
			return nil, 'cert_decode_error_'..tonumber(dc.err)
		end

		local ta   = ta_array[i - 1]
		local pkey = dc.pkey
		local kt   = tonumber(pkey.key_type)

		-- copy DN into a persistent buffer
		local dn_len  = _dn_pos
		local dn_copy = new('uint8_t[?]', dn_len)
		copy(dn_copy, _dn_acc, dn_len)
		keepalive[#keepalive + 1] = dn_copy
		ta.dn.data = dn_copy
		ta.dn.len  = dn_len
		ta.flags   = BR_X509_TA_CA

		if kt == BR_KEYTYPE_RSA then
			local rsa  = pkey.key.rsa
			local nlen = tonumber(rsa.nlen)
			local elen = tonumber(rsa.elen)
			local nc   = new('uint8_t[?]', nlen)
			local ec_e = new('uint8_t[?]', elen)
			copy(nc, rsa.n, nlen)
			copy(ec_e, rsa.e, elen)
			keepalive[#keepalive + 1] = nc
			keepalive[#keepalive + 1] = ec_e
			ta.pkey.key_type      = BR_KEYTYPE_RSA
			ta.pkey.key.rsa.n     = nc;   ta.pkey.key.rsa.nlen = nlen
			ta.pkey.key.rsa.e     = ec_e; ta.pkey.key.rsa.elen = elen
		elseif kt == BR_KEYTYPE_EC then
			local ec   = pkey.key.ec
			local qlen = tonumber(ec.qlen)
			local qc   = new('uint8_t[?]', qlen)
			copy(qc, ec.q, qlen)
			keepalive[#keepalive + 1] = qc
			ta.pkey.key_type     = BR_KEYTYPE_EC
			ta.pkey.key.ec.curve = tonumber(ec.curve)
			ta.pkey.key.ec.q     = qc; ta.pkey.key.ec.qlen = qlen
		else
			return nil, 'unsupported_ca_key_type_'..kt
		end
	end

	return ta_array, n, keepalive
end

--Certificate chain ----------------------------------------------------------

local function load_cert_chain(cert_pem)
	local objs, err = parse_pem(cert_pem)
	if not objs then return nil, err end
	local certs = {}
	for _, o in ipairs(objs) do
		if o.name == 'CERTIFICATE' then certs[#certs + 1] = o end
	end
	if #certs == 0 then return nil, 'no_certificates_in_chain' end
	local n        = #certs
	local chain    = new('br_x509_certificate[?]', n)
	local keepalive = {chain}
	for i, cert in ipairs(certs) do
		chain[i - 1].data     = cert.data
		chain[i - 1].data_len = cert.len
		keepalive[#keepalive + 1] = cert.data
	end
	return chain, n, keepalive
end

--Private key ----------------------------------------------------------------

-- Returns (skdc, key_type, keepalive) or (nil, err).
-- skdc kept alive because key struct pointers reference skdc.key_data.
local function load_private_key(key_pem)
	local objs, err = parse_pem(key_pem)
	if not objs then return nil, err end
	local der
	for _, o in ipairs(objs) do
		if o.name == 'RSA PRIVATE KEY'
		or o.name == 'EC PRIVATE KEY'
		or o.name == 'PRIVATE KEY' then
			der = o; break
		end
	end
	if not der then return nil, 'no_private_key_in_pem' end
	local skdc = new('br_skey_decoder_context')
	C.br_skey_decoder_init(skdc)
	C.br_skey_decoder_push(skdc, der.data, der.len)
	if tonumber(skdc.err) ~= 0 then
		return nil, 'key_decode_error_'..tonumber(skdc.err)
	end
	local kt = tonumber(skdc.key_type)
	if kt == 0 then return nil, 'key_decode_incomplete' end
	return skdc, kt, {skdc, der.data}
end

--SSL context builders -------------------------------------------------------

-- Returns (sc, eng_ptr, keepalive) or (nil, err).
-- eng_ptr is br_ssl_engine_context* (== sc cast, since eng is first field).
local function make_client_ctx(opt)
	local keepalive = {}
	local sc  = new('br_ssl_client_context')
	local xc  = new('br_x509_minimal_context')
	local buf = new('uint8_t[?]', BR_SSL_BUFSIZE_BIDI)
	local eng = cast('br_ssl_engine_context*', sc) -- eng is first field
	keepalive[#keepalive + 1] = sc
	keepalive[#keepalive + 1] = xc
	keepalive[#keepalive + 1] = buf

	if not (opt and opt.insecure_noverifycert) then
		if not (opt and opt.ca) then return nil, 'ca_required' end
		local ta, ta_n, ta_kp = load_trust_anchors(opt.ca)
		if not ta then return nil, ta_n end
		for _, v in ipairs(ta_kp) do keepalive[#keepalive + 1] = v end
		C.br_ssl_client_init_full(sc, xc, ta, ta_n)
	else
		C.br_ssl_client_init_full(sc, xc, nil, 0)
	end

	C.br_ssl_engine_set_buffer(eng, buf, BR_SSL_BUFSIZE_BIDI, 1)

	if opt and opt.cert and opt.key then
		local chain, chain_n, chain_kp = load_cert_chain(opt.cert)
		if not chain then return nil, chain_n end
		for _, v in ipairs(chain_kp) do keepalive[#keepalive + 1] = v end

		local skdc, kt, key_kp = load_private_key(opt.key)
		if not skdc then return nil, kt end
		for _, v in ipairs(key_kp) do keepalive[#keepalive + 1] = v end

		if kt == BR_KEYTYPE_RSA then
			local sk = copy_rsa_sk(skdc.key.rsa, keepalive)
			C.br_ssl_client_set_single_rsa(sc, chain, chain_n,
				sk, C.br_rsa_pkcs1_sign_get_default())
		elseif kt == BR_KEYTYPE_EC then
			local sk = copy_ec_sk(skdc.key.ec, keepalive)
			C.br_ssl_client_set_single_ec(sc, chain, chain_n,
				sk, bor(BR_KEYTYPE_KEYX, BR_KEYTYPE_SIGN), 0,
				C.br_ec_get_default(), C.br_ecdsa_sign_asn1_get_default())
		end
	end

	return sc, eng, keepalive
end

local function make_server_ctx(opt)
	if not opt then return nil, 'opt_required' end
	if not opt.cert then return nil, 'cert_required' end
	if not opt.key  then return nil, 'key_required'  end

	local keepalive = {}
	local sc  = new('br_ssl_server_context')
	local buf = new('uint8_t[?]', BR_SSL_BUFSIZE_BIDI)
	local eng = cast('br_ssl_engine_context*', sc) -- eng is first field
	keepalive[#keepalive + 1] = sc
	keepalive[#keepalive + 1] = buf

	local chain, chain_n, chain_kp = load_cert_chain(opt.cert)
	if not chain then return nil, chain_n end
	for _, v in ipairs(chain_kp) do keepalive[#keepalive + 1] = v end

	local skdc, kt, key_kp = load_private_key(opt.key)
	if not skdc then return nil, kt end
	for _, v in ipairs(key_kp) do keepalive[#keepalive + 1] = v end

	if kt == BR_KEYTYPE_RSA then
		local sk = copy_rsa_sk(skdc.key.rsa, keepalive)
		C.br_ssl_server_init_full_rsa(sc, chain, chain_n, sk)
	elseif kt == BR_KEYTYPE_EC then
		local sk        = copy_ec_sk(skdc.key.ec, keepalive)
		local issuer_kt = opt.cert_issuer_rsa and BR_KEYTYPE_RSA or BR_KEYTYPE_EC
		C.br_ssl_server_init_full_ec(sc, chain, chain_n, issuer_kt, sk)
	else
		return nil, 'unsupported_key_type_'..kt
	end

	C.br_ssl_engine_set_buffer(eng, buf, BR_SSL_BUFSIZE_BIDI, 1)
	return sc, eng, keepalive
end

--Push-pull engine driver ----------------------------------------------------

local _szp = new('size_t[1]') -- shared; safe because reads are captured before yields

-- Drive the engine until `target` state bits are set.
-- Returns true, or nil+err on error/closed.
local function engine_run(self, target)
	local eng = self.eng
	local tcp = self.tcp
	while true do
		local state = tonumber(C.br_ssl_engine_current_state(eng))
		if band(state, target) ~= 0 then
			return true
		end
		if band(state, BR_SSL_CLOSED) ~= 0 then
			local e = tonumber(eng.err)
			return nil, e == 0 and 'eof' or 'ssl_error_'..e
		end
		if band(state, BR_SSL_SENDREC) ~= 0 then
			local buf = C.br_ssl_engine_sendrec_buf(eng, _szp)
			local n   = tonumber(_szp[0])
			local ok, serr = tcp:try_send(buf, n)
			if not ok then return nil, serr end
			C.br_ssl_engine_sendrec_ack(eng, n)
		elseif band(state, BR_SSL_RECVREC) ~= 0 then
			local buf = C.br_ssl_engine_recvrec_buf(eng, _szp)
			local n   = tonumber(_szp[0])
			local len, rerr = tcp:try_recv(buf, n)
			if not len then return nil, rerr end
			C.br_ssl_engine_recvrec_ack(eng, len)
		else
			return nil, 'ssl_stall'
		end
	end
end

--Socket classes -------------------------------------------------------------

local stcp = {
	issocket     = true,
	istcpsocket  = true,
	istlssocket  = true,
	debug_prefix = 'X',
}

local client_stcp = merge({type = 'client_tls_socket'}, tcp_class)
local server_stcp = merge({type = 'server_tls_socket'}, tcp_class)

-- client methods (connected TLS socket, both sides)

function client_stcp:try_recv(buf, sz)
	if self._closed then return nil, 'closed' end
	local ok, err = engine_run(self, BR_SSL_RECVAPP)
	if not ok then return nil, err end
	local app_buf = C.br_ssl_engine_recvapp_buf(self.eng, _szp)
	local n = min(sz, tonumber(_szp[0]))
	copy(buf, app_buf, n)
	C.br_ssl_engine_recvapp_ack(self.eng, n)
	return n
end

function client_stcp:try_send(buf, sz)
	if self._closed then return nil, 'closed' end
	sz = sz or #buf
	local bp   = cast('const uint8_t*', buf)
	local sent = 0
	while sent < sz do
		local ok, err = engine_run(self, BR_SSL_SENDAPP)
		if not ok then return nil, err end
		local app_buf = C.br_ssl_engine_sendapp_buf(self.eng, _szp)
		local n = min(sz - sent, tonumber(_szp[0]))
		copy(app_buf, bp + sent, n)
		C.br_ssl_engine_sendapp_ack(self.eng, n)
		C.br_ssl_engine_flush(self.eng, 0)
		sent = sent + n
	end
	-- drain encrypted output to TCP; ignore EOF (data already accepted)
	local ok, err = engine_run(self, bor(BR_SSL_SENDAPP, BR_SSL_RECVAPP))
	if not ok and err ~= 'eof' then return nil, err end
	return true
end

function client_stcp:try_close()
	if self._closed then return true end
	self._closed = true
	C.br_ssl_engine_close(self.eng)
	-- best-effort TLS close_notify
	while true do
		local state = tonumber(C.br_ssl_engine_current_state(self.eng))
		if band(state, BR_SSL_CLOSED) ~= 0 then break end
		if band(state, BR_SSL_SENDREC) ~= 0 then
			local buf = C.br_ssl_engine_sendrec_buf(self.eng, _szp)
			local n   = tonumber(_szp[0])
			if not self.tcp:try_send(buf, n) then break end
			C.br_ssl_engine_sendrec_ack(self.eng, n)
		elseif band(state, BR_SSL_RECVREC) ~= 0 then
			local buf = C.br_ssl_engine_recvrec_buf(self.eng, _szp)
			local n   = tonumber(_szp[0])
			local len = self.tcp:try_recv(buf, n)
			if not len then break end
			C.br_ssl_engine_recvrec_ack(self.eng, len)
		else
			break
		end
	end
	live(self, nil)
	local ok, err = self.tcp:try_close()
	self.tcp        = nil
	self.eng        = nil
	self._keepalive = nil
	return ok, err
end

-- server (listening) socket -- no TLS context, just wraps the TCP listener

function server_stcp:try_close()
	if self._closed then return true end
	self._closed = true
	live(self, nil)
	local ok, err = self.tcp:try_close()
	self.tcp = nil
	return ok, err
end

-- shared stcp methods

function stcp:onclose(fn)
	if self._closed then return end
	after(self.tcp, '_after_close', fn)
end

function stcp:closed()
	return self._closed or false
end

function stcp:try_shutdown(mode)
	return self.tcp:try_shutdown(mode)
end

-- object constructors

local function wrap_conn_stcp(tcp, eng, keepalive)
	local s = object(client_stcp, {
		tcp        = tcp,
		eng        = eng,
		s          = tcp.s,
		_keepalive = keepalive,
		check_io   = check_io,
		checkp     = checkp,
		r = 0, w = 0,
	})
	live(s, client_stcp.type)
	return s
end

--Public API -----------------------------------------------------------------

function _G.client_stcp(tcp, servername, opt)
	local sc, eng, keepalive = make_client_ctx(opt)
	if not sc then return nil, eng end

	if C.br_ssl_client_reset(sc, servername, 0) == 0 then
		return nil, 'ssl_client_reset_failed'
	end

	local s = wrap_conn_stcp(tcp, eng, keepalive)
	local ok, err = engine_run(s, bor(BR_SSL_SENDAPP, BR_SSL_RECVAPP))
	if not ok then
		s:try_close()
		return nil, err
	end
	return s
end

function _G.server_stcp(tcp, opt)
	local s = object(server_stcp, {
		tcp      = tcp,
		s        = tcp.s,
		_opt     = opt,
		check_io = check_io,
		checkp   = checkp,
		r = 0, w = 0,
	})
	live(s, server_stcp.type)
	return s
end

function server_stcp:try_accept()
	local ctcp, err = self.tcp:try_accept()
	if not ctcp then return nil, err end

	local sc, eng, keepalive = make_server_ctx(self._opt)
	if not sc then
		ctcp:try_close()
		return nil, eng
	end

	if C.br_ssl_server_reset(sc) == 0 then
		ctcp:try_close()
		return nil, 'ssl_server_reset_failed'
	end

	local cs = wrap_conn_stcp(ctcp, eng, keepalive)
	local ok, herr = engine_run(cs, bor(BR_SSL_SENDAPP, BR_SSL_RECVAPP))
	if not ok then
		cs:try_close()
		return nil, herr
	end
	return cs
end

update(client_stcp, stcp)
update(server_stcp, stcp)

-- apply unprotect_io after update() so we wrap the correct methods
client_stcp.close  = unprotect_io(client_stcp.try_close)
server_stcp.close  = unprotect_io(server_stcp.try_close)
stcp.shutdown      = unprotect_io(stcp.try_shutdown)
client_stcp.recv   = unprotect_io(client_stcp.try_recv)
client_stcp.send   = unprotect_io(client_stcp.try_send)
server_stcp.accept = unprotect_io(server_stcp.try_accept)

client_stcp.try_read  = client_stcp.try_recv
client_stcp.read      = client_stcp.recv
client_stcp.try_write = client_stcp.try_send
client_stcp.write     = client_stcp.send
