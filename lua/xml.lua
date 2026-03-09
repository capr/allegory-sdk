--[=[

	XML formatting.
	Written by Cosmin Apreutesei. Public Domain.

FEATURES
	* does all necessary XML escaping.
	* prevents generating text that isn't well-formed.
	* generates namespace prefixes.
	* produces Canonical XML, suitable for use with digital signatures.

LIMITATIONS
	* only UTF8 encoding supported
	* no empty element tags
	* no <!DOCTYPE> declarations (write it yourself before calling w:start_doc())
	* no pretty-printing (add linebreaks and indentation yourself with w:text())

API
	xml() -> w                                Create a new genx writer.
	w:free()                                  Free the genx writer.
	w:start_doc(file)                         Start an XML document on a Lua file object
	w:start_doc(write)                        Start an XML document on write([s[, size]])
	w:end_doc()                               Flush pending updates and release the file handle
	w:ns(uri[, prefix]) -> ns                 Declare a namespace for reuse.
	w:tag(name[, ns | uri,prefix]) -> elem    Declare an element for reuse.
	w:attr(name[, ns | uri,prefix]) -> attr   Declare an attribute for reuse.
	w:comment(s)                              Add a comment to the current XML stream.
	w:pi(target, text)                        Add a PI to the current XML stream.
	w:start_tag(elem | name [, ns | uri,prefix])   Start a new XML element.
	w:end_tag()                               End the current element.
	w:add_attr(attr, val[, ns | uri,prefix])  Add an attribute to the current element.
																Attributes are sorted by name in the output stream.
	w:add_ns(ns | [uri,prefix])               Add a namespace to the current element.
	w:unset_default_namespace()               Add a `xmlns=""` declaration to unset the default namespace declaration.
																This is a no-op if no default namespace is in effect.
	w:text(s[, size])                         Add utf-8 text.
	w:char(char)                              Add an unicode code point.
	w:check_text(s) -> genxStatus             Check utf-8 text.
	w:scrub_text(s) -> s                      Scrub utf-8 text of invalid characters.

]=]

if not ... then require'xml_test'; return end

local ffi = require'ffi'
local C = ffi.load'genx'

--result of cpp genx.h from genx beta5
ffi.cdef[[
typedef struct FILE FILE;

typedef enum
{
  GENX_SUCCESS = 0,
  GENX_BAD_UTF8,
  GENX_NON_XML_CHARACTER,
  GENX_BAD_NAME,
  GENX_ALLOC_FAILED,
  GENX_BAD_NAMESPACE_NAME,
  GENX_INTERNAL_ERROR,
  GENX_DUPLICATE_PREFIX,
  GENX_SEQUENCE_ERROR,
  GENX_NO_START_TAG,
  GENX_IO_ERROR,
  GENX_MISSING_VALUE,
  GENX_MALFORMED_COMMENT,
  GENX_XML_PI_TARGET,
  GENX_MALFORMED_PI,
  GENX_DUPLICATE_ATTRIBUTE,
  GENX_ATTRIBUTE_IN_DEFAULT_NAMESPACE,
  GENX_DUPLICATE_NAMESPACE,
  GENX_BAD_DEFAULT_DECLARATION
} genxStatus;

typedef unsigned char * utf8;
typedef const unsigned char * constUtf8;

typedef struct genxWriter_rec_ genxWriter_rec, * genxWriter;
typedef struct genxNamespace_rec_ genxNamespace_rec, * genxNamespace;
typedef struct genxElement_rec_ genxElement_rec, * genxElement;
typedef struct genxAttribute_rec_ genxAttribute_rec, * genxAttribute;

genxWriter genxNew(
		void * (* alloc)(void * userData, int bytes),
		void (* dealloc)(void * userData, void * data),
		void * userData);
void genxDispose(genxWriter w);

void genxSetUserData(genxWriter w, void * userData);
void * genxGetUserData(genxWriter w);
void genxSetAlloc(genxWriter w, void * (* alloc)(void * userData, int bytes));
void genxSetDealloc(genxWriter w, void (* dealloc)(void * userData, void * data));
void * (* genxGetAlloc(genxWriter w))(void * userData, int bytes);
void (* genxGetDealloc(genxWriter w))(void * userData, void * data);

utf8 genxGetNamespacePrefix(genxNamespace ns);

genxNamespace genxDeclareNamespace(genxWriter w,
       constUtf8 uri, constUtf8 prefix,
       genxStatus * statusP);

genxElement genxDeclareElement(genxWriter w,
          genxNamespace ns, constUtf8 type,
          genxStatus * statusP);

genxAttribute genxDeclareAttribute(genxWriter w,
       genxNamespace ns,
       constUtf8 name, genxStatus * statusP);

genxStatus genxStartDocFile(genxWriter w, FILE * file);

typedef genxStatus (* send_callback)(void * userData, constUtf8 s);
typedef genxStatus (* sendBounded_callback)(void * userData, constUtf8 start, constUtf8 end);
typedef genxStatus (* flush_callback)(void * userData);

typedef struct {
	send_callback        send;
	sendBounded_callback sendBounded;
	flush_callback       flush;
} genxSender;

genxStatus genxStartDocSender(genxWriter w, genxSender * sender);
genxStatus genxEndDocument(genxWriter w);
genxStatus genxComment(genxWriter w, constUtf8 text);
genxStatus genxPI(genxWriter w, constUtf8 target, constUtf8 text);
genxStatus genxStartElementLiteral(genxWriter w, constUtf8 xmlns, constUtf8 type);
genxStatus genxStartElement(genxElement e);
genxStatus genxAddAttributeLiteral(genxWriter w, constUtf8 xmlns, constUtf8 name, constUtf8 value);
genxStatus genxAddAttribute(genxAttribute a, constUtf8 value);
genxStatus genxAddNamespace(genxNamespace ns, constUtf8 prefix); // NOTE: prefix changed from utf8 to constUtf8
genxStatus genxUnsetDefaultNamespace(genxWriter w);
genxStatus genxEndElement(genxWriter w);
genxStatus genxAddText(genxWriter w, constUtf8 start);
genxStatus genxAddCountedText(genxWriter w, constUtf8 start, int byteCount);
genxStatus genxAddBoundedText(genxWriter w, constUtf8 start, constUtf8 end);
genxStatus genxAddCharacter(genxWriter w, int c);
int genxNextUnicodeChar(constUtf8 * sp);
genxStatus genxCheckText(genxWriter w, constUtf8 s);
int genxCharClass(genxWriter w, int c);
int genxScrubText(genxWriter w, constUtf8 in, utf8 out);
char * genxGetErrorMessage(genxWriter w, genxStatus status);
char * genxLastErrorMessage(genxWriter w);
char * genxGetVersion();
]]

local function checkh(w, statusP, h)
	if h ~= nil then return h end
	local s = C.genxGetErrorMessage(w, statusP[0])
	error(s ~= nil and ffi.string(s) or 'unknown error')
end

local function checknz(w, status)
	if status == 0 then return end
	local s = C.genxGetErrorMessage(w, status)
	error(s ~= nil and ffi.string(s) or 'unknown error')
end

local function nzcaller(f)
	return function(w, ...)
		return checknz(w, f(w, ...))
	end
end

function xml(alloc, dealloc, userdata)
	local w = C.genxNew(alloc, dealloc, userdata)
	assert(w ~= nil, 'out of memory')
	ffi.gc(w, C.genxDispose)
	return w
end

local senders = {} --{[genxWriter] = genxSender}

local function free_sender(w)
	local sender = senders[w]
	if not sender then return end
	sender.send:free()
	sender.sendBounded:free()
	sender.flush:free()
	senders[w] = nil
end

local function free(w)
	C.genxDispose(ffi.gc(w, nil))
	free_sender(w)
end

ffi.metatype('genxWriter_rec', {__index = {

	free = free,

	start_doc = function(w, f, ...)
		free_sender(w)
		if type(f) == 'function' then
			--f is called as either: f(s), f(s, sz), or f() to signal EOF.
			local sender = ffi.new'genxSender'
			sender.send = ffi.new('send_callback', function(_, s) f(s); return 0 end)
			sender.sendBounded = ffi.new('sendBounded_callback', function(_, p1, p2) f(p1, p2-p1); return 0 end)
			sender.flush = ffi.new('flush_callback', function() f(); return 0 end)
			senders[w] = sender
			checknz(w, C.genxStartDocSender(w, sender))
		else
			checknz(w, C.genxStartDocFile(w, f))
		end
	end,

	end_doc = nzcaller(C.genxEndDocument),

	ns = function(w, uri, prefix, statusP)
		statusP = statusP or ffi.new'genxStatus[1]'
		return checkh(w, statusP, C.genxDeclareNamespace(w, uri, prefix, statusP))
	end,

	tag = function(w, name, ns, statusP)
		statusP = statusP or ffi.new'genxStatus[1]'
		return checkh(w, statusP, C.genxDeclareElement(w, ns, name, statusP))
	end,

	attr = function(w, name, ns, statusP)
		statusP = statusP or ffi.new'genxStatus[1]'
		return checkh(w, statusP, C.genxDeclareAttribute(w, ns, name, statusP))
	end,

	comment = nzcaller(C.genxComment),

	pi = nzcaller(C.genxPI),

	start_tag = function(w, e, ns, prefix)
		if type(ns) == 'string' then
			ns = w:ns(ns, prefix)
		end
		if type(e) == 'string' then
			e = w:tag(e, ns)
		end
		checknz(w, C.genxStartElement(e))
	end,

	add_attr = function(w, a, val, ns, prefix)
		if type(ns) == 'string' then
			ns = w:ns(ns, prefix)
		end
		if type(a) == 'string' then
			a = w:attr(a, ns)
		end
		checknz(w, C.genxAddAttribute(a, val))
	end,

	add_ns = function(w, ns, prefix)
		if type(ns) == 'string' then
			ns = w:ns(ns, prefix)
		end
		checknz(w, C.genxAddNamespace(ns, prefix))
	end,

	unset_default_namespace = nzcaller(C.genxUnsetDefaultNamespace),

	end_tag = nzcaller(C.genxEndElement),

	text = function(w, s, sz)
		checknz(w, C.genxAddCountedText(w, s, sz or #s))
	end,

	char = nzcaller(C.genxAddCharacter),

	check_text = C.genxCheckText,

	scrub_text = function(s_in)
		s_out = ffi.new('constUtf8[?]', #s_in + 1)
		if C.genxScrubText(s_in, s_out) ~= 0 then
			return ffi.string(s_out)
		else
			return s_in
		end
	end,

}, __gc = free})
