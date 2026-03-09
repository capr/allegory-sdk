--[=[

	XML parsing based on expat.
	Written by Cosmin Apreutesei. Public Domain.

xml_parse(source, callbacks) -> true

	Parse a XML from a string, cdata, file, or reader function, calling
	a callback for each piece of the XML parsed.

	The optional `namespacesep` field is a single-character string. If present,
	it causes XML namespaces to be resolved during parsing. Namespace URLs are
	then concatenated to tag names using the specified character.

	source = {path = S} | {string = S} | {cdata = CDATA, size = N} | {read = f} & {[namespacesep=S]}

	callbacks = {
	  element         = function(name, model) end,
	  attr_list       = function(elem, name, type, dflt, is_required) end,
	  xml             = function(version, encoding, standalone) end,
	  entity          = function(name, is_param_entity, val, base, sysid, pubid, notation) end,
	  start_tag       = function(name, attrs) end,
	  end_tag         = function(name) end,
	  cdata           = function(s) end,
	  pi              = function(target, data) end,
	  comment         = function(s) end,
	  start_cdata     = function() end,
	  end_cdata       = function() end,
	  default         = function(s) end,
	  default_expand  = function(s) end,
	  start_doctype   = function(name, sysid, pubid, has_internal_subset) end,
	  end_doctype     = function() end,
	  unparsed        = function(name, base, sysid, pubid, notation) end,
	  notation        = function(name, base, sysid, pubid) end,
	  start_namespace = function(prefix, uri) end,
	  end_namespace   = function(prefix) end,
	  not_standalone  = function() end,
	  ref             = function(parser, context, base, sysid, pubid) end,
	  skipped         = function(name, is_parameter_entity) end,
	  unknown         = function(name, info) end,
	}

xml_parse(source, [known_tags]) -> root_node

	Parse a XML to a tree of nodes. known_tags filters the output so that only
	the tags that known_tags indexes are returned.

	Nodes look like this:

		node = {tag=, attrs={<k>=v}, children={node1,...},
				tags={<tag> = node}, cdata=, parent=}

xml_children(node, tag) -> iter() -> node

	Iterate a node's children that have a specific tag.

]=]

if not ... then require'xml_parse_test'; return end

require'glue'
local C = ffi.load'expat'

--result of cpp expat.h from expat 2.1.0
cdef[[
typedef char XML_Char;
typedef char XML_LChar;
typedef long XML_Index;
typedef unsigned long XML_Size;
typedef unsigned char XML_Bool;

struct XML_ParserStruct;
typedef struct XML_ParserStruct *XML_Parser;

enum XML_Status {
  XML_STATUS_ERROR = 0,
  XML_STATUS_OK = 1,
  XML_STATUS_SUSPENDED = 2
};

enum XML_Error {
  XML_ERROR_NONE,
  XML_ERROR_NO_MEMORY,
  XML_ERROR_SYNTAX,
  XML_ERROR_NO_ELEMENTS,
  XML_ERROR_INVALID_TOKEN,
  XML_ERROR_UNCLOSED_TOKEN,
  XML_ERROR_PARTIAL_CHAR,
  XML_ERROR_TAG_MISMATCH,
  XML_ERROR_DUPLICATE_ATTRIBUTE,
  XML_ERROR_JUNK_AFTER_DOC_ELEMENT,
  XML_ERROR_PARAM_ENTITY_REF,
  XML_ERROR_UNDEFINED_ENTITY,
  XML_ERROR_RECURSIVE_ENTITY_REF,
  XML_ERROR_ASYNC_ENTITY,
  XML_ERROR_BAD_CHAR_REF,
  XML_ERROR_BINARY_ENTITY_REF,
  XML_ERROR_ATTRIBUTE_EXTERNAL_ENTITY_REF,
  XML_ERROR_MISPLACED_XML_PI,
  XML_ERROR_UNKNOWN_ENCODING,
  XML_ERROR_INCORRECT_ENCODING,
  XML_ERROR_UNCLOSED_CDATA_SECTION,
  XML_ERROR_EXTERNAL_ENTITY_HANDLING,
  XML_ERROR_NOT_STANDALONE,
  XML_ERROR_UNEXPECTED_STATE,
  XML_ERROR_ENTITY_DECLARED_IN_PE,
  XML_ERROR_FEATURE_REQUIRES_XML_DTD,
  XML_ERROR_CANT_CHANGE_FEATURE_ONCE_PARSING,
  XML_ERROR_UNBOUND_PREFIX,
  XML_ERROR_UNDECLARING_PREFIX,
  XML_ERROR_INCOMPLETE_PE,
  XML_ERROR_XML_DECL,
  XML_ERROR_TEXT_DECL,
  XML_ERROR_PUBLICID,
  XML_ERROR_SUSPENDED,
  XML_ERROR_NOT_SUSPENDED,
  XML_ERROR_ABORTED,
  XML_ERROR_FINISHED,
  XML_ERROR_SUSPEND_PE,
  XML_ERROR_RESERVED_PREFIX_XML,
  XML_ERROR_RESERVED_PREFIX_XMLNS,
  XML_ERROR_RESERVED_NAMESPACE_URI
};

enum XML_Content_Type {
  XML_CTYPE_EMPTY = 1,
  XML_CTYPE_ANY,
  XML_CTYPE_MIXED,
  XML_CTYPE_NAME,
  XML_CTYPE_CHOICE,
  XML_CTYPE_SEQ
};

enum XML_Content_Quant {
  XML_CQUANT_NONE,
  XML_CQUANT_OPT,
  XML_CQUANT_REP,
  XML_CQUANT_PLUS
};

typedef struct XML_cp XML_Content;

struct XML_cp {
  enum XML_Content_Type type;
  enum XML_Content_Quant quant;
  XML_Char * name;
  unsigned int numchildren;
  XML_Content * children;
};

typedef void (*XML_ElementDeclHandler) (void *userData, const XML_Char *name, XML_Content *model);

void XML_SetElementDeclHandler(XML_Parser parser, XML_ElementDeclHandler eldecl);

typedef void (*XML_AttlistDeclHandler) (
                                    void *userData,
                                    const XML_Char *elname,
                                    const XML_Char *attname,
                                    const XML_Char *att_type,
                                    const XML_Char *dflt,
                                    int isrequired);

void XML_SetAttlistDeclHandler(XML_Parser parser, XML_AttlistDeclHandler attdecl);

typedef void (*XML_XmlDeclHandler) (void *userData, const XML_Char *version, const XML_Char *encoding, int standalone);

void XML_SetXmlDeclHandler(XML_Parser parser, XML_XmlDeclHandler xmldecl);

typedef struct {
  void *(*malloc_fcn)(size_t size);
  void *(*realloc_fcn)(void *ptr, size_t size);
  void (*free_fcn)(void *ptr);
} XML_Memory_Handling_Suite;

XML_Parser XML_ParserCreate(const XML_Char *encoding);
XML_Parser XML_ParserCreateNS(const XML_Char *encoding, XML_Char namespaceSeparator);
XML_Parser XML_ParserCreate_MM(const XML_Char *encoding, const XML_Memory_Handling_Suite *memsuite,
											const XML_Char *namespaceSeparator);
XML_Bool XML_ParserReset(XML_Parser parser, const XML_Char *encoding);

typedef void (*XML_StartElementHandler) (void *userData, const XML_Char *name, const XML_Char **atts);

typedef void (*XML_EndElementHandler) (void *userData, const XML_Char *name);

typedef void (*XML_CharacterDataHandler) (void *userData, const XML_Char *s, int len);

typedef void (*XML_ProcessingInstructionHandler) (void *userData, const XML_Char *target, const XML_Char *data);

typedef void (*XML_CommentHandler) (void *userData, const XML_Char *data);

typedef void (*XML_StartCdataSectionHandler) (void *userData);

typedef void (*XML_EndCdataSectionHandler) (void *userData);

typedef void (*XML_DefaultHandler) (void *userData, const XML_Char *s, int len);

typedef void (*XML_StartDoctypeDeclHandler) (
                                            void *userData,
                                            const XML_Char *doctypeName,
                                            const XML_Char *sysid,
                                            const XML_Char *pubid,
                                            int has_internal_subset);

typedef void (*XML_EndDoctypeDeclHandler)(void *userData);

typedef void (*XML_EntityDeclHandler) (
                              void *userData,
                              const XML_Char *entityName,
                              int is_parameter_entity,
                              const XML_Char *value,
                              int value_length,
                              const XML_Char *base,
                              const XML_Char *systemId,
                              const XML_Char *publicId,
                              const XML_Char *notationName);

void XML_SetEntityDeclHandler(XML_Parser parser, XML_EntityDeclHandler handler);

typedef void (*XML_UnparsedEntityDeclHandler) (
                                    void *userData,
                                    const XML_Char *entityName,
                                    const XML_Char *base,
                                    const XML_Char *systemId,
                                    const XML_Char *publicId,
                                    const XML_Char *notationName);

typedef void (*XML_NotationDeclHandler) (
                                    void *userData,
                                    const XML_Char *notationName,
                                    const XML_Char *base,
                                    const XML_Char *systemId,
                                    const XML_Char *publicId);

typedef void (*XML_StartNamespaceDeclHandler) (
                                    void *userData,
                                    const XML_Char *prefix,
                                    const XML_Char *uri);

typedef void (*XML_EndNamespaceDeclHandler) (
                                    void *userData,
                                    const XML_Char *prefix);

typedef int (*XML_NotStandaloneHandler) (void *userData);

typedef int (*XML_ExternalEntityRefHandler) (
                                    XML_Parser parser,
                                    const XML_Char *context,
                                    const XML_Char *base,
                                    const XML_Char *systemId,
                                    const XML_Char *publicId);

typedef void (*XML_SkippedEntityHandler) (
                                    void *userData,
                                    const XML_Char *entityName,
                                    int is_parameter_entity);

typedef struct {
  int map[256];
  void *data;
  int (*convert)(void *data, const char *s);
  void (*release)(void *data);
} XML_Encoding;

typedef int (*XML_UnknownEncodingHandler) (void *encodingHandlerData, const XML_Char *name, XML_Encoding *info);

void XML_SetElementHandler(XML_Parser parser, XML_StartElementHandler start, XML_EndElementHandler end);
void XML_SetStartElementHandler(XML_Parser parser, XML_StartElementHandler handler);
void XML_SetEndElementHandler(XML_Parser parser, XML_EndElementHandler handler);
void XML_SetCharacterDataHandler(XML_Parser parser, XML_CharacterDataHandler handler);
void XML_SetProcessingInstructionHandler(XML_Parser parser, XML_ProcessingInstructionHandler handler);
void XML_SetCommentHandler(XML_Parser parser, XML_CommentHandler handler);
void XML_SetCdataSectionHandler(XML_Parser parser, XML_StartCdataSectionHandler start, XML_EndCdataSectionHandler end);
void XML_SetStartCdataSectionHandler(XML_Parser parser, XML_StartCdataSectionHandler start);
void XML_SetEndCdataSectionHandler(XML_Parser parser, XML_EndCdataSectionHandler end);
void XML_SetDefaultHandler(XML_Parser parser, XML_DefaultHandler handler);
void XML_SetDefaultHandlerExpand(XML_Parser parser, XML_DefaultHandler handler);
void XML_SetDoctypeDeclHandler(XML_Parser parser, XML_StartDoctypeDeclHandler start, XML_EndDoctypeDeclHandler end);
void XML_SetStartDoctypeDeclHandler(XML_Parser parser, XML_StartDoctypeDeclHandler start);
void XML_SetEndDoctypeDeclHandler(XML_Parser parser, XML_EndDoctypeDeclHandler end);
void XML_SetUnparsedEntityDeclHandler(XML_Parser parser, XML_UnparsedEntityDeclHandler handler);
void XML_SetNotationDeclHandler(XML_Parser parser, XML_NotationDeclHandler handler);
void XML_SetNamespaceDeclHandler(XML_Parser parser, XML_StartNamespaceDeclHandler start, XML_EndNamespaceDeclHandler end);
void XML_SetStartNamespaceDeclHandler(XML_Parser parser, XML_StartNamespaceDeclHandler start);
void XML_SetEndNamespaceDeclHandler(XML_Parser parser, XML_EndNamespaceDeclHandler end);
void XML_SetNotStandaloneHandler(XML_Parser parser, XML_NotStandaloneHandler handler);
void XML_SetExternalEntityRefHandler(XML_Parser parser, XML_ExternalEntityRefHandler handler);
void XML_SetExternalEntityRefHandlerArg(XML_Parser parser, void *arg);
void XML_SetSkippedEntityHandler(XML_Parser parser, XML_SkippedEntityHandler handler);
void XML_SetUnknownEncodingHandler(XML_Parser parser, XML_UnknownEncodingHandler handler, void *encodingHandlerData);
void XML_DefaultCurrent(XML_Parser parser);
void XML_SetReturnNSTriplet(XML_Parser parser, int do_nst);
void XML_SetUserData(XML_Parser parser, void *userData);
enum XML_Status XML_SetEncoding(XML_Parser parser, const XML_Char *encoding);
void XML_UseParserAsHandlerArg(XML_Parser parser);
enum XML_Error XML_UseForeignDTD(XML_Parser parser, XML_Bool useDTD);
enum XML_Status XML_SetBase(XML_Parser parser, const XML_Char *base);
const XML_Char * XML_GetBase(XML_Parser parser);
int XML_GetSpecifiedAttributeCount(XML_Parser parser);
int XML_GetIdAttributeIndex(XML_Parser parser);
enum XML_Status XML_Parse(XML_Parser parser, const char *s, int len, int isFinal);
void* XML_GetBuffer(XML_Parser parser, int len);
enum XML_Status XML_ParseBuffer(XML_Parser parser, int len, int isFinal);
enum XML_Status XML_StopParser(XML_Parser parser, XML_Bool resumable);
enum XML_Status XML_ResumeParser(XML_Parser parser);

enum XML_Parsing {
  XML_INITIALIZED,
  XML_PARSING,
  XML_FINISHED,
  XML_SUSPENDED
};

typedef struct {
  enum XML_Parsing parsing;
  XML_Bool finalBuffer;
} XML_ParsingStatus;

void XML_GetParsingStatus(XML_Parser parser, XML_ParsingStatus *status);

XML_Parser XML_ExternalEntityParserCreate(XML_Parser parser, const XML_Char *context, const XML_Char *encoding);

enum XML_ParamEntityParsing {
  XML_PARAM_ENTITY_PARSING_NEVER,
  XML_PARAM_ENTITY_PARSING_UNLESS_STANDALONE,
  XML_PARAM_ENTITY_PARSING_ALWAYS
};

int XML_SetParamEntityParsing(XML_Parser parser, enum XML_ParamEntityParsing parsing);
int XML_SetHashSalt(XML_Parser parser, unsigned long hash_salt);
enum XML_Error XML_GetErrorCode(XML_Parser parser);
XML_Size XML_GetCurrentLineNumber(XML_Parser parser);
XML_Size XML_GetCurrentColumnNumber(XML_Parser parser);
XML_Index XML_GetCurrentByteIndex(XML_Parser parser);
int XML_GetCurrentByteCount(XML_Parser parser);
const char * XML_GetInputContext(XML_Parser parser, int *offset, int *size);
void XML_FreeContentModel(XML_Parser parser, XML_Content *model);
void * XML_MemMalloc(XML_Parser parser, size_t size);
void * XML_MemRealloc(XML_Parser parser, void *ptr, size_t size);
void XML_MemFree(XML_Parser parser, void *ptr);
void XML_ParserFree(XML_Parser parser);
const XML_LChar* XML_ErrorString(enum XML_Error code);
const XML_LChar* XML_ExpatVersion(void);

typedef struct {
  int major;
  int minor;
  int micro;
} XML_Expat_Version;

XML_Expat_Version XML_ExpatVersionInfo(void);

enum XML_FeatureEnum {
  XML_FEATURE_END = 0,
  XML_FEATURE_UNICODE,
  XML_FEATURE_UNICODE_WCHAR_T,
  XML_FEATURE_DTD,
  XML_FEATURE_CONTEXT_BYTES,
  XML_FEATURE_MIN_SIZE,
  XML_FEATURE_SIZEOF_XML_CHAR,
  XML_FEATURE_SIZEOF_XML_LCHAR,
  XML_FEATURE_NS,
  XML_FEATURE_LARGE_SIZE,
  XML_FEATURE_ATTR_INFO
};

typedef struct {
  enum XML_FeatureEnum feature;
  const XML_LChar *name;
  long int value;
} XML_Feature;

const XML_Feature* XML_GetFeatureList(void);
]]

local
	str =
	str

local cbsetters = {
	'element',        C.XML_SetElementDeclHandler,            ctype'XML_ElementDeclHandler',
	'attlist',        C.XML_SetAttlistDeclHandler,            ctype'XML_AttlistDeclHandler',
	'xml',            C.XML_SetXmlDeclHandler,                ctype'XML_XmlDeclHandler',
	'entity',         C.XML_SetEntityDeclHandler,             ctype'XML_EntityDeclHandler',
	'start_tag',      C.XML_SetStartElementHandler,           ctype'XML_StartElementHandler',
	'end_tag',        C.XML_SetEndElementHandler,             ctype'XML_EndElementHandler',
	'cdata',          C.XML_SetCharacterDataHandler,          ctype'XML_CharacterDataHandler',
	'pi',             C.XML_SetProcessingInstructionHandler,  ctype'XML_ProcessingInstructionHandler',
	'comment',        C.XML_SetCommentHandler,                ctype'XML_CommentHandler',
	'start_cdata',    C.XML_SetStartCdataSectionHandler,      ctype'XML_StartCdataSectionHandler',
	'end_cdata',      C.XML_SetEndCdataSectionHandler,        ctype'XML_EndCdataSectionHandler',
	'default',        C.XML_SetDefaultHandler,                ctype'XML_DefaultHandler',
	'default_expand', C.XML_SetDefaultHandlerExpand,          ctype'XML_DefaultHandler',
	'start_doctype',  C.XML_SetStartDoctypeDeclHandler,       ctype'XML_StartDoctypeDeclHandler',
	'end_doctype',    C.XML_SetEndDoctypeDeclHandler,         ctype'XML_EndDoctypeDeclHandler',
	'unparsed',       C.XML_SetUnparsedEntityDeclHandler,     ctype'XML_UnparsedEntityDeclHandler',
	'notation',       C.XML_SetNotationDeclHandler,           ctype'XML_NotationDeclHandler',
	'start_namespace',C.XML_SetStartNamespaceDeclHandler,     ctype'XML_StartNamespaceDeclHandler',
	'end_namespace',  C.XML_SetEndNamespaceDeclHandler,       ctype'XML_EndNamespaceDeclHandler',
	'not_standalone', C.XML_SetNotStandaloneHandler,          ctype'XML_NotStandaloneHandler',
	'ref',            C.XML_SetExternalEntityRefHandler,      ctype'XML_ExternalEntityRefHandler',
	'skipped',        C.XML_SetSkippedEntityHandler,          ctype'XML_SkippedEntityHandler',
}

local NULL = new'void*'

local function decode_attrs(attrs) --char** {k1,v1,...,NULL}
	local t = {}
	local i = 0
	while true do
		local k = str(attrs[i]);   if not k then break end
		local v = str(attrs[i+1]); if not v then break end
		t[k] = v
		i = i + 2
	end
	return t
end

local pass_nothing = function(_) end
local cbdecoders = {
	element = function(_, name, model) return str(name), model end,
	attr_list = function(_, elem, name, type, dflt, is_required)
		return str(elem), str(name), str(type), str(dflt), is_required ~= 0
	end,
	xml = function(_, version, encoding, standalone)
		return str(version), str(encoding), standalone ~= 0
	end,
	entity = function(_, name, is_param_entity, val, val_len, base, sysid, pubid, notation)
		return str(name), is_param_entity ~= 0, str(val, val_len), str(base),
					str(sysid), str(pubid), str(notation)
	end,
	start_tag = function(_, name, attrs) return str(name), decode_attrs(attrs) end,
	end_tag = function(_, name) return str(name) end,
	cdata = function(_, s, len) return str(s, len) end,
	pi = function(_, target, data) return str(target), str(data) end,
	comment = function(_, s) return str(s) end,
	start_cdata = pass_nothing,
	end_cdata = pass_nothing,
	default = function(_, s, len) return str(s, len) end,
	default_expand = function(_, s, len) return str(s, len) end,
	start_doctype = function(_, name, sysid, pubid, has_internal_subset)
		return str(name), str(sysid), str(pubid), has_internal_subset ~= 0
	end,
	end_doctype = pass_nothing,
	unparsed = function(name, base, sysid, pubid, notation)
		return str(name), str(base), str(sysid), str(pubid), str(notation)
	end,
	notation = function(_, name, base, sysid, pubid)
		return str(name), str(base), str(sysid), str(pubid)
	end,
	start_namespace = function(_, prefix, uri) return str(prefix), str(uri) end,
	end_namespace = function(_, prefix) return str(prefix) end,
	not_standalone = pass_nothing,
	ref = function(parser, context, base, sysid, pubid)
		return parser, str(context), str(base), str(sysid), str(pubid)
	end,
	skipped = function(_, name, is_parameter_entity) return str(name), is_parameter_entity ~= 0 end,
	unknown = function(_, name, info) return str(name), info end,
}

local parser = {}

function parser.read(read, callbacks, options)
	local cbt = {}
	local function cb(cbtype, callback, decode)
		local cb = cast(cbtype, function(...) return callback(decode(...)) end)
		cbt[#cbt+1] = cb
		return cb
	end
	local function free_callbacks()
		for _,cb in ipairs(cbt) do
			cb:free()
		end
	end
	return fpcall(function(finally)
		finally(free_callbacks)

		local parser = options.namespacesep and C.XML_ParserCreateNS(options.encoding, options.namespacesep:byte())
				or C.XML_ParserCreate(options.encoding)
		finally(function() C.XML_ParserFree(parser) end)

		for i=1,#cbsetters,3 do
			local k, setter, cbtype = cbsetters[i], cbsetters[i+1], cbsetters[i+2]
			if callbacks[k] then
				setter(parser, cb(cbtype, callbacks[k], cbdecoders[k]))
			elseif k == 'entity' then
				setter(parser, cb(cbtype,
						function(parser) C.XML_StopParser(parser, false) end,
						function(parser) return parser end))
			end
		end
		if callbacks.unknown then
			C.XML_SetUnknownEncodingHandler(parser,
				cb('XML_UnknownEncodingHandler', callbacks.unknown, cbdecoders.unknown), nil)
		end

		C.XML_SetUserData(parser, parser)

		repeat
			local data, size, more = read()
			if C.XML_Parse(parser, data, size, more and 0 or 1) == 0 then
				error(format('XML parser error at line %d, col %d: "%s"',
						tonumber(C.XML_GetCurrentLineNumber(parser)),
						tonumber(C.XML_GetCurrentColumnNumber(parser)),
						str(C.XML_ErrorString(C.XML_GetErrorCode(parser)))))
			end
		until not more
	end)
end

function parser.path(file, callbacks, options)
	return fpcall(function(finally)
		local f = assert(io.open(file, 'rb'))
		finally(function() f:close() end)
		local function read()
			local s = f:read(16384)
			if s then
				return s, #s, true
			else
				return nil, 0
			end
		end
		parser.read(read, callbacks, options)
	end)
end

function parser.string(s, callbacks, options)
	local function read()
		return s, #s
	end
	return parser.read(read, callbacks, options)
end

function parser.cdata(cdata, callbacks, options)
	local function read()
		return cdata, options.size
	end
	return parser.read(read, callbacks, options)
end

local function maketree_callbacks(known_tags)
	local root = {tag = 'root', attrs = {}, children = {}, tags = {}}
	local t = root
	local skip
	return {
		cdata = function(s)
			t.cdata = s
		end,
		start_tag = function(s, attrs)
			if skip then skip = skip + 1; return end
			if known_tags and not known_tags[s] then skip = 1; return end

			t = {tag = s, attrs = attrs, children = {}, tags = {}, parent = t}
			local ct = t.parent.children
			ct[#ct+1] = t
			t.parent.tags[t.tag] = t
		end,
		end_tag = function(s)
			if skip then
				skip = skip - 1
				if skip == 0 then skip = nil end
				return
			end

			t = t.parent
		end,
	}, root
end

function xml_parse(t, callbacks)
	local root = true
	if not isfunc(callbacks) then
		local known_tags = callbacks
		callbacks, root = maketree_callbacks(known_tags)
	end
	for k,v in pairs(t) do
		if parser[k] then
			local ok, err = parser[k](v, callbacks, t)
			if not ok then return nil, err end
			return root
		end
	end
	error'source missing'
end

function xml_children(t,tag) --iterate a node's children of a specific tag
	local i=1
	return function()
		local v
		repeat
			v = t.children[i]
			i = i + 1
		until not v or v.tag == tag
		return v
	end
end
