--[=[

	Path manipulation for UNIX paths.
	Written by Cosmin Apreutesei. Public Domain.

	basename(s) -> s|nil                   get the last component of a path
	dirname(s[, levels=1]) -> s|nil        get the path without last component(s)
	path_nameext(s) -> name|nil, ext|nil   split `basename(s)` into name and extension
	path_ext(s) -> s|nil                   return only the extension from `nameext(s)`
	indir(dir, ...) -> path                combine path with one or more sub-paths
	path_normalize(s, [opt]) -> s          normalize a path in various ways
	path_commonpath(p1, p2) -> p|nil       common path prefix between two paths
	relpath(s, pwd) -> s|nil               convert absolute path to relative

]=]

if not ... then require'path_test'; return end

require'glue'
local
	typeof, select, assertf, tostring, add, remove, concat =
	typeof, select, assertf, tostring, add, remove, concat

--Get the last path component of a path. Returns '' for 'a/'.
function basename(s)
	return s:match'[^/]*$'
end

local function remove_dots(s)
	local s0
	s = s:gsub('//+', '/')   -- // -> /
	repeat  -- a/././b -> a/./b -> a/b
		s0 = s
		s = s:gsub('/%./', '/')
	until s == s0
	s = s:gsub('^%./', '')   -- ./b -> b
	s = s:gsub('/%.$', '')   -- a/. -> a
	return s
end

--Get a path without the last component and slash.
--Returns nil for '', '/' and '.'; returns '.' for simple filenames.
--Semantics chosen so that recursive dirname() always ends in nil.
--NOTE: paths with '.' components circumvent the semantics of "going up"
--so we remove those before computing dirname.
function dirname(s, levels)
	s = remove_dots(s)
	levels = levels or 1
	while levels > 0 do
		if s == '' or s == '/' or s == '.' then return nil end
		local s = s:match'^(.*)/' or '.' -- /a/b -> /a; /a -> ''; a -> .
		if s == '' then return '/' end -- /a -> /
		levels = levels - 1
	end
	return s
end

--Split a path's last component into the name and extension parts like so:
--   a.txt    -> a        , txt
--   .bashrc  -> .bashrc  , nil
--   a        -> a        , nil
--   a.       -> a        , ''
function path_nameext(s)
	local patt = '^(.-)%.([^%./]*)$'
	local file = basename(s)
	if not file then
		return nil, nil
	end
	local name, ext = file:match(patt)
	if not name or name == '' then -- 'dir' or '.bashrc'
		return file, nil
	end
	return name, ext
end

function path_ext(s)
	return (select(2, path_nameext(s)))
end

--Make a path by combining dir with one or more relative sub-paths.
--Being extra-careful here because usage bugs can wipe out the drive.
function indir(dir, s, ...)
	local t = typeof(dir)
	assertf(t == 'string' or t == 'number', 'indir: invalid dir arg type: %s', t)
	dir = tostring(dir)
	local t = typeof(s)
	assertf(t == 'number' or t == 'string', 'indir: invalid file arg type: %s', t)
	s = tostring(s)
	assertf(not s:starts'/', 'indir: invalid file: %s', s) --appending abs path
	assertf(dir ~= '', 'indir: empty dir')
	assertf(s ~= '', 'indir: empty file')
	local s = dir:gsub('/*$', '/') .. s
	return select('#', ...) > 0 and indir(s, ...) or s
end

--Iterate a path's components. For absolute paths, the first element is ''.
--Empty elements are skipped.
function path_split(s)
	local next_pc = s:gmatch'([^/]+)/*'
	local started = not s:match'^/+'
	return function()
		if not started then
			started = true
			return ''
		else
			return next_pc()
		end
	end
end

--[[
Normalize a path. opt can contain:

 * dot_dirs       : keep `.` dirs (false).
 * dot_dot_dirs   : keep unnecessary `..` dirs (true).
   WARNING: removing `..` breaks the path if there are symlinks involved!
 * endsep (false) : true to add, false to remove, 'leave' to skip.

If normalization results in the empty relative path '', then '.' is returned.
]]
function path_normalize(s, opt)
	opt = opt or {}
	local t = {} --{dir1, sep1, ...}
	local lastsep --last separator that was not added to the list
	for s in path_split(s) do
		if s == '.' and not opt.dot_dirs then
			--skip adding the `.` dir and the separator following it
			lastsep = '/'
		elseif s == '..' and opt.dot_dot_dirs == false and #t > 0 then
			--find the last dir past any `.` dirs, in case opt.dot_dirs = true.
			local i = #t-1
			while t[i] == '.' do
				i = i - 2
			end
			--remove the last dir (and the separator following it)
			--that's not `..` and it's not the root element.
			if i > 0 and ((i > 1 or t[i] ~= '') and t[i] ~= '..') then
				remove(t, i)
				remove(t, i)
				lastsep = '/'
			elseif #t == 2 and t[1] == '' then
				--skip any `..` after the root slash
				lastsep = '/'
			else
				add(t, s)
				add(t, '/')
			end
		else
			add(t, s)
			add(t, '/')
			lastsep = nil
		end
	end
	if not s:starts'/' and #t == 0 then
		--rel path '' is invalid. fix that.
		add(t, '.')
		add(t, lastsep)
	elseif lastsep == '' and (#t > 2 or t[1] ~= '') then
		--if there was no end separator originally before removing path
		--components, remove the left over end separator now.
		remove(t)
	end
	local s = concat(t)
	if opt.endsep ~= 'leave' then
		if opt.endsep == true then
			s = s..'/'
		elseif opt.endsep == nil or opt.endsep == false then
			s = s:gsub('/+$', '')
		end
	end
	return s
end

---number of non-empty path components, excluding prefixes.
local function path_depth(p)
	local n = 0
	for _ in p:gmatch'()[^/]+' do -- () prevents creating a string that we don't need
		n = n + 1
	end
	return n
end

--Get the common path prefix of two paths, including the end separator if both
--paths share it, or nil if the paths don't have anything in common.
function path_commonpath(p1, p2)
	local p = #p1 <= #p2 and p1 or p2
	if p == '' then return nil end
	local sep = ('/'):byte(1)
	local si = 0 --index where the last common separator was found
	for i = 1, #p + 1 do --going 1 byte beyond the end where we "see" a sep
		local c1 = p1:byte(i)
		local c2 = p2:byte(i)
		local sep1 = c1 == nil or c1 == sep
		local sep2 = c2 == nil or c2 == sep
		if sep1 and sep2 then
			si = i
		elseif c1 ~= c2 then
			break
		end
	end
	if si == 0 then return nil end
	return p:sub(1, si)
end

--Convert a path (abs or rel) into a rel path that is relative to pwd.
--Returns nil if the paths don't have a base path in common. The ending slash
--is preserved if present.
function relpath(s, pwd)
	local prefix = path_commonpath(s, pwd)
	if not prefix then return nil end
	local endsep = s:match'/*$'
	local pwd_suffix = pwd:sub(#prefix + 1)
	local n = path_depth(pwd_suffix)
	local p1 = ('../'):rep(n - 1) .. (n > 0 and '..' or '')
	local p2 = s:sub(#prefix + 1)
	local p2 = p2:gsub('^/+', '')
	local p2 = p2:gsub('/+$', '')
	local p2 = p1 == '' and p2 == '' and '.' or p2
	return p1 .. (p1 ~= '' and p2 ~= '' and '/' or '') .. p2 .. endsep
end
