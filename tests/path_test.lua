require'unit'
require'path'

--basename -------------------------------------------------------------------

local function test_basename(s, s2)
	local s1 = basename(s)
	print('basename', s, '->', s1)
	assert(s1 == s2, string.format('basename(%q) expected %q, got %q', s, s2, tostring(s1)))
end

-- basic
test_basename('a'         , 'a')
test_basename('a/b'       , 'b')
test_basename('/a'        , 'a')
test_basename('/a/b'      , 'b')
test_basename('/a/b/c'    , 'c')

-- trailing slash gives empty string
test_basename('a/'        , '')
test_basename('a/b/'      , '')
test_basename('/a/'       , '')

-- root and empty
test_basename('/'         , '')
test_basename(''          , '')

-- multiple slashes
test_basename('a//b'      , 'b')
test_basename('//a'       , 'a')
test_basename('a//'       , '')

-- dotfiles and dots
test_basename('.bashrc'   , '.bashrc')
test_basename('/a/.bashrc', '.bashrc')
test_basename('.'         , '.')
test_basename('..'        , '..')
test_basename('/.'        , '.')
test_basename('/..'       , '..')
test_basename('a/.'       , '.')
test_basename('a/..'      , '..')

-- long names
test_basename('a/b/c/d/e/f/g', 'g')

--dirname --------------------------------------------------------------------

local function test_dirname(s, s2)
	local s1 = dirname(s)
	print('dirname', s, '->', s1)
	assert(s1 == s2, string.format('dirname(%q) expected %s, got %s', s, tostring(s2), tostring(s1)))
end

-- returns nil for terminal cases
test_dirname(''   , nil)
test_dirname('.'  , nil)
test_dirname('/'  , nil)

-- dir is root
test_dirname('/a' , '/')
test_dirname('/aa', '/')

-- dir is current dir for bare filenames
test_dirname('a'  , '.')
test_dirname('aa' , '.')

-- trailing slash
test_dirname('a/' , 'a')
test_dirname('a/b/', 'a/b')

-- normal nested paths
test_dirname('a/b'   , 'a')
test_dirname('aa/bb' , 'aa')
test_dirname('/a/b'  , '/a')
test_dirname('/a/b/c', '/a/b')

-- dot components removed before computing dirname
test_dirname('./'     , nil)   -- remove_dots('./' -> '') -> nil
test_dirname('./a'    , '.')
test_dirname('a/./b'  , 'a')
test_dirname('/a/./b' , '/a')

-- consecutive dots (the fix from remove_dots looping)
test_dirname('a/././b'    , 'a')
test_dirname('a/./././b'  , 'a')
test_dirname('./././a'    , '.')
test_dirname('/./././a'   , '/')
test_dirname('a/././.'    , '.')   -- all dots collapse, bare 'a' remains

-- trailing dot
test_dirname('a/.'  , '.')
test_dirname('a/b/.', 'a')

-- recursive dirname converges to nil
local s = '/a/b/c'
local results = {}
while s do
	s = dirname(s)
	table.insert(results, s)
end
test(results, {'/a/b', '/a', '/'})

local s = 'a/b/c'
local results = {}
while s do
	s = dirname(s)
	table.insert(results, s)
end
test(results, {'a/b', 'a', '.'})

--path_nameext ---------------------------------------------------------------

local function test_nameext(s, name2, ext2)
	local name1, ext1 = path_nameext(s)
	print('nameext', s, '->', name1, ext1)
	assert(name1 == name2, string.format('path_nameext(%q) name expected %s, got %s', s, tostring(name2), tostring(name1)))
	assert(ext1 == ext2, string.format('path_nameext(%q) ext expected %s, got %s', s, tostring(ext2), tostring(ext1)))
end

-- normal files
test_nameext('a.txt'       , 'a'      , 'txt')
test_nameext('a.tar.gz'    , 'a.tar'  , 'gz')
test_nameext('/a/b/c.txt'  , 'c'      , 'txt')

-- no extension
test_nameext('a'           , 'a'      , nil)
test_nameext('/a/b/c'      , 'c'      , nil)

-- dotfile (leading dot, no ext)
test_nameext('.bashrc'     , '.bashrc', nil)
test_nameext('/a/.bashrc'  , '.bashrc', nil)
test_nameext('.gitignore'  , '.gitignore', nil)

-- dotfile with extension
test_nameext('.bash.conf'  , '.bash'  , 'conf')

-- trailing dot (empty extension)
test_nameext('a.'          , 'a'      , '')
test_nameext('/a/b/c.'     , 'c'      , '')

-- empty and root paths
test_nameext(''            , ''       , nil)
test_nameext('/'           , ''       , nil)
test_nameext('a/'          , ''       , nil)

-- multiple dots
test_nameext('a.b.c.d'     , 'a.b.c'  , 'd')

--path_ext -------------------------------------------------------------------

local function test_ext(s, ext2)
	local ext1 = path_ext(s)
	print('ext', s, '->', ext1)
	assert(ext1 == ext2, string.format('path_ext(%q) expected %s, got %s', s, tostring(ext2), tostring(ext1)))
end

test_ext('a.txt'   , 'txt')
test_ext('a.tar.gz', 'gz')
test_ext('a'       , nil)
test_ext('.bashrc' , nil)
test_ext('a.'      , '')
test_ext(''        , nil)
test_ext('/'       , nil)
test_ext('a/'      , nil)

--indir ----------------------------------------------------------------------

local function test_indir(expected, ...)
	local ok, result = pcall(indir, ...)
	if expected == nil then
		-- expecting an error
		print('indir', ..., '-> ERROR (expected)')
		assert(not ok, 'indir: expected error')
	else
		print('indir', ..., '->', result)
		assert(ok, 'indir: unexpected error: ' .. tostring(result))
		assert(result == expected, string.format('indir expected %q, got %q', expected, tostring(result)))
	end
end

-- basic
test_indir('/a/b'    , '/a', 'b')
test_indir('a/b'     , 'a', 'b')
test_indir('a/b/c'   , 'a', 'b', 'c')
test_indir('a/b/c/d' , 'a', 'b', 'c', 'd')

-- dir with trailing slash (no double slash)
test_indir('a/b'     , 'a/', 'b')
test_indir('a/b'     , 'a//', 'b')
test_indir('a/b'     , 'a///', 'b')

-- root dir
test_indir('/b'      , '/', 'b')
test_indir('/b/c'    , '/', 'b', 'c')

-- numeric args
test_indir('a/1'     , 'a', 1)
test_indir('1/b'     , 1, 'b')
test_indir('1/2'     , 1, 2)

-- errors: appending absolute path
test_indir(nil       , 'a', '/b')

-- errors: empty dir
test_indir(nil       , '', 'b')

-- errors: empty file
test_indir(nil       , 'a', '')

--path_split -----------------------------------------------------------------

local function test_split(s, t2)
	local t1 = {}
	for s in path_split(s) do
		table.insert(t1, s)
	end
	print('split', s, '->', table.concat(t1, ', '))
	test(t1, t2)
end

-- empty string
test_split(''        , {})

-- root
test_split('/'       , {''})

-- absolute paths (first element is '')
test_split('/a'      , {'', 'a'})
test_split('/a/b'    , {'', 'a', 'b'})
test_split('/a/b/c'  , {'', 'a', 'b', 'c'})

-- relative paths
test_split('a'       , {'a'})
test_split('a/b'     , {'a', 'b'})
test_split('a/b/c'   , {'a', 'b', 'c'})

-- trailing slash (skips empty element)
test_split('a/'      , {'a'})
test_split('/a/'     , {'', 'a'})

-- multiple slashes (empty elements skipped)
test_split('//a'     , {'', 'a'})
test_split('a//b'    , {'a', 'b'})

-- dot components
test_split('./a'     , {'.', 'a'})
test_split('a/./b'   , {'a', '.', 'b'})
test_split('../a'    , {'..', 'a'})

--path_normalize -------------------------------------------------------------

local function test_normalize(s, opt, s2)
	local s1 = path_normalize(s, opt)
	print('normalize', s, '->', s1)
	assert(s1 == s2, string.format('path_normalize(%q) expected %q, got %q', s, s2, s1))
end

-- remove dots (default)
test_normalize('.'         , nil, '.')
test_normalize('./'        , nil, '.')
test_normalize('./a'       , nil, 'a')
test_normalize('a/.'       , nil, 'a')
test_normalize('a/./b'     , nil, 'a/b')
test_normalize('/./a'      , nil, '/a')
test_normalize('a/./b/./c' , nil, 'a/b/c')

-- remove dots: root result needs endsep='leave' to preserve /
local leave = {endsep = 'leave'}
test_normalize('/.'        , leave, '/')
test_normalize('/./.'      , leave, '/')

-- keep dots with dot_dirs = true
local keep_dots = {dot_dirs = true}
test_normalize('a/./b'     , keep_dots, 'a/./b')
test_normalize('./a'       , keep_dots, './a')

-- remove dotdot (default: dot_dot_dirs removes them)
test_normalize('a/b/..'    , nil, 'a')
test_normalize('a/b/c/..'  , nil, 'a/b')
test_normalize('a/..'      , nil, '.')
test_normalize('/a/b/..'   , nil, '/a')

-- remove dotdot: root result needs endsep='leave'
test_normalize('/a/..'     , leave, '/')
test_normalize('/a/b/../..' , leave, '/')

-- dotdot that can't be resolved (goes above cwd)
test_normalize('..'         , nil, '..')
test_normalize('../a'       , nil, '../a')
test_normalize('../../a'    , nil, '../../a')
test_normalize('a/../..'    , nil, '..')

-- dotdot on root (can't go above /), use endsep='leave'
test_normalize('/..'        , leave, '/')
test_normalize('/../a'      , nil, '/a')
test_normalize('/../../a'   , nil, '/a')

-- keep dotdot with dot_dot_dirs = false
local keep_dotdot = {dot_dot_dirs = false}
test_normalize('a/b/..'     , keep_dotdot, 'a/b/..')
test_normalize('a/..'       , keep_dotdot, 'a/..')

-- endsep options
test_normalize('a/b'  , {endsep = true}   , 'a/b/')
test_normalize('a/b/' , {endsep = true}   , 'a/b/')
test_normalize('a/b/' , {endsep = false}  , 'a/b')
test_normalize('a/b'  , {endsep = false}  , 'a/b')
test_normalize('a/b/' , {endsep = 'leave'}, 'a/b/')
test_normalize('a/b'  , {endsep = 'leave'}, 'a/b')

-- endsep=false strips root / (known behavior)
test_normalize('/'    , nil  , '')
test_normalize('/'    , leave, '/')

-- combined dot + dotdot removal
test_normalize('a/./b/../c'     , nil, 'a/c')
test_normalize('a/b/./../../c'  , nil, 'c')
test_normalize('/a/./b/../c'    , nil, '/a/c')

-- multiple slashes collapsed
test_normalize('a//b'            , nil, 'a/b')
test_normalize('/a//b///c'       , nil, '/a/b/c')

-- empty relative path normalizes to '.'
test_normalize('a/..'            , nil, '.')

-- complex paths
test_normalize('a/b/c/../../d'   , nil, 'a/d')
test_normalize('/a/b/c/../../d'  , nil, '/a/d')
test_normalize('../a/b/../c'     , nil, '../a/c')

--path_commonpath ------------------------------------------------------------

local function test_commonpath(p1, p2, expected)
	local result = path_commonpath(p1, p2)
	print('commonpath', p1, p2, '->', result)
	assert(result == expected, string.format(
		'path_commonpath(%q, %q) expected %s, got %s',
		p1, p2, tostring(expected), tostring(result)))
end

-- identical paths
test_commonpath('a/b'     , 'a/b'     , 'a/b')
test_commonpath('/a/b'    , '/a/b'    , '/a/b')

-- common prefix
test_commonpath('/a/b'    , '/a/c'    , '/a/')
test_commonpath('a/b'     , 'a/c'     , 'a/')
test_commonpath('/a/b/c'  , '/a/b/d'  , '/a/b/')
test_commonpath('/a/b/c'  , '/a/x/y'  , '/a/')

-- one is prefix of the other
test_commonpath('/a/b'    , '/a/b/c'  , '/a/b')
test_commonpath('/a/b/c'  , '/a/b'    , '/a/b')
test_commonpath('a'       , 'a/b'     , 'a')

-- root only
test_commonpath('/a'      , '/b'      , '/')

-- no common path
test_commonpath('a'       , 'b'       , '')
test_commonpath(''        , 'a'       , nil)
test_commonpath('a'       , ''        , nil)
test_commonpath(''        , ''        , nil)

-- partial component match: result is up to last common separator
test_commonpath('abc'     , 'abd'     , '')
test_commonpath('/abc'    , '/abd'    , '/')

-- symmetry
test_commonpath('a/b/c'   , 'a/b/d'  , 'a/b/')
test_commonpath('a/b/d'   , 'a/b/c'  , 'a/b/')

-- trailing slashes
test_commonpath('a/b/'    , 'a/b/'   , 'a/b/')
test_commonpath('/a/'     , '/a/'    , '/a/')

--relpath --------------------------------------------------------------------

local function test_relpath(s, pwd, s2)
	local s1 = relpath(s, pwd)
	print('relpath', s, pwd, '->', s1)
	assert(s1 == s2, string.format(
		'relpath(%q, %q) expected %s, got %s',
		s, pwd, tostring(s2), tostring(s1)))
end

-- same dir
test_relpath('a/b'     , 'a/b'   , '.')
test_relpath('a/b/'    , 'a/b'   , './')

-- child path
test_relpath('a/b/c'   , 'a/b'   , 'c')
test_relpath('a/b/c/d' , 'a/b'   , 'c/d')

-- parent path (go up)
test_relpath('a/b'     , 'a/b/c' , '..')
test_relpath('a/b/'    , 'a/b/c' , '../')
test_relpath('a'       , 'a/b/c' , '../..')

-- sibling path
test_relpath('a/c'     , 'a/b'   , '../c')
test_relpath('a/b'     , 'a/c'   , '../b')
test_relpath('a/b/'    , 'a/c'   , '../b/')

-- deeper sibling
test_relpath('a/b/c'   , 'a/d/e' , '../../b/c')
test_relpath('a/b/'    , 'a/c/d' , '../../b/')

-- empty paths: path_commonpath returns nil for empty strings
test_relpath(''        , ''      , nil)
test_relpath(''        , 'a'     , nil)
test_relpath('a'       , ''      , nil)
test_relpath('a/'      , ''      , nil)

-- absolute paths
test_relpath('/a/c'    , '/a/b'  , '../c')
test_relpath('/a/b/c'  , '/a/b'  , 'c')
test_relpath('/a/b'    , '/a/c/d', '../../b')

-- no common prefix but non-empty inputs: commonpath returns '' (not nil)
test_relpath('a'       , 'b'     , '../a')

-- multi-level up + non-empty
test_relpath('a/b'     , 'a/c/d' , '../../b')

-- endsep preserved
test_relpath('a/b/c/'  , 'a/b'   , 'c/')

print''
print'All path tests passed!'
