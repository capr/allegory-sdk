require'path'

--basename -------------------------------------------------------------------

local function test(s, s2)
	local s1 = basename(s, pl)
	print('basename', s, '->', s1)
	assert(s1 == s2)
end
test(''    , '')
test('/'   , '')
test('a'   , 'a')
test('a/'  , '')
test('/a'  , 'a')
test('a/b' , 'b')
test('a/b/', '')
test('a/b' , 'b')
test('a/b/', '')

--nameext --------------------------------------------------------------------

local function test(s, name2, ext2)
	local name1, ext1 = path_nameext(s, pl)
	print('nameext', s, '->', name1, ext1)
	assert(name1 == name2)
	assert(ext1 == ext2)
end

test('',             '', nil)
test('/',            '', nil)
test('a/',           '', nil)
test('/a/b/a',       'a', nil)
test('/a/b/a.',      'a', '') --invalid filename on Windows
test('/a/b/a.txt',   'a', 'txt')
test('/a/b/.bashrc', '.bashrc', nil)

--dirname --------------------------------------------------------------------

local function test(s, s2)
	local s1 = dirname(s, pl)
	print('dirname', s, '->', s1)
	assert(s1 == s2)
end

--empty rel path has no dir
test('', nil)

--current dir has no dir
test('.', nil)

--root has no dir
test('/', nil)

--dir is root
test('/b' , '/')
test('/aa', '/')

--dir is the current dir
test('a'  , '.')
test('aa' , '.')

--dir of empty filename
test('a/', 'a')
test('./', '.')

--dir of non-empty filename
test('a/b'  , 'a')
test('aa/bb', 'aa')
test('a/b'  , 'a')

--gsplit ---------------------------------------------------------------------

function test(s, full, t2)
	local t1 = {}
	for s, sep in path_split(s, full) do
		table.insert(t1, s)
		table.insert(t1, sep)
	end
	print('split', s, full, '->', pp(t1))
	assert(pp(t1) == pp(t2))
end

test(''       {})
test('/'      {'', '/'})
test('/a'     {'', '/', 'a', ''})
test('/a/'    {'', '/', 'a', '/'})
test('//a//'  {'', '\\/', 'a', '\\/'})
test('a/b/c'  {'a', '/', 'b\\c', ''})

--normalize ------------------------------------------------------------------

local function test(s, opt, s2)
	local s1 = path_normalize(s, opt)
	print('normal', s, 'opt', '->', s1)
	assert(s1 == s2)
end

--remove `.`
local opt = {dot_dot_dirs = true, endsep = 'leave', sep = 'leave'}
test('.', 'win', opt, '.')
test('./', 'win', opt, './')
test('C:.', 'win', opt, 'C:')
test('C:./', 'win', opt, 'C:')
test('.\\', 'win', opt, '.\\')
test('./.', 'win', opt, '.')
test('./.\\', 'win', opt, '.\\')
test('/.', 'win', opt, '/')
test('\\./', 'win', opt, '\\') --root slash kept
test('/.\\.', 'win', opt, '/') --root slash kept
test('/a/.', 'win', opt, '/a')
test('/./a', 'win', opt, '/a')
test('./a', 'win', opt, 'a')
test('a/.', 'win', opt, 'a')
test('a\\.', 'win', opt, 'a')
test('a\\./', 'win', opt, 'a\\')
test('a/b\\c', 'win', opt, 'a/b\\c')
test('a\\././b///', 'win', opt, 'a\\b///')
test('a/.\\.\\b\\\\', 'win', opt, 'a/b\\\\')

--remove `..`
local opt = {dot_dirs = true, endsep = 'leave', sep = 'leave'}
test('a/b/..', 'win', opt, 'a') --remove endsep from leftover
test('a/b/c/..', 'win', opt, 'a/b') --remove endsep from leftover
test('a/..', 'win', opt, '.') --no leftover to remove endsep from
test('\\a/..', 'win', opt, '\\') --can't remove endsep from empty abs path
test('\\a/../', 'win', opt, '\\') --keep endsep
test('\\../', 'win', opt, '\\') --remove from root, keep endsep
test('a\\b/../', 'win', opt, 'a\\') --keep endsep
test('a/../', 'win', opt, './') --no leftover to see endsep
test('C:/a/b/..', 'win', opt, 'C:/a')
test('C:/a/b/c/../..', 'win', opt, 'C:/a')
--remove till empty
test('a/..', 'win', opt, '.')
test('a/b/../..', 'win', opt, '.')
test('C:/a/..', 'win', opt, 'C:/') --keep endsep
test('C:/a/b/../..', 'win', opt, 'C:/') --keep endsep
--one `..` too many from rel paths
test('..', 'win', opt, '..')
test('../', 'win', opt, '../')
test('../..', 'win', opt, '../..')
test('../..\\', 'win', opt, '../..\\')
test('a/..\\', 'win', opt, '.\\')
test('a/b/../../..', 'win', opt, '..')
--one `..` too many from abs paths
test('/..', 'win', opt, '/')
test('/..\\', 'win', opt, '/')
test('/../..', 'win', opt, '/')
test('/../..\\', 'win', opt, '/')
test('C:/a/b/../../..', 'win', opt, 'C:/')
--skip `.` dirs when removing
test('a/b/./././..', 'win', opt, 'a/././.')
test('a/./././..', 'win', opt, '././.')
test('./././..', 'win', opt, './././..')
test('/./././..', 'win', opt, '/./././..')

--default options: remove `.` and `..` and end-slash, set-sep-if-mixed.
test('C:///a/././b/x/../c\\d', 'win', nil, 'C:\\a\\b\\c\\d')
--default options: even when not mixed, separators are collapsed.
test('C:///a/././b/x/../c/d', 'win', nil, 'C:/a/b/c/d')
--default options: remove endsep
test('.\\', 'win', nil, '.')
test('.\\././.\\', 'win', nil, '.')
test('C:./', 'win', nil, 'C:')

--combine (& implicitly abs) -------------------------------------------------

local function test(s1, s2, p2, err2)
	local p1, err1 = path_combine(s1, s2, pl)
	print('combine', s1, s2, '->', p1, err1, err1)
	assert(p1 == p2)
	if err2 then
		assert(err1:find(err2, 1, true))
	end
end

-- any + '' -> any
test('C:a/b', '', 'win', 'C:a/b')

-- any + c/d -> any/c/d
test('C:\\', 'c/d', 'win', 'C:\\c/d')

-- C:a/b + /d/e -> C:/d/e/a/b
test('C:a/b', '\\d\\e', 'win', 'C:\\d\\e/a/b')

-- C:/a/b + C:d/e -> C:/a/b/d/e
test('C:/a/b', 'C:d\\e', 'win', 'C:/a/b/d\\e')

-- errors
test('/a', '/b', 'win', nil, 'cannot combine') --types
test('C:', 'D:', 'win', nil, 'cannot combine') --drives

--rel ------------------------------------------------------------------------

local function test(s, pwd, s2, sep, default_sep)
	local s1 = relpath(s, pwd, sep, default_sep)
	print('rel', s, pwd, '->', s1)
	assert(s1 == s2)
end

test('/a/c',   '/a/b', '../c', 'win')
test('/a/b/c', '/a/b', 'c',    'win')

test('',  '',    '.'      )
test('',  'a',   '..'     )
test('a',  '',   'a'      )
test('a/', '',   'a/'     )
test('a',  'b',  '../a',  '/')
test('a/', 'b',  '../a/'  )
test('a',  'b/', '../a'   )

test('a/b',    'a/c',   '../b'     ) --1 updir + non-empty
test('a/b/',   'a/c',   '../b/'    ) --1 updir + non-empty + endsep
test('a/b',    'a/b/c', '..'       ) --1 updir + empty
test('a/b/',   'a/b/c', '../'      ) --1 updir + empty + endsep
test('a/b/c',  'a/b',   'c'        ) --0 updirs + non-empty
test('a/b',    'a/b',   '.'        ) --0 updirs + empty
test('a/b/',   'a/b',   './'       ) --0 updirs + empty + endsep
test('C:a/b/', 'C:a/b', './'       ) --0 updirs + empty + endsep
test('a/b',    'a/c/d', '../../b'  ) --2 updirs + non-empty
test('a/b/',   'a/c/d', '../../b/' ) --2 updirs + non-empty + endsep
test('C:/a/b', 'C:/a' , 'b'        ) --0 updirs + non-empty (DOS)
