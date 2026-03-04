require'multipart'
require'fs'

local req = multipart_mail{
	from = 'thedude@dude.com',
	text = 'Hello Dude!',
	html = '<h1>Hello</h1><p>Hello Dude</p>',
	inlines = {
		{
			cid = 'img1',
			filename = 'progressive.jpg',
			contents = load(exedir()..'/../tests/jpeg_test/progressive.jpg'),
		},
		{
			cid = 'img2',
			filename = 'birds.jpg',
			contents = load(exedir()..'/../tests/resize_image_test/birds.jpg'),
		},
	},
	attachments = {
		{
			filename = 'att1.txt',
			content_type = 'text/plain',
			contents = 'att1!',
		},
		{
			filename = 'att2.txt',
			content_type = 'text/plain',
			contents = 'att2!',
		},
	},
}

local s1 = load(exedir()..'/../tests/multipart_test/multipart_test.txt')
local s2 = pp(req.headers, '\t') .. '\n' .. req.message:gsub('\r', '')
assert(s1 == s2)
