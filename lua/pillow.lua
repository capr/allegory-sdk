--[=[

	Fast image resampling based on Pillow SIMD.
	Written by Cosmin Apreutesei. Public Domain.

	pillow_image(bmp) -> img      create an image object from a bitmap
	img:resize([w], [h], [filter], [cx1], [cy1], [cx2], [cy2])  resize and/or crop
	img:bitmap() -> bmp           get the image as a bitmap with bmp:free()
	img:free()                    free the image

]=]

local ffi = require'ffi'
require'cpu_supports'
local C = ffi.load('pillow_simd'..(cpu_supports'avx2' and '_avx2' or ''))

ffi.cdef[[

typedef struct _pillow_image_t pillow_image_t;

pillow_image_t* pillow_image_create_for_data(
	char *data, const char* mode, int w, int h, int stride, int bottom_up);

void pillow_image_free(pillow_image_t*);

int    pillow_image_width  (pillow_image_t* im);
int    pillow_image_height (pillow_image_t* im);
char*  pillow_image_mode   (pillow_image_t* im);
char** pillow_image_rows   (pillow_image_t* im);

pillow_image_t* pillow_resample(
	pillow_image_t* im, int w, int h, int cx, int cy, int cw, int ch, int filter);

]]

local function ptr(p) return p ~= nil and p or nil end

local modes = {
	rgbx8 = 'RGB',
	rgba8 = 'RGBA',
	RGBa8 = 'RGBa', --premultiplied alpha
	g8    = 'L',
	cmyk8 = 'CMYK',
	yccx8 = 'YCbCr', --not in bitmap module
	labx8 = 'Lab', --not in bitmap module
}

local formats = {}
for k,v in pairs(modes) do formats[v] = k end

function pillow_image(bmp)
	local mode = assert(modes[bmp.format], 'unsupported format')
	return assert(ptr(C.pillow_image_create_for_data(
		bmp.data, mode, bmp.w, bmp.h, bmp.stride, bmp.bottom_up and 1 or 0)))
end

local filters = {
	lanczos  = 1,
	bilinear = 2,
	bicubic  = 3,
	box      = 4,
	hamming  = 5,
}
local min, max = math.min, math.max
local function clamp(x, x0, x1)
	return min(max(x, x0), x1)
end
local function resize(im, w, h, filter, cx1, cy1, cx2, cy2)
	local filter = assert(filters[filter or 'bilinear'], 'unknown filter')
	local w0 = im:width()
	local h0 = im:height()
	cx1 = cx1 or 0
	cy1 = cy1 or 0
	cx2 = cx2 or w0
	cy2 = cy2 or h0
	if cx2 < 0 then cx2 = w0 - (-cx2) end
   if cy2 < 0 then cy2 = h0 - (-cy2) end
	cx1 = clamp(cx1, 0, w0)
	cy1 = clamp(cy1, 0, h0)
	cx2 = clamp(cx2, 0, w0)
	cy2 = clamp(cy2, 0, h0)
	cx2 = max(cx2, cx1)
	cy2 = max(cy2, cy1)
	w = max(w or (cx2 - cx1), 1)
	h = max(h or (cy2 - cy1), 1)
	return assert(ptr(C.pillow_resample(im, w, h, cx1, cy1, cx2, cy2, filter)))
end

local function to_bitmap(im)
	local w = im:width()
	local h = im:height()
	local stride = w * 4
	return {
		format = formats[im:mode()],
		w = w, h = h, stride = stride, size = stride * h,
		rows = im:rows(),
		free = function() im:free() end,
	}
end

ffi.metatype('pillow_image_t', {__index = {
	free   = C.pillow_image_free,
	rows   = C.pillow_image_rows,
	width  = C.pillow_image_width,
	height = C.pillow_image_height,
	mode   = function(im) return ffi.string(C.pillow_image_mode(im)) end,
	bitmap = to_bitmap,
	resize = resize,
}})

if not ... then --self-test

	--create a 100x100 rgbx8 bitmap with known pixel data
	local w, h = 100, 100
	local stride = w * 4
	local data = ffi.new('char[?]', stride * h)
	for y = 0, h-1 do
		for x = 0, w-1 do
			local p = data + y * stride + x * 4
			p[0] = x       --R
			p[1] = y       --G
			p[2] = x + y   --B
			p[3] = 0       --X
		end
	end
	local bmp = {format = 'rgbx8', w = w, h = h, stride = stride, data = data}

	--create image and check properties
	local im = pillow_image(bmp)
	assert(im:width() == 100)
	assert(im:height() == 100)
	assert(im:mode() == 'RGB')

	--resize with each filter
	for _, filter in ipairs{'lanczos', 'bilinear', 'bicubic', 'box', 'hamming'} do
		local im2 = im:resize(50, 50, filter)
		assert(im2:width() == 50)
		assert(im2:height() == 50)
		im2:free()
	end

	--resize with default filter
	local im2 = im:resize(200, 150)
	assert(im2:width() == 200)
	assert(im2:height() == 150)
	im2:free()

	--crop and resize
	local im2 = im:resize(30, 30, 'bilinear', 10, 10, 50, 50)
	assert(im2:width() == 30)
	assert(im2:height() == 30)
	im2:free()

	--bitmap round-trip
	local im2 = im:resize(60, 40)
	local bmp2 = im2:bitmap()
	assert(bmp2.w == 60)
	assert(bmp2.h == 40)
	assert(bmp2.format == 'rgbx8')
	assert(bmp2.stride == 60 * 4)
	bmp2.free()

	im:free()

	print'pillow ok'

end
