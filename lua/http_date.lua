
--HTTP date parsing and formatting (RFC 1123 only).
--Written by Cosmin Apreutesei. Public Domain.

require'glue'

local wdays  = {'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'}
local months = {'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'}

local wdays_map  = index(wdays)
local months_map = index(months)

--wkday "," SP 2DIGIT-day SP month SP 4DIGIT-year SP 2DIGIT ":" 2DIGIT ":" 2DIGIT SP "GMT"
--eg. Sun, 06 Nov 1994 08:49:37 GMT
function http_date_parse(s)
	local w,d,mo,y,h,m,s = s:match'([A-Za-z]+), (%d+)[ %-]([A-Za-z]+)[ %-](%d+) (%d+):(%d+):(%d+) GMT'
	d,y,h,m,s = tonumber(d),tonumber(y),tonumber(h),tonumber(m),tonumber(s)
	w = wdays_map[w]
	mo = months_map[mo]
	if not (w and mo and d >= 1 and d <= 31 and y <= 9999
			and h <= 23 and m <= 59 and s <= 59) then return end
	return {wday = w, day = d, year = y, month = mo,
			hour = h, min = m, sec = s, utc = true}
end

function http_date_format(t)
	if istab(t) then
		t = time(t)
	end
	local t = date('!*t', t)
	return string.format('%s, %02d %s %04d %02d:%02d:%02d GMT',
		wdays[t.wday], t.day, months[t.month], t.year, t.hour, t.min, t.sec)
end

--self-test

if not ... then
	require'unit'
	local d = {day = 6, sec = 37, wday = 1, min = 49, year = 1994, month = 11, hour = 8, utc = true}
	test(http_date_parse'Sun, 06 Nov 1994 08:49:37 GMT', d)
	test(http_date_parse'Sun, 06-Nov-1994 08:49:37 GMT', d)
	test(http_date_parse'Sun Nov 66 08:49:37 1994', nil)
	d.wday = nil --it gets populated based on date.
	test(http_date_format(d), 'Sun, 06 Nov 1994 08:49:37 GMT')
	print'http_date ok'
end
