--go@ c:\tools\plink.exe -i c:\users\woods\.ssh\id_ed25519.ppk root@172.20.10.9 ~/sdk/bin/debian12/luajit sdk/lua/mdbx_schema.lua
return {

	tables = {

		users = {

			fields = {
				{name = 'uid'    , type = 'u32'},
				{name = 'name'   , type = 'string' , maxlen = 256},
				{name = 'email'  , type = 'string' , maxlen = 256},
				{name = 'active' , type = 'u8'},
			},
			pk = 'uid',

		},

	},


}
