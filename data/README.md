# data/

Dat file `IP2LOCATION-LITE-DB1.BIN` (hoac `DB5` tro len neu muon co ca region/city)
vao thu muc nay truoc khi chay pipeline.

Tai mien phi (can dang ky tai khoan, khong mat phi) tai:
https://lite.ip2location.com

Khong commit file `.BIN` len git (da bi loai trong `.gitignore`) - license cua
IP2Location khong cho phep redistribute lai database.

Neu doi ten file khac `IP2LOCATION-LITE-DB1.BIN`, sua `_DB_FILENAME` trong
`country/country.py` cho khop.
