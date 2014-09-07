pybusta
=======

A Python library and sample CLI utility (flishell.py) to access, index, query and extract files from local Flibusta mirror (e.g.: magnet:?xt=urn:btih:1077f09ffb0a146f25960a8f43cf32087bfc5deb )

FliShell CLI installation:

1. Make sure you have python >=2.7 installed
2. Clone the repository
3. Create directory "data" inside cloned directory
4. Copy/symlink fb2.Flibusta.Net into data directory
5. Use: 
  * Query a local search index. Creates one if absent:
	./flishell.py search --title "example book" --author "example author"
  * Choose a book to extract, copy id (e.g. 12345)
  * Extract a book chosen into data/books directory: 
	./flishell.py extract --id 12345


Webusta web-shell installation:
1. See installation steps 1-4 above
2. Python modules required: pyramid pyramid__mako sqlite3
3. Binds by default to port 8080. Can be changed in the source code
4. Use:
  * run: ./webusta.py
  * point your browser to http://localhost:8080
