pybusta
=======

A Python library and samle CLI utility to access, index, query and extract files from local Flibusta mirror (e.g.: magnet:?xt=urn:btih:1077f09ffb0a146f25960a8f43cf32087bfc5deb )

FliShell CLI installation:

1. Make sure you have python >=2.7 installed
2. Clone the repository
3. Create directory "data" inside cloned directory
4. Copy/symlink fb2.Flibusta.Net into data directory
5. Use: 
# Query a local search index. Creates one if absent.
	./flishell.py search --title "example book" --author "example author"
# Choose a book to extract, copy id (e.g. 12345)
# Extract a book chosen into data/books directory
	./flishell.py extract --id 12345
