gencommandspys:
	curl https://raw.githubusercontent.com/antirez/redis-doc/master/commands.json > commands.json
	python gen_commands.py