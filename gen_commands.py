import json
from pprint import pprint, pformat


def gen_commands():
    commands = json.loads(open('commands.json', 'r').read())
    t = 'commands = {}'.format(pformat(commands))
    f = open('redis_fancy_cli/commands.py', 'w')
    f.write(t)
    f.close()


if __name__ == '__main__':
    gen_commands()

