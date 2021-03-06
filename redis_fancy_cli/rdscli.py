from __future__ import unicode_literals
from prompt_toolkit import prompt, ANSI
from prompt_toolkit.styles import Style, merge_styles
from redis.connection import SYM_EMPTY, SYM_STAR, SYM_CRLF, SYM_DOLLAR

from .redis_lexer import RedisLexer, tokenize_redis_command
from prompt_toolkit.lexers import PygmentsLexer
from pygments.styles import get_style_by_name
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from pygments import highlight, lexers, formatters
import redis
import json
from prompt_toolkit import print_formatted_text
from prompt_toolkit.completion import Completer, Completion, merge_completers
from prompt_toolkit.formatted_text import HTML
import six
import time
import click
import logging
from cli_helpers.tabular_output import TabularOutputFormatter
from .config import get_config
from .commands import commands
import xml

logger = None


def debug(s, *args):
    logger.debug(s + '%s ' * len(args), *args)


def b(x):
    """\
    force bytes
    :param x:
    :return:
    """
    return x.encode('latin-1') if not isinstance(x, bytes) else x


REDIS_COMMANDS = [ 
        'APPEND', 'AUTH', 'BGREWRITEAOF', 'BGSAVE', 'BITCOUNT', 'BITFIELD', 'BITOP', 'BITPOS',
        'BLPOP', 'BRPOP', 'BRPOPLPUSH', 'CLIENT KILL', 'CLIENT LIST', 'CLIENT GETNAME', 'CLIENT PAUSE', 'CLIENT REPLY',
        'CLIENT SETNAME', 'CLUSTER ADDSLOTS', 'CLUSTER COUNT-FAILURE-REPORTS', 'CLUSTER COUNTKEYSINSLOT',
        'CLUSTER DELSLOTS', 'CLUSTER FAILOVER', 'CLUSTER FORGET', 'CLUSTER GETKEYSINSLOT',
        'CLUSTER INFO', 'CLUSTER KEYSLOT', 'CLUSTER MEET', 'CLUSTER NODES', 'CLUSTER REPLICATE',
        'CLUSTER RESET', 'CLUSTER SAVECONFIG', 'CLUSTER SET-CONFIG-EPOCH',
        'CLUSTER SETSLOT', 'CLUSTER SLAVES', 'CLUSTER SLOTS', 'COMMAND', 
        'COMMAND COUNT', 'COMMAND GETKEYS', 'COMMAND INFO', 'CONFIG GET',
        'CONFIG REWRITE', 'CONFIG SET', 'CONFIG RESETSTAT', 'DBSIZE', 'DEBUG OBJECT', 'DEBUG SEGFAULT', 'DECR', 'DECRBY',
        'DEL', 'DISCARD', 'DUMP', 'ECHO', 'EVAL', 'EVALSHA', 'EXEC', 'EXISTS',
        'EXPIRE', 'EXPIREAT', 'FLUSHALL', 'FLUSHDB', 'GEOADD', 'GEOHASH', 'GEOPOS', 'GEODIST',
        'GEORADIUS', 'GEORADIUSBYMEMBER', 'GET', 'GETBIT', 'GETRANGE', 'GETSET', 'HDEL', 'HEXISTS',
        'HGET', 'HGETALL', 'HINCRBY', 'HINCRBYFLOAT', 'HKEYS', 'HLEN', 'HMGET', 'HMSET',
        'HSET', 'HSETNX', 'HSTRLEN', 'HVALS', 'INCR', 'INCRBY', 'INCRBYFLOAT', 'INFO',
        'KEYS', 'LASTSAVE', 'LINDEX', 'LINSERT', 'LLEN', 'LPOP', 'LPUSH', 'LPUSHX',
        'LRANGE', 'LREM', 'LSET', 'LTRIM', 'MGET', 'MIGRATE', 'MONITOR', 'MOVE',
        'MSET', 'MSETNX', 'MULTI', 'OBJECT', 'PERSIST', 'PEXPIRE', 'PEXPIREAT', 'PFADD',
        'PFCOUNT', 'PFMERGE', 'PING', 'PSETEX', 'PSUBSCRIBE', 'PUBSUB', 'PTTL', 'PUBLISH',
        'PUNSUBSCRIBE', 'QUIT', 'RANDOMKEY', 'READONLY', 'READWRITE', 'RENAME', 'RENAMENX', 'RESTORE',
        'ROLE', 'RPOP', 'RPOPLPUSH', 'RPUSH', 'RPUSHX', 'SADD', 'SAVE', 'SCARD',
        'SCRIPT DEBUG', 'SCRIPT EXISTS', 'SCRIPT FLUSH', 'SCRIPT KILL', 'SCRIPT LOAD', 'SDIFF', 'SDIFFSTORE', 'SELECT',
        'SET', 'SETBIT', 'SETEX', 'SETNX', 'SETRANGE', 'SHUTDOWN ', 'NOSAVE ', 'SAVE',
        'SINTER', 'SINTERSTORE', 'SISMEMBER', 'SLAVEOF', 'SLOWLOG', 'SMEMBERS', 'SMOVE', 'SORT',
        'SPOP', 'SRANDMEMBER', 'SREM', 'STRLEN', 'SUBSCRIBE', 'SUNION', 'SUNIONSTORE', 'SYNC',
        'TIME', 'TOUCH', 'TTL', 'TYPE', 'UNSUBSCRIBE', 'UNWATCH', 'WAIT', 'WATCH',
        'ZADD', 'ZCARD', 'ZCOUNT', 'ZINCRBY', 'ZINTERSTORE', 'ZLEXCOUNT', 'ZRANGE', 'ZRANGEBYLEX',
        'ZREVRANGEBYLEX', 'ZRANGEBYSCORE', 'ZRANK', 'ZREM', 'ZREMRANGEBYLEX', 'ZREMRANGEBYRANK', 'ZREMRANGEBYSCORE', 'ZREVRANGE',
        'ZREVRANGEBYSCORE', 'ZREVRANK', 'ZSCORE', 'ZUNIONSTORE', 'SCAN', 'SSCAN', 'HSCAN', 'ZSCAN' 
    ]
REDIS_COMMANDS.sort()


def is_json(s):
    try:
        return json.loads(s)
    except:
        pass


def colourful_json(o):
    formatted_json = json.dumps(o, indent=4)
    colorful_json = highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())
    return colorful_json


def force_unicode(s):
    if isinstance(s, six.binary_type):
        return s.decode('utf-8', 'backslashreplace')
    if isinstance(s, int):
        return str(s)
    return s


class memoize_for(object):
    """\
    dead simple memoize decorator, returns same result for ttl seconds
    """
    def __init__(self, ttl):
        self.last_call = -1
        self.cached = None
        self.ttl = ttl

    def __call__(self, fn):
        def wrapper(*args, **kwargs):
            if (time.time() - self.last_call) > self.ttl:
                self.last_call = time.time()
                self.cached = fn(*args, **kwargs)
            return self.cached
        return wrapper


class State(object):
    def __init__(self, client):
        self.client = client

    def when(self, cmd, previous_state):
        return self

    def process_command(self, cmd):
        return cmd

    def process_reply(self, resp):
        return resp

    def state_change(self, previous_state, next_state):
        pass


class DefaultState(State):
    def when(self, cmd, current_state):
        return self


class MonitorState(State):
    def when(self, cmd, current_state):
        if cmd and cmd[0].upper().startswith('MONITOR'):
            return self
        return current_state

    def process_command(self, cmd):
        return cmd

    def process_reply(self, resp):
        if resp == b'OK':
            while True:
                # this is blocking
                resp = self.client.rds.read_response()
                print_formatted_text(HTML(force_unicode(resp)))
        return resp


class SelectState(State):
    def when(self, cmd, current_state):
        debug("settings select state", cmd, current_state)
        if cmd and cmd[0].upper() == 'SELECT':
            return self
        return current_state

    def process_command(self, cmd):
        self.next_db = cmd[-1]
        return cmd

    def process_reply(self, resp):
        debug("select resp [", resp)
        if resp == b'OK':
            self.client.selected_db = self.next_db
        self.client.set_state(self.client.avail_states[0])
        return resp


class InfoState(State):
    def when(self, cmd, current_state):
        if cmd and cmd[0].upper().startswith('INFO'):
            return self
        return current_state

    def process_command(self, cmd):
        return cmd

    def process_reply(self, resp):
        data = force_unicode(resp)
        tmp_data = {}
        t = []
        for line in data.split('\r\n'):
            if line.startswith('#'):
                header = line.replace('#', '')
                tmp_data[header] = []
                t = tmp_data[header]
            else:
                t.append(line)

        ret = []
        headers = list()
        for header, data in tmp_data.items():
            ret.append(header)
            values = []
            for lndata in data:
                if not lndata.strip():
                    continue
                k, v = lndata.split(':')
                if ',' in v:
                    vals = []
                    headers.append(' ')
                    for tv in v.split(','):
                        tk, rtv = tv.split('=')
                        if tk not in headers:
                            headers.append(tk)
                        vals.append(rtv)
                    tv = [k] + vals
                    values.append(tv)
                else:
                    values.append([k, v])

            formatter = TabularOutputFormatter()
            ret.append('\n'.join([ln for ln in formatter.format_output(values, headers, format_name='fancy_grid')]))

        self.client.set_state(self.client.avail_states[0])
        return '\n'.join(ret)


def with_scores_headers(cmd):
    debug("withscores >>>", cmd)
    if cmd[-1].upper() == 'WITHSCORES':
        return ('Key', 'Value')
    return ('Value',)


def table_headers(cmd):
    return ('Key', 'Value')


table_commands = {
    'HGETALL': table_headers,
    'ZRANGE':  with_scores_headers,
    'ZREVRANGE': with_scores_headers,
    'ZRANGEBYLEX': table_headers,
    'ZRANGEBYSCORE': table_headers,
    'ZREVRANGEBYLEX': table_headers,
    'ZREVRANGEBYSCORE': table_headers,
    'ZREVRANK': table_headers,
}


class TableOutputState(State):
    def when(self, cmd, current_state):
        if cmd and cmd[0].upper() in table_commands:
            return self
        return current_state

    def process_command(self, cmd):
        # we store a ref. for cmd, because if withscores etc. is there,
        # then the data shape is different
        self.cmd = cmd
        return cmd

    def process_reply(self, resp):
        data = force_unicode(resp)
        if not self.client.table_mode:
            return data

        headers = table_commands[self.cmd[0].upper()](self.cmd)

        values = []
        i = iter(data)
        for v in zip(*([i] * len(headers)) ):
            values.append(v)

        formatter = TabularOutputFormatter()
        self.client.set_state(self.client.avail_states[0])
        return '\n'.join([ln for ln in formatter.format_output(values, headers, format_name='fancy_grid')])


class Client(object):
    def __init__(self, rds):
        self.rds = rds
        self.selected_db = 0
        self.table_mode = True
        self.default_state = DefaultState(self)

        self.avail_states = [
            self.default_state,
            SelectState(self),
            InfoState(self),
            TableOutputState(self),
            MonitorState(self)
        ]

        self.state = self.avail_states[0]
        self.find_next_state('')

    def find_next_state(self, cmd):
        next_state = self.state
        for st in self.avail_states:
            next_state = st.when(cmd, next_state)
        self.set_state(next_state)

    def set_state(self, state):
        self.state = state

    def process_command(self, cmd):
        self.find_next_state(cmd)
        return self.state.process_command(cmd)

    def process_reply(self, reply):
        return self.state.process_reply(reply)

    def send_command(self, raw):
        cmd = tokenize_redis_command(raw)
        args = self.process_command(cmd)
        logger.debug(args)
        self.rds.send_command(*args)

    def send_and_receive_command(self, raw):
        self.send_command(raw)
        return self.read_response()

    def pack_command(self, *args):
        """Pack a series of arguments into the Redis protocol
        this expects only array of bytes
        """
        output = []
        buff = SYM_EMPTY.join(
            (SYM_STAR, b(str(len(args))), SYM_CRLF))

        for arg in args:
            buff = SYM_EMPTY.join((buff, SYM_DOLLAR, b(str(len(arg))),
                                   SYM_CRLF, arg, SYM_CRLF))
        output.append(buff)
        return output

    def read_response(self):
        try:
            resp = self.rds.read_response()
        except redis.exceptions.ResponseError as e:
            print_formatted_text(HTML('<b>Response error: {}</b>'.format(e)))
            logger.exception('response error')
            raise

        resp = self.process_reply(resp)
        return resp

    @memoize_for(2)
    def keycount(self):
        """
        dbsize might be disabled in a shared env. for some reason,
        we return -1 and try to be safe for that case and dont do key completion
        :return:
        """
        try:
            self.send_command('DBSIZE')
            resp = self.read_response()
            logger.debug("resp dbsize %s", resp)
            return resp
        except:
            return -1


def print_response(resp):
    """ if the response is text, first try to print as json,
        if not possible then print the response.
    """
    if isinstance(resp, list):
        colors = ['\u001b[32m', '\u001b[33m']
        for c, ln in enumerate(resp):
            color = colors[ c % len(colors) ]
            print_formatted_text(ANSI('{}{}'.format(color, '{}) {}'.format(c+1, force_unicode(ln)))))
        return

    j = is_json(resp)
    if j:
        return print_formatted_text(ANSI(colourful_json(j)))

    if isinstance(resp, int):
        return print_formatted_text(resp)

    try:
        print_formatted_text(HTML(force_unicode(resp)))
    except xml.parsers.expat.ExpatError:
        print_formatted_text(HTML(str(resp)))
    except:
        if isinstance(resp, bytes):
            logger.critical("got error on response %s", force_unicode(resp.decode('utf8', 'backslashreplace')))
        logger.info(resp)
        logger.exception('Exception in print_formatted_text')


class RedisCompleter(Completer):

    def set_client(self, client):
        self.client = client

    def get_completions(self, document, complete_event):
        """
        TODO: very complicated !

        :param document:
        :param complete_event:
        :return:
        """
        word_before_cursor = document.get_word_before_cursor()  
        # logger.debug('word before cursor: "%s"', word_before_cursor)

        if not document.text:
            for k in REDIS_COMMANDS:
                yield Completion(k, display_meta='', start_position=-len(word_before_cursor))

        elif document.text.startswith('.'):
            for k, v in sorted(CLI_COMMANDS.items()):
                yield Completion(k, display_meta=v, start_position=-len(word_before_cursor)-1)

        elif ' ' not in document.text:
            for k, v in commands.items():
                if k.lower().startswith(document.text.lower()):
                    yield Completion(k, display_meta=v['summary'], start_position=-len(word_before_cursor))
        else:
            if document.text.lower().strip() == 'select':
                for k in range(16):
                    yield Completion(str(k), start_position=-len(word_before_cursor))

            if document.text.lower().strip() == 'info':
                for n in ['Keyspace', 'Cluster', 'CPU', 'Replication', 'Stats', 'Persistence', 'Memory', 'Clients', 'Server']:
                    yield Completion(n, start_position=-len(word_before_cursor))
            else:
                if not self.client.keycount():
                    return

                if self.client.keycount() == -1:
                    return

                if self.client.keycount() > 1000:
                    return

                ask_k = word_before_cursor.replace('keys ', '').strip()
                if not ask_k:
                    return
                debug('"keys *{}*"'.format(ask_k))
                self.client.rds.send_command('keys *{}*'.format(ask_k))
                resp = self.client.rds.read_response()
                for k in resp:
                    yield Completion(force_unicode(k), start_position=-len(word_before_cursor))


CLI_COMMANDS = {}

cli_command_fns = {}

class cli_command(object):
    def __init__(self, cmd, help_text=None):
        self.cmd = cmd
        self.help_text = help_text

    def __call__(self, fn):
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        cli_command_fns['.{}'.format(self.cmd)] = wrapper
        CLI_COMMANDS['.{}'.format(self.cmd)] = self.help_text
        return wrapper


@cli_command('help', 'get info about a command')
def run_help_command(*args, client=None):
    for arg in args:
        c = commands.get(arg.upper())
        if not c:
            print_formatted_text('Command does not exists')
            return
        print_formatted_text(c)


@cli_command('table', 'enable/disable table mode output for some command results')
def enable_disable_table_mode(*args, client=None):
    client.table_mode = not client.table_mode
    print_formatted_text("table output [{}]".format(client.table_mode))


@cli_command('copydb', 'copies all keys from one database to another database')
def run_copy_database(*args, client=None):
    client.send_command('keys *')
    keys = client.read_response()
    dumps = []
    for k in keys:
        # print_formatted_text("key - {}".format(k))
        client.send_command('dump "{}"'.format(k.decode('utf-8')))
        v = client.read_response()
        dumps.append((k, v))
    client.send_command('select {}'.format(args[0]))
    print_formatted_text("selected {}".format(client.read_response()))
    cnt = 0
    for k, v in dumps:
        cmd = [b'restore', k, b'0', v]
        packed_command = client.pack_command(*cmd)
        client.rds.send_packed_command(packed_command)
        try:
            client.read_response()
            cnt += 1
        except redis.exceptions.ResponseError as e:
            if 'BUSYKEY' in str(e):
                print_formatted_text(HTML('<b>BUSYKEY {} - make sure target database is clear</b>'.format(k)))
    print_formatted_text(HTML('<b>{} keys copied</b>'.format(cnt)))


@cli_command('flushdb', 'flushes the database')
def flushdb(*args, client=None):
    if args[0]:
        client.send_and_receive_command('select {}'.format(args[0]))
        print_formatted_text(client.send_and_receive_command('flushdb'))
    else:
        print_formatted_text(HTML('<b>Need a db number</b>'))


@cli_command('now', 'show unix timestamp')
def show_unix_time(*args, client=None):
    print_formatted_text("{}".format(int(time.time())))


@cli_command('exit', 'exits the cli')
def exit():
    pass


@cli_command('connect', 'connects to another redis server')
def connect(host, port, database=0, password=None, client=None):
    rds = redis.Connection(host=host,
                           port=int(port), db=database, password=password)
    client.host = host
    client.port = port
    client.password = password
    rds.connect()
    client.rds = rds
    print_formatted_text("connected to:{}:{} [{}]".format(host, port, database))


def run_dot_command(command, client=None):
    cmd = command.split(' ')
    debug('cli command', command)
    cli_command_fns[cmd[0]](*cmd[1:], client=client)


@click.command()
@click.option("--host", '-h', default="127.0.0.1", help="Host")
@click.option("--port", '-p', default=6379, help="Host")
@click.option("--database", '-d', default=0, help="Database")
@click.option("--password", "-a", default=None, help="Authentication/Password")
def main(host, port, database, password):
    global logger
    config = get_config()
    # print("log file", config['log_file'])
    logging.basicConfig(filename=config['log_file'], level=logging.getLevelName(config['log_level']))
    logger = logging.getLogger(__name__)

    style_base = style_from_pygments_cls(get_style_by_name('monokai'))

    style_custom = Style.from_dict({
        'completion-menu.completion': 'bg:#008888 #ffffff',
        'completion-menu.completion.current': 'bg:#00aaaa #000000',
        'scrollbar.background': 'bg:#88aaaa',
        'scrollbar.button': 'bg:#222222',
    })

    style = merge_styles([style_base, style_custom])

    session = PromptSession(history=FileHistory(config['history_file']), style=style)

    def bottom_toolbar():
        return HTML('{}:{} db:{} keys:{}'.format(client.host, client.port, client.selected_db, client.keycount()))

    def send_command(raw):
        try:
            client.send_command(raw)
            resp = client.read_response()
        except Exception as e:
            logger.exception('error on send command')
            return print_formatted_text('Error: {} '.format(e))

        if not resp:
            return print_formatted_text('null')

        print_response(resp)

    rds = redis.Connection(host=host, port=int(port), db=database, password=password)
    rds.connect()

    client = Client(rds=rds)
    client.host = host
    client.port = port
    client.pasword = password
    client.selected_db = database
    rds_completer = RedisCompleter()
    rds_completer.set_client(client)
    completer = merge_completers([rds_completer])

    multiline = False

    while True:
        try:
            text = session.prompt('{}:{} [{}]> '.format(client.host, client.port, client.selected_db),
                    lexer=PygmentsLexer(RedisLexer),
                    completer=completer,
                    bottom_toolbar=bottom_toolbar,
                    multiline=multiline,
                    auto_suggest=AutoSuggestFromHistory())

            if text.lower() == '.multiline':
                multiline = not multiline
                print_formatted_text("multiline mode [{}]".format(multiline))
                if multiline:
                    print_formatted_text('press Escape and then ENTER to send command')
                continue

            c = text.split(' ')
            if c[0].startswith('.'):
                # our dot commands
                try:
                    run_dot_command(text, client=client)
                except KeyError as e:
                    print_formatted_text('Unknown .command [{}]'.format(text))
                continue

            if text.upper() in ('QUIT', 'EXIT', '.EXIT', '.QUIT'):
                return

            if text:
                send_command(text)
        except KeyboardInterrupt:
            print("Quit")
            break
        except EOFError:
            print("Quit")
            break


if __name__ == '__main__':
    """
    TODO: 
        - wrap long lines in hgetall (and others) tables
        - use https://raw.githubusercontent.com/antirez/redis-doc/master/commands.json to get commands
        - use COMMAND to get command list to merge 
        - package
        - brew package
        - print tables for ZRANGE
        - .raw mode
        - ssh tunnel
        - client in receive mode !
            - multi
            - watch
            - subscribe to channels
            - pipe
        - connections ?
    """
    main()
