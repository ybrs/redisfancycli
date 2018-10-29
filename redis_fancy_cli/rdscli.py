from __future__ import unicode_literals
from prompt_toolkit import prompt, ANSI
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
logger = None


def debug(s, *args):
    logger.debug(s + '%s ' * len(args), *args)


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
        return s.decode('utf-8')
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
        if cmd and cmd[0].upper().startswith('SELECT '):
            return self
        return current_state

    def process_command(self, cmd):
        self.next_db = cmd.split(' ')[-1]
        return cmd

    def process_reply(self, resp):
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


class HgetallState(State):
    def when(self, cmd, current_state):
        if cmd and cmd[0].upper().startswith('HGETALL'):
            return self
        return current_state

    def process_command(self, cmd):
        return cmd

    def process_reply(self, resp):
        data = force_unicode(resp)
        debug('hgetall ', data)
        values = []
        headers = ['key', 'value']
        i = iter(data)
        for k, v in zip(i, i):
            values.append([k, v])

        formatter = TabularOutputFormatter()
        self.client.set_state(self.client.avail_states[0])
        return '\n'.join([ln for ln in formatter.format_output(values, headers, format_name='fancy_grid')])


class Client(object):
    def __init__(self, rds):
        self.rds = rds
        self.selected_db = 0
        self.default_state = DefaultState(self)

        self.avail_states = [
            self.default_state,
            SelectState(self),
            InfoState(self),
            HgetallState(self),
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

    def read_response(self):
        resp = self.rds.read_response()
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
    except:
        if isinstance(resp, bytes):
            logger.critical("got error on response %s", force_unicode(resp.decode()))
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
                yield Completion(k, start_position=-len(word_before_cursor))

        elif ' ' not in document.text:
            for k in REDIS_COMMANDS:
                if k.lower().startswith(document.text.lower()):
                    yield Completion(k, start_position=-len(word_before_cursor))
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

                self.client.rds.send_command('keys *{}*'.format(word_before_cursor))
                resp = self.client.rds.read_response()
                for k in resp:
                    yield Completion(force_unicode(k), start_position=-len(word_before_cursor))

@click.command()
@click.option("--host", '-h', default="127.0.0.1", help="Host")
@click.option("--port", '-p', default=6379, help="Host")
@click.option("--database", '-d', default=0, help="Database")
def main(host, port, database):
    global logger
    config = get_config()

    logging.basicConfig(filename=config['log_file'], level=logging.getLevelName(config['log_level']))
    logger = logging.getLogger(__name__)

    style = style_from_pygments_cls(get_style_by_name('monokai'))
    session = PromptSession(history=FileHistory(config['history_file']))

    def bottom_toolbar():
        return HTML('127.0.0.1:6379 db:{} keys:{}'.format(client.selected_db, client.keycount()))

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

    rds = redis.Connection(host=host, port=int(port), db=database)
    rds.connect()

    client = Client(rds=rds)
    rds_completer = RedisCompleter()
    rds_completer.set_client(client)
    completer = merge_completers([rds_completer])

    multiline = False

    while True:
        try:
            text = session.prompt('> ',
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

            if text.upper() == 'QUIT':
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
        - package
        - brew package
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
