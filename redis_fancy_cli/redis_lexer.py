import re

from pygments.formatters.terminal256 import Terminal256Formatter
from pygments.lexer import RegexLexer, words
from pygments.token import Text, Keyword, Name, String, Number
from pygments.lexers.scripting import LuaLexer
from pygments.lexer import RegexLexer, bygroups, using

"""
set somejson '{
    "echo": "bar"
}'

"""

class RedisLexer(RegexLexer):
    """
    Lexer for Redis.
    """

    name = 'Redis'
    aliases = ['redis']

    flags = re.IGNORECASE
    tokens = {
        'root': [
            (r'\s+', Text),
            (words((
        'APPEND', 'AUTH', 'BGREWRITEAOF', 'BGSAVE', 'BITCOUNT', 'BITFIELD', 'BITOP', 'BITPOS',
        'BLPOP', 'BRPOP', 'BRPOPLPUSH', 'CLIENT KILL', 'CLIENT LIST', 'CLIENT GETNAME', 'CLIENT PAUSE', 'CLIENT REPLY',
        'CLIENT SETNAME', 'CLUSTER ADDSLOTS', 'CLUSTER COUNT-FAILURE-REPORTS', 'CLUSTER COUNTKEYSINSLOT',
        'CLUSTER DELSLOTS', 'CLUSTER FAILOVER', 'CLUSTER FORGET', 'CLUSTER GETKEYSINSLOT',
        'CLUSTER INFO', 'CLUSTER KEYSLOT', 'CLUSTER MEET', 'CLUSTER NODES', 'CLUSTER REPLICATE',
        'CLUSTER RESET', 'CLUSTER SAVECONFIG', 'CLUSTER SET-CONFIG-EPOCH',
        'CLUSTER SETSLOT', 'CLUSTER SLAVES', 'CLUSTER SLOTS', 'COMMAND', 
        'COMMAND COUNT', 'COMMAND GETKEYS', 'COMMAND INFO', 'CONFIG GET',
        'CONFIG REWRITE', 'CONFIG SET', 'CONFIG RESETSTAT', 'DBSIZE', 'DEBUG OBJECT', 'DEBUG SEGFAULT', 'DECR', 'DECRBY',
        'DEL', 'DISCARD', 'DUMP', 'ECHO', 'EVALSHA', 'EXEC', 'EXISTS',
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
        'ZREVRANGEBYSCORE', 'ZREVRANK', 'ZSCORE', 'ZUNIONSTORE', 'SCAN', 'SSCAN', 'HSCAN', 'ZSCAN' ), suffix=r'\b'),
             Keyword),

            (r'EVAL \"', String.Single, ('string-lua', )),
            (r'EVAL \'', String.Single, ('string-lua2',)),

            (r'[0-9]+', Number.Integer),
            (r"'(''|[^'])*'", String.Single),
            (r'"(""|[^"])*"', String.Double),
            (r'[a-z_][\w$]*', Name)
        ],
        'string-lua2': [
            (r'(.+)((?<!\\)\')',
             bygroups(using(LuaLexer), String.Single),
            )
        ],
        'string-lua': [
            (r'(.+)((?<!\\)\")',
             bygroups(using(LuaLexer), String.Single),
            )
        ]
    }


def tokenize_redis_command(s):
    args = []
    buf = []
    l = len(s)
    cnt = 0

    def buffer_until(remainder, char):
        buf = []
        c = 0
        while c < len(remainder):
            k = remainder[c]
            if k == char:
                if remainder[c-1] != '\\':
                    return (''.join(buf), c+2)
            buf.append(k)
            c += 1
        return (''.join(buf), c+2)

    while cnt < l:
        k = s[cnt]
        if k == '"' or k == "'":
            b, c = buffer_until(s[cnt+1:], k)
            cnt = cnt + c
            args.append(b)
        elif k == ' ':
            args.append(''.join(buf))
            buf = []
        else:
            buf.append(k)

        cnt += 1
    if buf:
        args.append(''.join(buf))

    return args

if __name__ == '__main__':
    from pygments import highlight
    import re
    code = '''eval 'return redis.call("get","foo")' 0 '''

    test_cases = [
        # ('''set foo bar''', ['set', 'foo', 'bar']),
        # ('''set foo bar baz!''', ['set', 'foo', 'bar', 'baz!']),
        # ('''set "foo x" bar''', ['set', 'foo x', 'bar']),
        # ('''set "foo x" "bar"''', ['set', 'foo x', 'bar']),
        # ('''set "foo x " "bar''', ['set', 'foo x ', 'bar']),
        # (r'set "foo \"xyz\" " "bar', ['set', 'foo \\"xyz\\" ', 'bar']),
        # (r"set 'foo \'xyz\' ' 'bar'", ['set', "foo \\'xyz\\' ", 'bar']),

        # test multiline
        ("""set foo xyz '{
            "hello": "bar"
        }' """, ['set', "foo", "xyz",
                               r'''{
            "hello": "bar"
        }''']),

    ]

    for k, v in test_cases:
        assert tokenize_redis_command(k) == v




    # print(highlight(code, RedisLexer(), Terminal256Formatter()))
    #
    # code = '''eval "return redis.call(\"get\") return x" 0 ZSCORE'''
    # print(re.match(r'(.+)((?<!\\)\")', code).groups())
    # print(highlight(code, RedisLexer(), Terminal256Formatter()))
