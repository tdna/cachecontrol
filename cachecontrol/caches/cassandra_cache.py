from __future__ import division

from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


def total_seconds(td):
    """Python 2.6 compatability"""
    if hasattr(td, 'total_seconds'):
        return td.total_seconds()

    ms = td.microseconds
    secs = (td.seconds + td.days * 24 * 3600)
    return int((ms + secs * 10**6) / 10**6)


class Cache(Model):
    key = columns.Text(primary_key=True)
    val = columns.Text()
    cached_at = columns.DateTime()


class CassandraCache(object):

    def __init__(self, session):
        self.session = session

    def get(self, key):
        res = Cache.objects(key=key)
        return dict(res.get()) if res else None

    def set(self, key, value, expires=None):
        cache = Cache(key=key, val=value, cached_at=datetime.now())

        if not expires:
            cache.save()
        else:
            ttl = total_seconds(expires - datetime.now())
            ttl = 1 if ttl < 0 else ttl
            cache.ttl(ttl).save()

    def delete(self, key):
        Cache.objects(key=key).delete()

    def clear(self):
        """Helper for clearing all the keys in a database. Use with
        caution!"""
        self.session.execute('TRUNCATE cache')

    def close(self):
        self.session.shutdown()
