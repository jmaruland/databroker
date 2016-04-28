from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import six
from pkg_resources import resource_filename
from contextlib import contextmanager
import json
import logging
import os.path
import shutil

from pymongo import MongoClient

import boltons.cacheutils

from .handlers_base import DuplicateHandler
import os

from . import core
from . import core_v0

_API_MAP = {0: core_v0,
            1: core}


logger = logging.getLogger(__name__)

try:
    from collections import ChainMap as _ChainMap
except ImportError:
    class _ChainMap(object):
        def __init__(self, primary, fallback=None):
            if fallback is None:
                fallback = {}
            self.fallback = fallback
            self.primary = primary

        def __getitem__(self, k):
            try:
                return self.primary[k]
            except KeyError:
                return self.fallback[k]

        def __setitem__(self, k, v):
            self.primary[k] = v

        def __contains__(self, k):
            return k in self.primary or k in self.fallback

        def __delitem__(self, k):
            del self.primary[k]

        def pop(self, k, v):
            return self.primary.pop(k, v)

        @property
        def maps(self):
            return [self.primary, self.fallback]

        @property
        def parents(self):
            return self.fallback

        def new_child(self, m=None):
            if m is None:
                m = {}

            return _ChainMap(m, self)


class FileStoreRO(object):

    KNOWN_SPEC = dict()
    # load the built-in schema
    for spec_name in ['AD_HDF5', 'AD_SPE']:
        tmp_dict = {}
        resource_name = 'json/{}_resource.json'.format(spec_name)
        datum_name = 'json/{}_datum.json'.format(spec_name)
        with open(resource_filename('filestore', resource_name), 'r') as fin:
            tmp_dict['resource'] = json.load(fin)
        with open(resource_filename('filestore', datum_name), 'r') as fin:
            tmp_dict['datum'] = json.load(fin)
        KNOWN_SPEC[spec_name] = tmp_dict

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, val):
        if self._api is not None:
            raise RuntimeError("Can not change api version at runtime")
        self._api = _API_MAP[val]
        self._version = val

    def __init__(self, config, handler_reg=None, version=1):
        self.config = config
        self._api = None
        self.version = version
        if handler_reg is None:
            handler_reg = {}

        self.handler_reg = _ChainMap(handler_reg)

        self._datum_cache = boltons.cacheutils.LRU(max_size=1000000)
        self._handler_cache = boltons.cacheutils.LRU()
        self._resource_cache = boltons.cacheutils.LRU(on_miss=self._r_on_miss)
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None
        self.__res_update_col = None
        self.known_spec = dict(self.KNOWN_SPEC)

    def disconnect(self):
        self.__db = None
        self.__conn = None
        self.__datum_col = None
        self.__res_col = None

    def reconfigure(self, config):
        self.disconnect()
        self.config = config

    def _r_on_miss(self, k):
        col = self._resource_col
        if self.version == 0:
            ret = col.find_one({'_id': k})
        elif self.version == 1:
            ret = col.find_one({'uid': k})
        return ret

    def resource_given_uid(self, uid):
        col = self._resource_col
        return self._api.resource_given_uid(col, uid)

    def get_datum(self, eid):
        return self._api.get_datum(self._datum_col, eid,
                                   self._datum_cache, self.get_spec_handler,
                                   logger)

    def register_handler(self, key, handler, overwrite=False):
        if (not overwrite) and (key in self.handler_reg):
            if self.handler_reg[key] is handler:
                return
            raise DuplicateHandler(
                "You are trying to register a second handler "
                "for spec {}, {}".format(key, self))

        self.deregister_handler(key)
        self.handler_reg[key] = handler

    def deregister_handler(self, key):
        handler = self.handler_reg.pop(key, None)
        if handler is not None:
            name = handler.__name__
            for k in list(self._handler_cache):
                if k[1] == name:
                    del self._handler_cache[k]

    @contextmanager
    def handler_context(self, temp_handlers):
        stash = self.handler_reg
        self.handler_reg = self.handler_reg.new_child(temp_handlers)
        try:
            yield self
        finally:
            popped_reg = self.handler_reg.maps[0]
            self.handler_reg = stash
            for handler in popped_reg.values():
                name = handler.__name__
                for k in list(self._handler_cache):
                    if k[1] == name:
                        del self._handler_cache[k]

    @property
    def _db(self):
        if self.__db is None:
            conn = self._connection
            self.__db = conn.get_database(self.config['database'])
        return self.__db

    @property
    def _resource_col(self):
        if self.__res_col is None:
            self.__res_col = self._db.get_collection('resource')
            self.__res_col.create_index('resource_id')

        return self.__res_col

    @property
    def _resource_update_col(self):
        if self.__res_update_col is None:
            self.__res_update_col = self._db.get_collection('resource_update')
            self.__res_update_col.create_index('resource')

        return self.__res_update_col

    @property
    def _datum_col(self):
        if self.__datum_col is None:
            self.__datum_col = self._db.get_collection('datum')
            self.__datum_col.create_index('datum_id', unique=True)
            self.__datum_col.create_index('resource')

        return self.__datum_col

    @property
    def _connection(self):
        if self.__conn is None:
            self.__conn = MongoClient(self.config['host'],
                                      self.config.get('port', None))
        return self.__conn

    def get_spec_handler(self, resource):
        """
        Given a document from the base FS collection return
        the proper Handler

        This should get memozied or shoved into a class eventually
        to minimize open/close thrashing.

        Parameters
        ----------
        resource : ObjectId
            ObjectId of a resource document

        Returns
        -------

        handler : callable
            An object that when called with the values in the event
            document returns the externally stored data

        """
        resource = self._resource_cache[resource]

        h_cache = self._handler_cache

        spec = resource['spec']
        handler = self.handler_reg[spec]
        if self.version == 0:
            key = (str(resource['_id']), handler.__name__)
        elif self.version == 1:
            key = (str(resource['uid']), handler.__name__)

        try:
            return h_cache[key]
        except KeyError:
            pass

        kwargs = resource['resource_kwargs']
        rpath = resource['resource_path']
        chroot = resource.get('chroot', '')
        rpath = os.path.join(chroot, rpath)
        ret = handler(rpath, **kwargs)
        h_cache[key] = ret
        return ret


class FileStore(FileStoreRO):
    def insert_resource(self, spec, resource_path, resource_kwargs, chroot=''):
        col = self._resource_col

        return self._api.insert_resource(col, spec, resource_path,
                                         resource_kwargs,
                                         self.known_spec,
                                         chroot=chroot)

    def insert_datum(self, resource, datum_id, datum_kwargs):
        col = self._datum_col
        if datum_kwargs is None:
            datum_kwargs = {}

        return self._api.insert_datum(col, resource, datum_id, datum_kwargs,
                                      self.known_spec, self._resource_col)

    def bulk_insert_datum(self, resource, datum_ids, datum_kwarg_list):
        col = self._datum_col

        return self._api.bulk_insert_datum(col, resource, datum_ids,
                                           datum_kwarg_list)

    def shift_chroot(self, resource, shift):
        if self.version == 0:
            raise NotImplementedError('V0 has no notion of chroot')

        if shift == 0:
            return resource

        def safe_join(inp):
            if not inp:
                return ''
            return os.path.join(*inp)
        actual_resource = self.resource_given_uid(resource)
        if not isinstance(resource, six.string_types):
            if dict(actual_resource) != dict(resource):
                raise RuntimeError('The resource you hold and the resource '
                                   'the data base holds do not match '
                                   'yours: {!r} db: {!r}'.format(
                                       resource, actual_resource))
        resource = actual_resource
        abs_path = resource['chroot'][0] == os.sep
        chroot = [_ for _ in resource['chroot'].split(os.sep) if _]
        rpath = [_ for _ in resource['resource_path'].split(os.sep) if _]

        if shift > 0:
            # to the right
            if shift > len(rpath):
                raise RuntimeError('Asked to shift farther to right '
                                   'than there are directories')
            new_chroot = safe_join(chroot + rpath[:shift])
            new_rpath = safe_join(rpath[shift:])
        else:
            # sometime to the left
            shift = len(chroot) + shift
            if shift < 0:
                raise RuntimeError('Asked to shift farther to left '
                                   'than there are directories')
            new_chroot = safe_join(chroot[:shift])
            new_rpath = safe_join((chroot[shift:] + rpath))
        if abs_path:
            new_chroot = os.sep + new_chroot

        new = dict(resource)
        new['chroot'] = new_chroot
        new['resource_path'] = new_rpath

        update_col = self._resource_update_col
        resource_col = self._resource_col
        return self._api.update_resource(update_col, resource_col,
                                         resource, new)


class FileStoreMoving(FileStore):
    def change_chroot(self, resource_or_uid, new_chroot, remove_origin=True,
                      verify=True):
        datum_col = self._datum_col
        # get list of files
        resource = self.resource_given_uid(resource_or_uid)
        handler = self.get_spec_handler(resource['uid'])

        datum_gen = self._api.get_datumkw_by_resuid_gen(datum_col,
                                                        resource['uid'])
        file_list = handler.get_file_list(datum_gen)

        # check that all files share the same chroot
        old_chroot = resource['chroot']
        for f in file_list:
            if not f.startswith(old_chroot):
                raise RuntimeError('something is very wrong, the files '
                                   'do not all share the same root, ABORT')

        # sort out where new files should go
        new_file_list = [os.path.join(new_chroot,
                                      os.path.relpath(f, old_chroot))
                         for f in file_list]
        # copy the files to the new location
        for fin, fout in zip(file_list, new_file_list):
            # copy files
            print(fin, fout)
            os.makedirs(os.path.dirname(fout), exist_ok=True)
            shutil.copy2(fin, fout)

        # update the database
        new_resource = dict(resource)
        new_resource['chroot'] = new_chroot

        update_col = self._resource_update_col
        resource_col = self._resource_col
        ret = self._api.update_resource(update_col, resource_col,
                                        resource, new_resource)

        # remove original files
        if remove_origin:
            for f in file_list:
                os.unlink(f)

        # nuke caches
        uid = resource['uid']
        self._resource_cache.pop(uid, None)
        for k in list(self._handler_cache):
            if k[0] == uid:
                del self._handler_cache[k]


        return ret
