from pathlib import Path

from tiled.trees.files import Tree as FileTree
from suitcase.mongo_normalized import Serializer

from .mongo_normalized import Tree as MongoNormalizedTree


class JSONLReader:

    specs = ["BlueskyRun"]

    def __init__(self, tree):
        self._tree = tree
        database = tree.database
        self._mongo_normalized_serializer = Serializer(database, database)

    def consume_file(self, filepath):
        import json

        with open(filepath) as file:
            lines = iter(file)
            name, doc = json.loads(next(lines))
            if name != "start":
                raise ValueError("File is expected to start with ('start', {...})")
            uid = doc["uid"]
            self._mongo_normalized_serializer(name, doc)
            for line in lines:
                name, doc = json.loads(line)
                self._mongo_normalized_serializer(name, doc)
        return self._tree[uid]


class MsgpackReader:

    specs = ["BlueskyRun"]

    def __init__(self, tree):
        self._tree = tree
        database = tree.database
        self._mongo_normalized_serializer = Serializer(database, database)

    def consume_file(self, filepath):
        import msgpack

        with open(filepath, "rb") as file:
            with msgpack.Unpacker(file) as items:
                name, doc = next(items)
                if name != "start":
                    raise ValueError("File is expected to start with ('start', {...})")
                uid = doc["uid"]
                self._mongo_normalized_serializer(name, doc)
                for name, doc in items:
                    self._mongo_normalized_serializer(name, doc)
        return self._tree[uid]


def key_from_filename(filename):
    "'blah.jsonl' -> 'blah'"
    return Path(filename).stem


class JSONLTree(FileTree):

    # This is set up in Tree.from_directory.
    DEFAULT_READERS_BY_MIMETYPE = {}

    specs = ["CatalogOfBlueskyRuns"]

    @classmethod
    def from_directory(cls, directory, **kwargs):

        tree = MongoNormalizedTree.from_mongomock(**kwargs)
        jsonl_reader = JSONLReader(tree)
        mimetypes_by_file_ext = {
            ".jsonl": "application/x-bluesky-jsonl",
        }
        readers_by_mimetype = {
            "application/x-bluesky-jsonl": jsonl_reader.consume_file,
        }
        return super().from_directory(
            directory,
            readers_by_mimetype=readers_by_mimetype,
            mimetypes_by_file_ext=mimetypes_by_file_ext,
            key_from_filename=key_from_filename,
            error_if_missing=False,
            tree=tree,
        )

    def __init__(self, *args, tree, **kwargs):
        self._tree = tree
        super().__init__(*args, **kwargs)

    @property
    def database(self):
        return self._tree.database

    def get_serializer(self):
        import event_model
        import time
        from suitcase.jsonl import Serializer

        tree = self  # since 'self' is shadowed below to mean Serializer's self

        class SerializerWithUpdater(Serializer):
            def stop(self, doc):
                super().stop(doc)
                # Blocks until this new Run is processed by the tree.
                tree.update_now()
                time.sleep(3)

        def factory(name, doc):
            serializer = SerializerWithUpdater(self.directory)
            return [serializer], []

        rr = event_model.RunRouter([factory])
        return rr

    def new_variation(self, **kwargs):
        return super().new_variation(tree=self._tree, **kwargs)

    def search(self, *args, **kwargs):
        return self._tree.search(*args, **kwargs)


class MsgpackTree(FileTree):

    # This is set up in Tree.from_directory.
    DEFAULT_READERS_BY_MIMETYPE = {}

    specs = ["CatalogOfBlueskyRuns"]

    @classmethod
    def from_directory(cls, directory, **kwargs):

        tree = MongoNormalizedTree.from_mongomock(**kwargs)
        msgpack_reader = MsgpackReader(tree)
        mimetypes_by_file_ext = {
            ".msgpack": "application/x-bluesky-msgpack",
        }
        readers_by_mimetype = {
            "application/x-bluesky-msgpack": msgpack_reader.consume_file,
        }
        return super().from_directory(
            directory,
            readers_by_mimetype=readers_by_mimetype,
            mimetypes_by_file_ext=mimetypes_by_file_ext,
            key_from_filename=key_from_filename,
            error_if_missing=False,
            tree=tree,
        )

    def __init__(self, *args, tree, **kwargs):
        self._tree = tree
        super().__init__(*args, **kwargs)

    @property
    def database(self):
        return self._tree.database

    def get_serializer(self):
        import time
        import event_model
        from suitcase.msgpack import Serializer

        tree = self  # since 'self' is shadowed below to mean Serializer's self

        class SerializerWithUpdater(Serializer):
            def stop(self, doc):
                super().stop(doc)
                # Blocks until this new Run is processed by the tree.
                tree.update_now()
                time.sleep(3)

        def factory(name, doc):
            serializer = SerializerWithUpdater(self.directory)
            return [serializer], []

        rr = event_model.RunRouter([factory])
        return rr

    def new_variation(self, **kwargs):
        return super().new_variation(tree=self._tree, **kwargs)

    def search(self, *args, **kwargs):
        return self._tree.search(*args, **kwargs)
