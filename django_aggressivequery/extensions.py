# -*- coding:utf-8 -*-
import copy
import functools
from collections import defaultdict

# extension type
extension_types = [":prefetch", ":selecting", ":join"]


class ExtensionRepository(object):
    def __init__(self, type_map=None, name_map=None):
        self.type_map = type_map or defaultdict(list)
        self.name_map = name_map or {}

    def __copy__(self):
        return self.__class__(type_map=copy.copy(self.type_map), name_map=copy.copy(self.name_map))

    def register(self, extension, override=False):
        if not override and extension.name in self.name_map:
            raise ValueError("extension '{}' is already registered".format(extension.name))

        self.type_map[extension.type].append(extension)
        self.name_map[extension.name] = extension
        return self

    def with_name(self, name):
        return self.name_map[name]

    def with_type(self, type_):
        return self.type_map[type_]


class Extension(object):
    name = None
    type = None

    def setup(self, aqs):
        raise NotImplementedError("setup")

    def apply(self, aqs):
        raise NotImplementedError("apply")


class OnPrefetchExtension(Extension):
    type = ":prefetch"


class OnJoinExtension(Extension):
    type = ":join"


class OnSelectingExtension(Extension):
    type = ":select"


class PrefetchFilterExtension(OnPrefetchExtension):
    """adding filter on prefetched query"""
    name = "prefetch_filter"

    def __init__(self, filters=None):
        self.filters = filters or defaultdict(list)

    def __copy__(self):
        return self.__class__(filters=copy.copy(self.filters))

    def setup(self, aqs, **conditions):
        new_qs = aqs._clone()
        new_extension = new_qs.optimizer.extensions.with_name(self.name)
        for name, filter_fn in conditions.items():
            new_extension.filters[name].append(filter_fn)
        return new_qs

    def apply(self, prefetch_qs, name):
        filters = self.filters[name]
        return functools.reduce(lambda qs, f: f(qs), filters, prefetch_qs)


class CustomPrefetchExtension(OnPrefetchExtension):
    """like a Prefetch(<name>, <queryset>, to_attr=<attrname>)"""
    name = "custom_prefetch"

    def __init__(self, prefetch):
        self.prefetch = prefetch

    def __call__(self, qs):
        self.qs.prefetch


default_extension_repository = (
    ExtensionRepository()
    .register(PrefetchFilterExtension())
)
