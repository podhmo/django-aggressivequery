# -*- coding:utf-8 -*-
import copy
import functools
from collections import defaultdict
from .functional import cached_property
from .structures import excluded_result, dict_from_keys

# extension type
extension_types = [":prefetch", ":selecting", ":join", ":wrap"]
# TODO: apply extension (:selecting, :join)


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


class WrappingExtension(Extension):
    type = ":wrap"


class SkipFieldsExtension(WrappingExtension):
    """skipping needless fields"""
    name = "skip_filter"

    def setup(self, aqs, skips):
        new_query = aqs._clone()
        new_query.optimizer = _FilteredQueryOptimizer(new_query.optimizer, skips)
        return new_query


class _FilteredQueryOptimizer(object):
    def __init__(self, optimizer, skips):
        self._optimizer = optimizer
        self.skips = skips

    def __getattr__(self, k):
        return getattr(self._optimizer, k)

    def __copy__(self):
        return self.__class__(copy.copy(self._optimizer), self.skips)

    def optimize(self, qs, result=None):
        return self._optimizer.optimize(qs, result or self.result)

    @cached_property
    def result(self):
        return excluded_result(self._optimizer.result, dict_from_keys(self.skips))


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
