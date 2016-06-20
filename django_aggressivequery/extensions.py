# -*- coding:utf-8 -*-
import copy
import functools
from collections import defaultdict
from .functional import cached_property
from .structures import excluded_result, dict_from_keys, CustomHint

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

    def get_self_from_aqs(self, aqs):
        return aqs.optimizer.extensions.with_name(self.name)


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

    def setup(self, aqs, skip_list):
        if not isinstance(skip_list, (tuple, list)):
            raise ValueError("skip_list is only tuple or list type. (['name'] rather than 'name')")

        new_aqs = aqs._clone()
        new_aqs.optimizer = _FilteredQueryOptimizer(new_aqs.optimizer, skip_list)
        return new_aqs


class _FilteredQueryOptimizer(object):
    """decorator object for QueryOptimizer"""
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
        new_aqs = aqs._clone()
        new_extension = self.get_self_from_aqs(new_aqs)
        for name, filter_fn in conditions.items():
            new_extension.filters[name].append(filter_fn)
        return new_aqs

    def apply(self, prefetch_qs, name):
        filters = self.filters[name]
        return functools.reduce(lambda qs, f: f(qs), filters, prefetch_qs)


class CustomPrefetchExtension(WrappingExtension):
    """like a Prefetch(<name>, <queryset>, to_attr=<attrname>)"""
    name = "custom_prefetch"

    def __init__(self, prefetchs=None):
        self.prefetchs = prefetchs or {}

    def __copy__(self):
        return self.__class__(prefetchs=copy.copy(self.prefetchs))

    def setup(self, aqs, **prefetchs):
        new_aqs = aqs._clone()
        new_extractor = new_aqs.optimizer.transaction.extractor
        new_extractor.hintmap = _CustomPrefetchHintMap(new_extractor.hintmap, prefetchs)
        new_extension = self.get_self_from_aqs(new_aqs)
        for name, prefetch in prefetchs.items():
            if not prefetch.to_attr:
                raise ValueError("{}: custom_prefetch required a Prefetch object with `to_attr` option".format(name))
            if prefetch.to_attr != name.rsplit("__", 1)[-1]:
                raise ValueError("{}: custom_prefetch suffix is mismatch {} != {}".format(name, prefetch.to_attr, name))
            new_extension.prefetchs[name] = prefetch
        return new_aqs

    def __call__(self, qs):
        self.qs.prefetch


class _CustomPrefetchHintIterator(object):
    def __init__(self, hintmap, history, iterator):
        self.hintmap = hintmap  # _CustomPrefetchHintMap
        self.history = history
        self.iterator = iterator

    def clone(self, tokens):
        return self.__class__(self.hintmap, self.history, self.iterator.clone(tokens))

    def __iter__(self):
        for v in self.iterator:
            yield v
        itr2 = self.custom_iterator()
        if itr2:
            for v in itr2:
                yield v

    def custom_iterator(self):
        prefix_list = self.history[1:]
        suffixes = self.hintmap.suffixes
        for t in self.iterator.tokens:
            if t in self.iterator.history:
                continue
            self.iterator.history.add(t)

            if t.endswith(suffixes):
                prefix_list.append(t)
                full_name = "__".join(prefix_list)
                prefix_list.pop()
                if full_name in self.hintmap.prefetchs:
                    prefetch = self.hintmap.prefetchs[full_name]
                    rel_model = prefetch.queryset.model
                    hint = CustomHint(name=t,
                                      is_relation=True,
                                      value=prefetch,
                                      rel_model=rel_model,
                                      rel_name=t,
                                      is_reverse_related=False,
                                      type=":prefetch")
                    yield hint, False


class _CustomPrefetchHintMap(object):
    """decorator object for HintMap"""
    iterator_cls = _CustomPrefetchHintIterator

    def __init__(self, hintmap, prefetchs):
        self.hintmap = hintmap
        self.prefetchs = prefetchs
        self.suffixes = tuple(k.rsplit("__", 1)[-1] for k in prefetchs.keys())

    def __getattr__(self, k):
        return getattr(self.hintmap, k)

    def iterator(self, model, tokens, history=None):
        return self.iterator_cls(self, history, self.hintmap.iterator(model, tokens, history=history))
