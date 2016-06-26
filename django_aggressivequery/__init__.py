# -*- coding:utf-8 -*-
import copy
import itertools
import sys
import json
import logging
from functools import partial
from django.db.models.fields import related
from django.db.models.fields import reverse_related
from django.db.models import Prefetch
from .functional import cached_property
from .structures import Pair
from . import extensions as ex
from . import extraction
logger = logging.getLogger(__name__)


class Inspector(object):
    def __init__(self, hintmap=None):
        self.hintmap = hintmap

    def depth(self, result, i=1):
        if not result.subresults:
            return i
        else:
            return max(self.depth(r, i + 1) for r in result.subresults)

    # todo: performance
    def collect_joins(self, result):
        # can join: one to one*, one* to one, many to one
        matched = {}
        for h in result.related:
            # custom_hint
            if hasattr(h, "type"):
                if type == ":join":
                    matched[h.name] = h
                continue
            if isinstance(h.field.field, (related.OneToOneField)):
                matched[h.name] = h
        for h in result.reverse_related:
            if isinstance(h.field.rel, (reverse_related.OneToOneRel, reverse_related.ManyToOneRel)):
                matched[h.name] = h
        for sr in result.subresults:
            if sr.name in matched:
                yield Pair(hint=matched[sr.name], result=sr)

    def collect_prefetch_list(self, result):
        matched = {}
        for h in result.related:
            # custom_hint
            if hasattr(h, "type"):
                if h.type == ":prefetch":
                    matched[h.name] = h
                continue
            if isinstance(h.field.field, (related.ManyToManyField, related.ForeignKey)):
                if not isinstance(h.field.field, related.OneToOneField):
                    matched[h.name] = h
        for h in result.reverse_related:
            if isinstance(h.field.rel, (reverse_related.ManyToOneRel, reverse_related.ManyToManyRel)):
                if not isinstance(h.field.rel, reverse_related.OneToOneRel):
                    matched[h.name] = h
        for sr in result.subresults:
            if sr.name in matched:
                yield Pair(hint=matched[sr.name], result=sr)

    def collect_selections(self, result):
        xs = [f.name for f in result.fields]
        ys = []
        related_mapping = {h.name: h.rel_model for h in result.related}
        for sr in result.subresults:
            if sr.name in related_mapping:
                # this is limitation. for django's compiler.
                sub_fields = (h.name for h in self.hintmap.iterator(related_mapping[sr.name], extraction.NOREL))
            else:
                sub_fields = self.collect_selections(sr)
            ys.append(["{}__{}".format(sr.name, name) for name in sub_fields])
        return itertools.chain(xs, *ys)

    def pp(self, result, out=sys.stdout):
        d = result.asdict()
        return out.write(json.dumps(d, indent=2))


# utilities
def reset_select_related(qs, join_targets):
    # remove all and set new settings
    new_qs = qs.select_related(None)
    if join_targets:
        new_qs = new_qs.select_related(*join_targets)
    logger.debug("@select_related: %r - %r", qs.model.__name__, join_targets)
    return new_qs


def reset_prefetch_related(qs, prefetch_targets):
    # remove all and set new settings
    new_qs = qs.prefetch_related(None)
    if prefetch_targets:
        new_qs = new_qs.prefetch_related(*prefetch_targets)
    logger.debug("@prefetch: %r - %r", qs.model.__name__, [{"through": p.prefetch_through, "query_model": p.queryset.model.__name__} for p in prefetch_targets])
    return new_qs


class QueryOptimizer(object):
    def __init__(self, transaction, enable_selections=True, extensions=None):
        self.transaction = transaction
        self.enable_selections = enable_selections
        self.extensions = extensions or ex.ExtensionRepository()

    @property
    def result(self):
        return self.transaction.result

    @property
    def inspector(self):
        return self.transaction.inspector

    def __copy__(self):
        return self.__class__(
            transaction=copy.copy(self.transaction),
            enable_selections=self.enable_selections,
            extensions=copy.copy(self.extensions)
        )

    def optimize(self, qs, result=None):
        result = result or self.result
        qs, lazy_prefetch_list = self._optimize_join(qs.all(), result)
        qs = self._optimize_prefetch(qs, result, lazy_prefetch_list=lazy_prefetch_list)
        qs = self._optimize_selections(qs, result)
        return qs

    def _optimize_selections(self, qs, result, name=None, externals=None):
        if not self.enable_selections:
            return qs
        fields = list(itertools.chain(self.inspector.collect_selections(result), externals or []))
        logger.debug("@selection, %r, %r", qs.model.__name__, fields)
        return qs.only(*fields)

    def _optimize_join(self, qs, result, name=None):
        # todo: nested
        lazy_join_list = list(self.collect_lazy_join_list_recursive(result, name=name))
        lazy_prefetch_list = []
        join_targets = []
        for lazy_join in lazy_join_list:
            join_targets.append(lazy_join())
            lazy_prefetch_list.extend(self.collect_lazy_prefetch_list_recusrive(lazy_join.result, name=lazy_join.name))
        return reset_select_related(qs, join_targets), lazy_prefetch_list

    def _optimize_prefetch(self, qs, result, name=None, lazy_prefetch_list=None):
        # todo: nested, settings filter lazy
        lazy_prefetch_list = lazy_prefetch_list or []
        lazy_prefetch_list.extend(self.collect_lazy_prefetch_list_recusrive(result, name=name))
        prefetch_targets = []
        extension_list = self.extensions.with_type(":prefetch")
        for lazy_prefetch in lazy_prefetch_list:
            if hasattr(lazy_prefetch.hint, "type") and lazy_prefetch.hint.type == ":prefetch":  # custom hint
                prefetch_qs, to_attr = lazy_prefetch.hint.value.queryset, lazy_prefetch.hint.name
                for extension in extension_list:
                    prefetch_qs = extension.apply(prefetch_qs, lazy_prefetch.name)
                # xxx: TODO: management lookup_name and to_attr name explicitly
                lazy_prefetch.name = lazy_prefetch.name.replace(lazy_prefetch.hint.name, lazy_prefetch.hint.value.prefetch_through)
            else:
                prefetch_qs, to_attr = lazy_prefetch.hint.rel_model.objects.all(), None  # default
                for extension in extension_list:
                    prefetch_qs = extension.apply(prefetch_qs, lazy_prefetch.name)

            prefetch_qs, sub_lazy_prefch = self._optimize_join(prefetch_qs, lazy_prefetch.result, name=lazy_prefetch.name)
            if not hasattr(lazy_prefetch.hint, "type") and lazy_prefetch.hint.rel_fk:
                prefetch_qs = self._optimize_selections(prefetch_qs, lazy_prefetch.result, externals=[lazy_prefetch.hint.rel_fk])
            else:
                prefetch_qs = self._optimize_selections(prefetch_qs, lazy_prefetch.result)
            prefetch_targets.append(lazy_prefetch(prefetch_qs, to_attr=to_attr))
        return reset_prefetch_related(qs, prefetch_targets)

    def collect_lazy_join_list_recursive(self, result, name=None):
        pairs = self.inspector.collect_joins(result)
        for h, sr in pairs:
            lazy_join = LazyJoin(h.name, h, sr)
            if name is not None:
                lazy_join = lazy_join.prefixed(name)
            yield lazy_join
            for sub_join in self.collect_lazy_join_list_recursive(sr):
                yield sub_join.prefixed(lazy_join.name)

    def collect_lazy_prefetch_list_recusrive(self, result, name=None):
        pairs = self.inspector.collect_prefetch_list(result)
        for h, sr in pairs:
            lazy_prefetch = LazyPrefetch(h.name, h, sr)
            if name is not None:
                lazy_prefetch = lazy_prefetch.prefixed(name)
            yield lazy_prefetch
            for sub_prefetch in self.collect_lazy_prefetch_list_recusrive(sr):
                yield sub_prefetch.prefixed(lazy_prefetch.name)

    def pp(self, result=None, out=sys.stdout):
        return self.inspector.pp(result or self.result, out=out)


class LazyPair(object):
    def __init__(self, name, hint, result):
        self.name = name
        self.hint = hint
        self.result = result

    def prefixed(self, prefix):
        return self.__class__(
            "{}__{}".format(prefix, self.name),
            self.hint, self.result
        )


class LazyJoin(LazyPair):
    def __call__(self):
        return self.name


class LazyPrefetch(LazyPair):
    def __call__(self, prefetch_qs, to_attr=None):
        return Prefetch(self.name, queryset=prefetch_qs, to_attr=to_attr)


class AggressiveQuery(object):
    def __init__(self, queryset, optimizer):
        self.source_queryset = queryset
        self.optimizer = optimizer

    def __copy__(self):
        return AggressiveQuery(
            self.source_queryset.all(),
            copy.copy(self.optimizer)
        )

    def _clone(self):
        return copy.copy(self)

    # implementing methods prefetch_filter, skip_filter as extension
    def __getattr__(self, k):
        extension = self.optimizer.extensions.with_name(k)
        return partial(extension.setup, self)

    @cached_property
    def aggressive_queryset(self):
        return self.optimizer.optimize(self.source_queryset)

    def to_queryset(self):
        return self.aggressive_queryset

    @property
    def query(self):
        return self.aggressive_queryset.query

    def __iter__(self):
        return iter(self.aggressive_queryset)

    def __getitem__(self, k):
        return self.aggressive_queryset[k]

    def pp(self, out=sys.stdout):
        return self.optimizer.pp(out=out)


class ExtractorTransaction(object):
    def __init__(self, qs, name_list, extractor=None, sorted=True):
        self.qs = qs
        self.name_list = name_list
        self.extractor = extractor or extraction.HintExtractor()

    def __copy__(self):
        return self.__class__(
            self.qs._clone(),
            copy.copy(self.name_list),
            copy.copy(self.extractor)
        )

    @cached_property
    def result(self):
        return self.extractor.extract(self.qs.model, self.name_list)

    @cached_property
    def inspector(self):
        return Inspector(self.extractor.hintmap)


default_hint_extractor = extraction.HintExtractor()

default_extension_repository = (
    ex.ExtensionRepository()
    .register(ex.PrefetchFilterExtension())
    .register(ex.SkipFieldsExtension())
    .register(ex.CustomPrefetchExtension())
)


# todo: cache
def from_queryset(qs, name_list, more_specific=False,
                  extensions=default_extension_repository):
    logger.debug("name_list: %s", name_list)
    if not isinstance(name_list, (tuple, list)):
        raise ValueError("name list is only tuple or list type. (['attr'] rather than 'attr')")
    qs = qs.all() if not hasattr(qs, "_clone") else qs
    specific_list = name_list if more_specific else include_star_selection(name_list)
    ex_transaction = ExtractorTransaction(qs, specific_list)
    optimizer = QueryOptimizer(ex_transaction, enable_selections=more_specific, extensions=extensions)
    return AggressiveQuery(qs, optimizer)


def include_star_selection(name_list):
    star_list = ["*"]
    for s in name_list:
        xs = s.split("__")
        for i in range(1, len(xs) + 1):
            star_list.append("{}__*".format("__".join(xs[:i])))
    return star_list
