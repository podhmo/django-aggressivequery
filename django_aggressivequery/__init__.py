# -*- coding:utf-8 -*-
import copy
import functools
import itertools
import sys
import json
import logging
import django
from collections import defaultdict, OrderedDict
from django.utils.functional import cached_property
from django.db.models.fields import related
from django.db.models.fields import reverse_related
from django.db.models import Prefetch
from .structures import Hint, TmpResult, Result, Pair


logger = logging.getLogger(__name__)

NOREL = ":norel:"
REL = ":rel:"
ALL = ":all:"


class HintIterator(object):
    def __init__(self, d, tokens, history=None):
        self.d = d
        self.tokens = tokens
        self.history = history or set()

    def parse_token(self, t):
        if t == ALL:
            for s in self.d.values():
                yield s, False
        elif t == REL:
            for s in self.d.values():
                if s.is_relation:
                    yield s, False
        elif t == NOREL:
            for s in self.d.values():
                if not s.is_relation:
                    yield s, False
        else:
            s = self.d.get(t)
            if s is not None:
                yield s, True

    def clone(self, tokens):
        return self.__class__(self.d, tokens)

    def __iter__(self):
        for t in self.tokens:
            for s, selected in self.parse_token(t):
                if s not in self.history:
                    self.history.add(s)
                    yield s, selected


class HintMap(object):
    iterator_cls = HintIterator

    def __init__(self):
        self.cache = {}  # Dict[model, Hint]

    def extract(self, model):
        d = OrderedDict()
        for f in get_all_fields(model):
            name, is_relation, defer_name = f.name, f.is_relation, getattr(f, "attname", None)
            rel_name = None
            is_reverse_related = False
            rel_model = None
            rel_fk = None
            if is_relation:
                if hasattr(f, "rel_class"):
                    is_reverse_related = True
                    rel_name = f.rel.get_accessor_name()
                    rel_model = f.rel.model
                    rel_fk = getattr(f.rel, "attname", None)
                else:
                    rel_name = f.field.name
                    rel_model = f.field.model
                    rel_fk = getattr(f.field, "attname", None)
            d[f.name] = Hint(name=name,
                             is_relation=is_relation,
                             is_reverse_related=is_reverse_related,
                             rel_name=rel_name,
                             rel_model=rel_model,
                             rel_fk=rel_fk,
                             defer_name=defer_name,  # hmm
                             field=f)
            # hmm. supporting accessor_name? (e.g. `customerposition_set`)
            if hasattr(f, "get_accessor_name"):
                d[f.get_accessor_name()] = d[f.name]
        return d

    def load(self, model):
        d = self.cache.get(model)
        if d is None:
            d = self.cache[model] = self.extract(model)
        return d

    def iterator(self, model, tokens):
        return self.iterator_cls(self.load(model), tokens)


class HintExtractor(object):
    ALL = "*"

    def __init__(self, sorted=True):
        # TODO
        self.sorted = sorted
        self.bidirectional = False
        self.hintmap = HintMap()

    def extract(self, model, name_list):
        backref = set()
        backref.add((model, ""))
        tmp_result = self.drilldown(model, name_list, backref=backref, parent_name="")
        return self.classify(tmp_result)

    def seq(self, seq, key):
        return sorted(seq, key=key) if self.sorted else seq

    def classify(self, tmp_result):
        result = Result(name=tmp_result.name,
                        fields=[],
                        related=[],
                        reverse_related=[],
                        foreign_keys=[],
                        subresults=[])
        for h in self.seq(tmp_result.hints, key=lambda h: h.name):
            if not h.is_relation:
                result.fields.append(h)
                continue

            if h.defer_name is not None and not h.field.many_to_many:
                result.foreign_keys.append(h.defer_name)

            if h.is_reverse_related:
                result.reverse_related.append(h)
            else:
                result.related.append(h)

        for sr in self.seq(tmp_result.subresults, key=lambda r: r.name):
            result.subresults.append(self.classify(sr))
        return result

    def drilldown(self, model, name_list, backref, parent_name, indent=0):
        logger.info("%s name=%r model=%r %r", " " * (indent + indent), parent_name, model.__name__, name_list)
        hints = []
        names = []
        rels = defaultdict(list)
        for name in name_list:
            if name == self.ALL:
                names.append(NOREL)
            elif "__" not in name:
                names.append(name)
            else:
                prefix, sub_name = name.split("__", 1)
                rels[prefix].append(sub_name)

        iterator = self.hintmap.iterator(model, names)
        for hint, _ in iterator:
            hints.append(hint)

        subresults_dict = OrderedDict()
        for prefix, sub_name_list in rels.items():
            if prefix == self.ALL:
                prefix = REL
            for hint, selected in iterator.clone([prefix]):
                logger.debug("\t\t\tfield %r %r (%r %r)", model.__name__, hint.name, hint.rel_model.__name__, hint.rel_name)
                k = (model, hint.name)
                if not selected:
                    if indent == 1 and (hint.rel_model, "") in backref:
                        logger.info("\t\t\tskip %r %r %r", model.__name__, hint.name, hint.rel_model.__name__)
                        continue
                    if (hint.rel_model, hint.rel_name) in backref:
                        logger.info("\t\t\tskip %s %s %s", model.__name__, hint.name, hint.rel_model.__name__)
                        continue
                backref.add(k)
                hints.append(hint)
                self._merge(
                    subresults_dict,
                    self.drilldown(
                        hint.rel_model, sub_name_list,
                        backref=backref, parent_name=hint.name, indent=indent + 1
                    )
                )
                if k in backref:
                    backref.remove(k)
        return TmpResult(name=parent_name, hints=hints, subresults=list(subresults_dict.values()))

    def _merge(self, d, r):
        if r.name not in d:
            d[r.name] = r
        else:
            cr = d[r.name]
            cr.hints.extend(r.hints)
            cr.subresults.extend(r.subresults)


if django.VERSION >= (1, 8):
    def get_all_fields(m):
        return m._meta.get_fields()
else:
    # planning to drop 1.7
    from django.db.models.fields.related import RelatedField

    class FieldAdapter(object):
        def __init__(self, f):
            self.f = f
            self.is_relation = isinstance(f, RelatedField)

        def __getattr__(self, k):
            return getattr(self.f, k)

    def get_all_fields(m):
        xs = [m._meta._name_map.get(name, None)[0] for name in m._meta.get_all_field_names()]
        return [FieldAdapter(x) for x in xs if x]


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
        xs = [h for h in result.related if isinstance(h.field.field, (related.OneToOneField))]
        ys = [h for h in result.reverse_related if isinstance(h.field.rel, (reverse_related.OneToOneRel, reverse_related.ManyToOneRel))]
        return itertools.chain(xs, ys)

    def collect_prefetchs(self, result):
        matched = {}
        for h in result.related:
            if isinstance(h.field.field, (related.ManyToManyField, related.ForeignKey)):
                matched[h.name] = h
        for h in result.reverse_related:
            if isinstance(h.field.rel, (reverse_related.ManyToOneRel, reverse_related.ManyToManyRel)):
                matched[h.name] = h
        for sr in result.subresults:
            if sr.name in matched:
                yield Pair(hint=matched[sr.name], result=sr)

    def collect_selections(self, result):
        xs = itertools.chain([f.name for f in result.fields], result.foreign_keys)
        ys = []
        related_mapping = {h.name: h.rel_model for h in result.related}
        for sr in result.subresults:
            if sr.name in related_mapping:
                # this is limitation. for django's compiler.
                sub_fields = (h.name for h in self.hintmap.iterator(related_mapping[sr.name], NOREL))
            else:
                sub_fields = self.collect_selections(sr)
            ys.append(["{}__{}".format(sr.name, name) for name in sub_fields])
        return itertools.chain(xs, *ys)

    def pp(self, result, out=sys.stdout):
        d = result.asdict()
        return out.write(json.dumps(d, indent=2))


default_hint_extractor = HintExtractor()


# utilities
def reset_select_related(qs, join_targets):
    # remove all and set new settings
    new_qs = qs.select_related(None).select_related(*join_targets)
    logger.debug("@select_related: %r", join_targets)
    return new_qs


def reset_prefetch_related(qs, prefetch_targets):
    # remove all and set new settings
    new_qs = qs.prefetch_related(None).prefetch_related(*prefetch_targets)
    logger.debug("@prefetch: %r", prefetch_targets)
    return new_qs


class QueryOptimizer(object):
    def __init__(self, result, inspector, prefetch_filters=None):
        self.result = result
        self.inspector = inspector
        self.prefetch_filters = prefetch_filters or defaultdict(list)

    def __copy__(self):
        return self.__class__(
            self.result, self.inspector,
            prefetch_filters=copy.deepcopy(self.prefetch_filters)
        )

    def optimize(self, qs, result=None):
        result = result or self.result
        qs = self._optimize_join(qs.all(), result)
        qs = self._optimize_prefetch(qs, result)
        qs = self._optimize_selections(qs, result)
        return qs

    def _optimize_selections(self, qs, result, *externals):
        # todo: disable using only
        fields = list(itertools.chain(self.inspector.collect_selections(result), externals))
        logger.debug("@selection, %r", fields)
        return qs.only(*fields)

    def _optimize_join(self, qs, result):
        # todo: nested
        join_targets = [h.name for h in self.inspector.collect_joins(result)]
        return reset_select_related(qs, join_targets)

    def _optimize_prefetch(self, qs, result):
        # todo: nested, settings filter lazy
        prefetch_targets = []
        pairs = self.inspector.collect_prefetchs(result)
        for h, sr in pairs:
            prefetch_qs = self._optimize_prefetch(h.rel_model.objects.all(), sr)
            if h.rel_fk:
                prefetch_qs = self._optimize_selections(prefetch_qs, sr, h.rel_fk)
            else:
                prefetch_qs = self._optimize_selections(prefetch_qs, sr)
            prefetch_qs = functools.reduce(lambda qs, f: f(qs), self.prefetch_filters[h.name], prefetch_qs)
            prefetch_targets.append(Prefetch(h.name, queryset=prefetch_qs))  # support to_attr?
        return reset_prefetch_related(qs, prefetch_targets)


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

    def prefetch_filter(self, name, filter_fn):
        new_qs = self._clone()
        new_qs.optimizer.prefetch_filters[name].append(filter_fn)
        return new_qs

    @cached_property
    def aggressive_queryset(self):
        return self.optimizer.optimize(self.source_queryset)

    def to_query(self):
        return self.aggressive_queryset

    @property
    def query(self):
        return self.aggressive_queryset.query

    def __iter__(self):
        return iter(self.aggressive_queryset)

    def __getitem__(self, k):
        return self.aggressive_queryset[k]

    def pp(self, out=sys.stdout):
        return self.inspector.pp(self.result)


def from_query(qs, name_list, extractor=default_hint_extractor):
    result = extractor.extract(qs.model, name_list)
    inspector = Inspector(extractor.hintmap)
    optimizer = QueryOptimizer(result, inspector)
    return AggressiveQuery(qs, optimizer)
