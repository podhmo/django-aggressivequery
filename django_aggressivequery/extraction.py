import django
from collections import defaultdict, OrderedDict
from .structures import Hint, TmpResult, Result
import logging
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
            name, is_relation = f.name, f.is_relation
            rel_name = None
            is_reverse_related = False
            rel_model = None
            rel_fk = None
            if is_relation and not getattr(f, "primary_key", False):
                if hasattr(f, "rel_class"):
                    is_reverse_related = True
                    rel_name = f.rel.get_accessor_name()
                    rel_model = f.rel.model
                    rel_fk = getattr(f.rel, "attname", None)
                else:
                    name = f.get_accessor_name()
                    rel_name = f.field.name
                    rel_model = f.field.model
                    rel_fk = getattr(f.field, "attname", None)
            d[f.name] = Hint(name=name,
                             is_relation=is_relation,
                             is_reverse_related=is_reverse_related,
                             rel_name=rel_name,
                             rel_model=rel_model,
                             rel_fk=rel_fk,
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

    def iterator(self, model, tokens, history=None):
        return self.iterator_cls(self.load(model), tokens)


class HintExtractor(object):
    ALL = "*"

    def __init__(self, sorted=True, hintmap=None):
        self.sorted = sorted
        self.bidirectional = False
        self.hintmap = hintmap or HintMap()

    def __copy__(self):
        return self.__class__(sorted=self.sorted, hintmap=self.hintmap)

    def extract(self, model, name_list):
        backref = set()
        backref.add((model, ""))
        history = [""]
        tmp_result = self.drilldown(model, name_list, backref=backref, history=history)
        return self.classify(tmp_result)

    def seq(self, seq, key):
        return sorted(seq, key=key) if self.sorted else seq

    def classify(self, tmp_result):
        result = Result(name=tmp_result.name,
                        fields=[],
                        related=[],
                        reverse_related=[],
                        subresults=[])
        for h in self.seq(tmp_result.hints.values(), key=lambda h: h.name):
            if not h.is_relation:
                result.fields.append(h)
                continue

            if h.is_reverse_related:
                result.reverse_related.append(h)
            else:
                result.related.append(h)

        for sr in self.seq(tmp_result.subresults.values(), key=lambda r: r.name):
            result.subresults.append(self.classify(sr))
        return result

    def drilldown(self, model, name_list, backref, history, indent=0):
        parent_name = history[-1]
        logger.info("%s name=%r model=%r %r", " " * (indent + indent), parent_name, model.__name__, name_list)
        hints = OrderedDict()
        names = set()
        rels = defaultdict(list)
        for name in name_list:
            if name == self.ALL:
                names.add(NOREL)
            elif "__" not in name:
                if name not in rels:
                    names.add(name)
            else:
                prefix, sub_name = name.split("__", 1)
                rels[prefix].append(sub_name)

        iterator = self.hintmap.iterator(model, names, history=history)
        for hint, _ in iterator:
            hints[hint.name] = hint

        subresults_dict = OrderedDict()
        # hints is dict? REL and explicit relation name.
        for prefix, sub_name_list in rels.items():
            if prefix == self.ALL:
                prefix = REL
            for hint, selected in iterator.clone([prefix]):
                if not hint.is_relation:
                    hints[hint.name] = hint
                    continue

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
                hints[hint.name] = hint
                history.append(hint.name)
                self._merge(
                    subresults_dict,
                    self.drilldown(
                        hint.rel_model, sub_name_list,
                        backref=backref, history=history, indent=indent + 1
                    )
                )
                history.pop()
                if k in backref:
                    backref.remove(k)
        return TmpResult(name=parent_name, hints=hints, subresults=subresults_dict)

    def _merge(self, result_dict, tr):
        if tr.name not in result_dict:
            result_dict[tr.name] = tr
        else:
            cr = result_dict[tr.name]
            merge_tmp_result(cr, tr)


def merge_tmp_result(tr0, tr1):
    tr0.hints.update(tr1.hints)
    for name in tr1.subresults.keys():
        if name not in tr0.subresults:
            tr0.subresults[name] = tr1.subresults[name]
        else:
            merge_tmp_result(tr0.subresults[name], tr1.subresults[name])


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
