# -*- coding:utf-8 -*-
from collections import namedtuple, defaultdict


def tree():
    return defaultdict(tree)


Hint = namedtuple(
    "Hint",
    "name, is_relation, is_reverse_related, rel_name, rel_fk, rel_model, field"
)
CustomHint = namedtuple(
    "CustomHint",
    "name, type, is_relation, value, rel_model, rel_name, is_reverse_related"
)
TmpResult = namedtuple(
    "TmpResult",
    "name, hints, subresults"
)
Result = namedtuple(
    "Result",
    "name, fields, related, reverse_related, subresults"
)
Pair = namedtuple(
    "Pair",
    "hint, result"
)


def asdict_result(self):
    if not hasattr(self, "_asdict"):
        raise Exception("{!r} is not namedtuple".format(self))
    d = self._asdict()
    for k, v in d.items():
        if hasattr(v, "asdict"):
            d[k] = v.asdict()
        elif isinstance(v, (list, tuple)):
            d[k] = [sv.asdict() if hasattr(sv, "asdict") else sv for sv in v]
    return d


def repr_result(self):
    values = []
    for k, v in self._asdict().items():
        if v:
            values.append("{}={!r}".format(k, v))
    return "{}({})".format(self.__class__.__name__, ", ".join(values))


def dict_from_keys(keys, separator="__"):
    """xxx__yyy__zzz -> {xxx: {yyy: {zzz: {}}}}"""
    d = tree()
    for k in keys:
        splitted = k.split(separator)
        target = d
        for name in splitted[:-1]:
            target = target[name]
        target[splitted[-1]] = tree()
    return d


def excluded_result(self, skip_dict):
    skip_keys = {k for k, d in skip_dict.items() if len(d) == 0}
    fields = [h for h in self.fields if h.name not in skip_keys]
    related = [h for h in self.related if h.name not in skip_keys]
    reverse_related = [h for h in self.reverse_related if h.name not in skip_keys]
    subresults = [excluded_result(sr, skip_dict[sr.name]) for sr in self.subresults if sr.name not in skip_keys]
    return Result(name=self.name, fields=fields, related=related, reverse_related=reverse_related, subresults=subresults)


Result.__repr__ = repr_result
Result.asdict = asdict_result


def asdict_hint(self):
    d = self._asdict()
    d.pop("field")
    model = d.pop("rel_model", None)
    if model:
        d["_classname"] = model.__name__
    if d["is_relation"]:
        if d["is_reverse_related"]:
            d["_relclassname"] = self.field.rel.__class__.__name__
        else:
            d["_relclassname"] = self.field.field.__class__.__name__
    return d


def repr_hint(self):
    return "{}(name={!r})".format(self.__class__.__name__, self.name)


Hint.__repr__ = repr_hint
Hint.asdict = asdict_hint
