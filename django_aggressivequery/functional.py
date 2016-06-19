# -*- coding:utf-8 -*-
from django.utils.functional import cached_property as _cached_property


class ExAttributeError(Exception):
    pass


class cached_property(_cached_property):
    def __get__(self, instance, type=None):
        try:
            return super().__get__(instance, type=type)
        except AttributeError as e:
            raise ExAttributeError(e.args[0])
