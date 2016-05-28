# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class InpectorCollectPrefetchTargetsTests(TestCase):
    def _getTargetClass(self):
        from django_aggressivequery import Inspector
        return Inspector

    def _makeExtractor(self):
        from django_aggressivequery import HintExtractor
        return HintExtractor()

    def _makeOne(self, extractor):
        return self._getTargetClass()(extractor.hintmap)

    def test_many_to_many(self):
        model = m.Order
        name_list = ["name", "customers__name"]
        extractor = self._makeExtractor()
        result = extractor.extract(model, name_list)
        target = self._makeOne(extractor)

        actual = list(target.collect_prefetch_list(result))
        self.assertEqual(len(actual), 1)
        self.assertEqual(actual[0].hint.name, "customers")
        self.assertEqual([h.name for h in actual[0].result.fields], ["name"])
