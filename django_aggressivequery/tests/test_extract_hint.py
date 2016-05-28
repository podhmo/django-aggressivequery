# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class ExtractHintDictTests(TestCase):
    def _callFUT(self, model):
        from django_aggressivequery import HintMap
        return HintMap().extract(model)

    def test_for_prepare__load_candidates(self):
        candidates = self._callFUT(m.Customer)
        expected = [
            'id',
            'name',
            'memo1',
            'memo2',
            'memo3',
            'orders',
            'karma',
            'customerposition',
            'customerposition_set',
        ]
        self.assertEqual(tuple(sorted(candidates.keys())), tuple(sorted(expected)))

    def test_for_prepare__load_candidates2(self):
        candidates = self._callFUT(m.CustomerKarma)
        expected = [
            'id',
            'point',
            'customer',
            'memo1',
            'memo2',
            'memo3',
        ]
        self.assertEqual(tuple(sorted(candidates.keys())), tuple(sorted(expected)))

    def test_for_prepare__load_candidates3(self):
        from .models import Order
        candidates = self._callFUT(Order)
        expected = [
            'id',
            'name',
            'price',
            'memo1',
            'memo2',
            'memo3',
            'items',
            'customers',
        ]
        self.assertEqual(tuple(sorted(candidates.keys())), tuple(sorted(expected)))

    def test_for_prepare__load_candidates4(self):
        candidates = self._callFUT(m.Item)
        expected = [
            'id',
            'name',
            'price',
            'memo1',
            'memo2',
            'memo3',
            'order',
        ]
        self.assertEqual(tuple(sorted(candidates.keys())), tuple(sorted(expected)))
