# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class CustomPrefetchTests(TestCase):
    """extension custom_prefetch test"""

    def _makeOne(self, *args, **kwargs):
        from django_aggressivequery import from_queryset
        return from_queryset(*args, **kwargs)

    def _callFUT(self, aqs):
        from django.db.models import Prefetch
        return aqs.custom_prefetch(
            positive_items=Prefetch("items", m.Item.objects.filter(price__gte=0), to_attr="positive_items"),
            negative_items=Prefetch("items", m.Item.objects.filter(price__lt=0), to_attr="negative_items"),
        )

    def test_custom_prefetch__passed_prefetch_is_existed(self):
        # positive_items is existed
        aqs = self._makeOne(m.Order.objects.all(), ["positive_items__subitems"])
        result = self._callFUT(aqs)
        prefetchs = result.to_queryset()._prefetch_related_lookups
        self.assertIn("positive_items", [p.to_attr for p in prefetchs])
        self.assertEqual(prefetchs[0].queryset.model, m.Item)
        self.assertIn('"item"."price" >= 0', str(prefetchs[0].queryset.query))

    def test_custom_prefetch__unpassed_prefetch_is_not_existed(self):
        # negative_items is not existed
        aqs = self._makeOne(m.Order.objects.all(), ["positive_items__subitems"])
        result = self._callFUT(aqs)
        prefetchs = result.to_queryset()._prefetch_related_lookups
        self.assertNotIn("negative_items", [p.to_attr for p in prefetchs])

    def test_custom_prefetch__subrelation(self):
        # prefetch_related is ok, on positive_items
        aqs = self._makeOne(m.Order.objects.all(), ["positive_items__subitems"])
        result = self._callFUT(aqs)
        prefetchs = result.to_queryset()._prefetch_related_lookups
        self.assertIn(m.SubItem, [p.queryset.model for p in prefetchs])
        self.assertEqual(prefetchs[1].queryset.model, m.SubItem)
        self.assertEqual(prefetchs[1].to_attr, None)

    # join is ok, on positive_items -- hmm
