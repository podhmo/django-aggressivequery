# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class SkipFilterTests(TestCase):
    """extension skip_filter test"""

    def _makeOne(self, *args, **kwargs):
        from django_aggressivequery import from_queryset
        return from_queryset(*args, **kwargs)

    def test_it__nest1(self):
        # normally, join targets is all relations.
        aqs = self._makeOne(m.CustomerKarma.objects.all(), ["*"])
        self.assertIn('"customerkarma" INNER JOIN "customer"', str(aqs.query))

        # with skip filter, dejoin related objects.
        aqs = self._makeOne(m.CustomerKarma.objects.all(), ["*"])
        aqs = aqs.skip_filter(["customer"])
        self.assertNotIn('"customerkarma" INNER JOIN "customer"', str(aqs.query))

    def test_it__nest2(self):
        # normally, join targets is all relations.
        aqs = self._makeOne(m.CustomerPosition.objects.all(), ["*__*"])
        self.assertIn('INNER JOIN "customer"', str(aqs.query))
        self.assertIn('LEFT OUTER JOIN "customerkarma"', str(aqs.query))

        aqs = self._makeOne(m.CustomerPosition.objects.all(), ["*__*"])
        aqs = aqs.skip_filter(["customer__karma", "substitute__karma"])
        self.assertIn('INNER JOIN "customer"', str(aqs.query))
        self.assertNotIn('LEFT OUTER JOIN "customerkarma"', str(aqs.query))
