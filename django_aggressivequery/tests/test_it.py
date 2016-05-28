# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class Tests(TestCase):
    def _callFUT(self, query, fields, more_specific=False):
        from django_aggressivequery import from_query
        return from_query(query, fields, more_specific=more_specific)

    def setUp(self):
        foo = m.Customer.objects.create(name="foo")
        m.CustomerKarma.objects.create(point=0, customer=foo)
        bar = m.Customer.objects.create(name="bar")
        m.CustomerKarma.objects.create(point=10, customer=bar)

        order1 = m.Order.objects.create(name="order-1")
        m.Item.objects.create(name="order-1-item-a", order=order1)
        m.Item.objects.create(name="order-1-item-b", order=order1)
        m.Item.objects.create(name="order-1-item-c", order=order1)
        order2 = m.Order.objects.create(name="order-2")
        m.Item.objects.create(name="order-2-item-a", order=order2)
        m.Item.objects.create(name="order-2-item-b", order=order2)
        order1.customers.add(foo)
        order1.customers.add(bar)
        order1.save()
        order2.customers.add(bar)
        order2.save()

    def test_it__nested(self):
        qs = m.CustomerKarma.objects.filter(point__gt=0)
        optimized = self._callFUT(qs, ["customer__orders__items"])

        with self.assertNumQueries(3):
            buf = []
            optimized = optimized.prefetch_filter("customer__orders__items", lambda qs: qs.filter(name__contains="-a"))
            for karma in optimized:
                buf.append("karma: {}, customer: {}".format(karma.point, karma.customer.name))
                for order in karma.customer.orders.all():
                    buf.append("- order: {}, items: {}".format(order.name, ", ".join(item.name for item in order.items.all())))
            expected = """\
karma: 10, customer: bar
- order: order-1, items: order-1-item-a
- order: order-2, items: order-2-item-a"""
            actual = "\n".join(buf)
            self.assertEqual(expected, actual)

    def test_it__nested__more_specific(self):
        qs = m.CustomerKarma.objects.filter(point__gt=0)
        optimized = self._callFUT(qs, ["*", "customer__orders__items__*", "customer__orders__name"], more_specific=True)

        with self.assertNumQueries(3):
            buf = []
            optimized = optimized.prefetch_filter("customer__orders__items", lambda qs: qs.filter(name__contains="-a"))
            for karma in optimized:
                buf.append("karma: {}, customer: {}".format(karma.point, karma.customer.name))
                for order in karma.customer.orders.all():
                    buf.append("- order: {}, items: {}".format(order.name, ", ".join(item.name for item in order.items.all())))
            expected = """\
karma: 10, customer: bar
- order: order-1, items: order-1-item-a
- order: order-2, items: order-2-item-a"""
            actual = "\n".join(buf)
            self.assertEqual(expected, actual)

    def test__dont_use__this_feature__more_specific(self):
        from django.db.models import Prefetch
        qs = m.CustomerKarma.objects.filter(point__gt=0)
        with self.assertNumQueries(3):
            buf = []
            optimized = (
                qs
                .select_related("customer")
                .only("point", "customer__name")
                .prefetch_related(
                    "customer__orders",
                    Prefetch("customer__orders__items", queryset=m.Item.objects.filter(name__contains="-a").only("name", "order_id"))
                )
            )
            for karma in optimized:
                buf.append("karma: {}, customer: {}".format(karma.point, karma.customer.name))
                for order in karma.customer.orders.all():
                    buf.append("- order: {}, items: {}".format(order.name, ", ".join(item.name for item in order.items.all())))
            expected = """\
karma: 10, customer: bar
- order: order-1, items: order-1-item-a
- order: order-2, items: order-2-item-a"""
            actual = "\n".join(buf)
            self.assertEqual(expected, actual)
