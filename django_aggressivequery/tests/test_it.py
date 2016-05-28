# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class Tests(TestCase):
    def _callFUT(self, query, fields):
        from django_aggressivequery import from_query
        return from_query(query, fields)

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
        optimized = self._callFUT(qs, ["*", "customer__orders__items__*", "customer__orders__name"])

        with self.assertNumQueries(3):
            buf = []
            for karma in optimized:
                buf.append("karma: {}, customer: {}".format(karma.point, karma.customer.name))
                for order in karma.customer.orders.all():
                    buf.append("- order: {}, items: {}".format(order.name, ", ".join(item.name for item in order.items.all())))
            expected = """\
karma: 10, customer: bar
- order: order-1, items: order-1-item-a, order-1-item-b, order-1-item-c
- order: order-2, items: order-2-item-a, order-2-item-b"""
            actual = "\n".join(buf)
            self.assertEqual(expected, actual)
