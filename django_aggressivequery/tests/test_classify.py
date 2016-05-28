# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class ExtractorClassifyTests(TestCase):
    def _makeOne(self):
        from django_aggressivequery import HintExtractor
        return HintExtractor()

    def _makeInspector(self):
        from django_aggressivequery import Inspector
        return Inspector()

    # relation: CustomerKarma - Customer *-* Order -* Item, Customer -* CustomerPosition
    def test_it_nest1__star(self):
        model = m.CustomerKarma
        query = ["*"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='point')])"
        self.assertEqual(str(actual), expected)

    def test_it_nest2__star_id__karma(self):
        model = m.CustomerKarma
        query = ["*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(reverse_related=[Hint(name='customer')], foreign_keys=['customer_id'], subresults=[Result(name='customer', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest2__star_id__customer(self):
        model = m.Customer
        query = ["*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(related=[Hint(name='customerposition'), Hint(name='karma'), Hint(name='orders')], subresults=[Result(name='customerposition', fields=[Hint(name='id')]), Result(name='karma', fields=[Hint(name='id')]), Result(name='orders', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest2__star_id__order(self):
        model = m.Order
        query = ["*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(related=[Hint(name='items')], reverse_related=[Hint(name='customers')], subresults=[Result(name='customers', fields=[Hint(name='id')]), Result(name='items', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest2__star_id__item(self):
        model = m.Item
        query = ["*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(reverse_related=[Hint(name='order')], foreign_keys=['order_id'], subresults=[Result(name='order', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest2__star_id__customerposition(self):
        model = m.CustomerPosition
        query = ["*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(reverse_related=[Hint(name='customer'), Hint(name='substitute')], foreign_keys=['customer_id', 'substitute_id'], subresults=[Result(name='customer', fields=[Hint(name='id')]), Result(name='substitute', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest__each_field__customer(self):
        model = m.Customer
        query = ["karma__memo1", "orders__memo2", "customerposition__memo3"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(related=[Hint(name='customerposition'), Hint(name='karma'), Hint(name='orders')], subresults=[Result(name='customerposition', fields=[Hint(name='memo3')]), Result(name='karma', fields=[Hint(name='memo1')]), Result(name='orders', fields=[Hint(name='memo2')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest3__star_id__item(self):
        model = m.Item
        query = ["*__*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(reverse_related=[Hint(name='order')], foreign_keys=['order_id'], subresults=[Result(name='order', reverse_related=[Hint(name='customers')], subresults=[Result(name='customers', fields=[Hint(name='id')])])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest3__star_id__item__select__skiped_attributes__directly(self):
        model = m.Item
        query = ["*__*__id", "order__items"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(reverse_related=[Hint(name='order'), Hint(name='order')], foreign_keys=['order_id', 'order_id'], subresults=[Result(name='order', related=[Hint(name='items')], reverse_related=[Hint(name='customers')], subresults=[Result(name='customers', fields=[Hint(name='id')])])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest6__id__item(self):
        model = m.Item
        query = ["*", "*__*", "*__*__*", "*__*__*__*", "*__*__*__*__*", "*__*__*__*__*__*"]
        actual = self._makeOne().extract(model, query)

        inspector = self._makeInspector()
        self.assertEqual(inspector.depth(actual), 6)
        expected = "Result(fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name'), Hint(name='price')], reverse_related=[Hint(name='order')], foreign_keys=['order_id'], subresults=[Result(name='order', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name'), Hint(name='price')], reverse_related=[Hint(name='customers')], subresults=[Result(name='customers', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name')], related=[Hint(name='customerposition'), Hint(name='karma')], subresults=[Result(name='customerposition', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name')], reverse_related=[Hint(name='customer'), Hint(name='substitute')], foreign_keys=['customer_id', 'substitute_id'], subresults=[Result(name='customer', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name')], related=[Hint(name='customerposition'), Hint(name='karma')], subresults=[Result(name='customerposition', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name')]), Result(name='karma', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='point')])]), Result(name='substitute', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name')], related=[Hint(name='karma')], subresults=[Result(name='karma', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='point')])])]), Result(name='karma', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='point')])])])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest6__id__customer(self):
        model = m.Customer
        query = ["id", "*__id", "*__*__id", "*__*__*__id", "*__*__*__*__id", "*__*__*__*__*__id"]
        actual = self._makeOne().extract(model, query)

        inspector = self._makeInspector()
        self.assertEqual(inspector.depth(actual), 3)
        expected = "Result(fields=[Hint(name='id')], related=[Hint(name='customerposition'), Hint(name='karma'), Hint(name='orders')], subresults=[Result(name='customerposition', fields=[Hint(name='id')]), Result(name='karma', fields=[Hint(name='id')]), Result(name='orders', fields=[Hint(name='id')], related=[Hint(name='items')], subresults=[Result(name='items', fields=[Hint(name='id')])])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest6__star__customer(self):
        model = m.Customer
        query = ["id", "*__id", "*__*__id", "*__*__*__id", "*__*__*__*__id", "*__*__*__*__*__id"]
        actual = self._makeOne().extract(model, query)

        inspector = self._makeInspector()
        self.assertEqual(inspector.depth(actual), 3)
        expected = "Result(fields=[Hint(name='id')], related=[Hint(name='customerposition'), Hint(name='karma'), Hint(name='orders')], subresults=[Result(name='customerposition', fields=[Hint(name='id')]), Result(name='karma', fields=[Hint(name='id')]), Result(name='orders', fields=[Hint(name='id')], related=[Hint(name='items')], subresults=[Result(name='items', fields=[Hint(name='id')])])])"
        self.assertEqual(str(actual), expected)

    def test_it_usualy_case(self):
        model = m.CustomerKarma
        query = ["id", "customer__id", "customer__customerposition__id"]
        actual = self._makeOne().extract(model, query)

        inspector = self._makeInspector()
        self.assertEqual(inspector.depth(actual), 3)
        expected = "Result(fields=[Hint(name='id')], reverse_related=[Hint(name='customer')], foreign_keys=['customer_id'], subresults=[Result(name='customer', fields=[Hint(name='id')], related=[Hint(name='customerposition')], subresults=[Result(name='customerposition', fields=[Hint(name='id')])])])"
        self.assertEqual(str(actual), expected)
