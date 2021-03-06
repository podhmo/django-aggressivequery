# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class ExtractorClassifyTests(TestCase):
    def _makeOne(self):
        from django_aggressivequery.extraction import HintExtractor
        return HintExtractor()

    def _makeInspector(self):
        from django_aggressivequery import Inspector
        return Inspector()

    # relation: CustomerKarma - Customer *-* Order -* Item, Customer -* Customerposition
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
        expected = "Result(reverse_related=[Hint(name='customer')], subresults=[Result(name='customer', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest2__star_id__customer(self):
        model = m.Customer
        query = ["*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(related=[Hint(name='customerposition_set'), Hint(name='karma'), Hint(name='orders')], subresults=[Result(name='customerposition_set', fields=[Hint(name='id')]), Result(name='karma', fields=[Hint(name='id')]), Result(name='orders', fields=[Hint(name='id')])])"
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
        expected = "Result(related=[Hint(name='subitems')], reverse_related=[Hint(name='order')], subresults=[Result(name='order', fields=[Hint(name='id')]), Result(name='subitems', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest2__star_id__customerposition_set(self):
        model = m.CustomerPosition
        query = ["*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(reverse_related=[Hint(name='customer'), Hint(name='substitute')], subresults=[Result(name='customer', fields=[Hint(name='id')]), Result(name='substitute', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest__each_field__customer(self):
        model = m.Customer
        query = ["karma__memo1", "orders__memo2", "customerposition_set__memo3"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(related=[Hint(name='customerposition_set'), Hint(name='karma'), Hint(name='orders')], subresults=[Result(name='customerposition_set', fields=[Hint(name='memo3')]), Result(name='karma', fields=[Hint(name='memo1')]), Result(name='orders', fields=[Hint(name='memo2')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest3__star_id__item(self):
        model = m.Item
        query = ["*__*__id"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(related=[Hint(name='subitems')], reverse_related=[Hint(name='order')], subresults=[Result(name='order', reverse_related=[Hint(name='customers')], subresults=[Result(name='customers', fields=[Hint(name='id')])]), Result(name='subitems')])"
        self.assertEqual(str(actual), expected)

    def test_it_nest3__star_id__item__select__skiped_attributes__directly(self):
        model = m.Item
        query = ["*__*__id", "order__items"]
        actual = self._makeOne().extract(model, query)
        expected = "Result(related=[Hint(name='subitems')], reverse_related=[Hint(name='order')], subresults=[Result(name='order', related=[Hint(name='items')], reverse_related=[Hint(name='customers')], subresults=[Result(name='customers', fields=[Hint(name='id')])]), Result(name='subitems')])"
        self.assertEqual(str(actual), expected)

    def test_it_nest6__id__item(self):
        model = m.Item
        query = ["id", "*__id", "*__*__id", "*__*__*__id", "*__*__*__*__id", "*__*__*__*__*__id"]
        actual = self._makeOne().extract(model, query)

        inspector = self._makeInspector()
        self.assertEqual(inspector.depth(actual), 4)
        expected = "Result(fields=[Hint(name='id')], related=[Hint(name='subitems')], reverse_related=[Hint(name='order')], subresults=[Result(name='order', fields=[Hint(name='id')], reverse_related=[Hint(name='customers')], subresults=[Result(name='customers', fields=[Hint(name='id')], related=[Hint(name='customerposition_set'), Hint(name='karma')], subresults=[Result(name='customerposition_set', fields=[Hint(name='id')]), Result(name='karma', fields=[Hint(name='id')])])]), Result(name='subitems', fields=[Hint(name='id')])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest6__id__customer(self):
        model = m.Customer
        query = ["id", "*__id", "*__*__id", "*__*__*__id", "*__*__*__*__id", "*__*__*__*__*__id"]
        actual = self._makeOne().extract(model, query)

        inspector = self._makeInspector()
        self.assertEqual(inspector.depth(actual), 4)
        expected = "Result(fields=[Hint(name='id')], related=[Hint(name='customerposition_set'), Hint(name='karma'), Hint(name='orders')], subresults=[Result(name='customerposition_set', fields=[Hint(name='id')]), Result(name='karma', fields=[Hint(name='id')]), Result(name='orders', fields=[Hint(name='id')], related=[Hint(name='items')], subresults=[Result(name='items', fields=[Hint(name='id')], related=[Hint(name='subitems')], subresults=[Result(name='subitems', fields=[Hint(name='id')])])])])"
        self.assertEqual(str(actual), expected)

    def test_it_nest6__star__customer(self):
        model = m.Customer
        query = ["*", "*__*", "*__*__*", "*__*__*__*", "*__*__*__*__*", "*__*__*__*__*__*"]
        actual = self._makeOne().extract(model, query)

        inspector = self._makeInspector()
        self.assertEqual(inspector.depth(actual), 4)
        expected = "Result(fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name')], related=[Hint(name='customerposition_set'), Hint(name='karma'), Hint(name='orders')], subresults=[Result(name='customerposition_set', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name')]), Result(name='karma', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='point')]), Result(name='orders', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name'), Hint(name='price')], related=[Hint(name='items')], subresults=[Result(name='items', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name'), Hint(name='price')], related=[Hint(name='subitems')], subresults=[Result(name='subitems', fields=[Hint(name='id'), Hint(name='memo1'), Hint(name='memo2'), Hint(name='memo3'), Hint(name='name')])])])])"
        self.assertEqual(str(actual), expected)

    def test_it_usualy_case(self):
        model = m.CustomerKarma
        query = ["id", "customer__id", "customer__customerposition_set__id"]
        actual = self._makeOne().extract(model, query)

        inspector = self._makeInspector()
        self.assertEqual(inspector.depth(actual), 3)
        expected = "Result(fields=[Hint(name='id')], reverse_related=[Hint(name='customer')], subresults=[Result(name='customer', fields=[Hint(name='id')], related=[Hint(name='customerposition_set')], subresults=[Result(name='customerposition_set', fields=[Hint(name='id')])])])"
        self.assertEqual(str(actual), expected)

    def test__no_duplicated(self):
        model = m.CustomerPosition
        query1 = ["customer__*__*"]
        actual11 = self._makeOne().extract(model, query1)
        actual12 = self._makeOne().extract(model, query1)
        self.assertEqual(str(actual11), str(actual12))

        query2 = ["customer__karma", "customer__*__*"]
        actual21 = self._makeOne().extract(model, query2)
        self.assertEqual(str(actual11), str(actual21))
