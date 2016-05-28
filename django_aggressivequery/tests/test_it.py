# -*- coding:utf-8 -*-
from django.test import TestCase
from . import models as m


class FromQueryOneToOneTests(TestCase):
    def _callFUT(self, query, fields):
        from django_aggressivequery import from_query
        return from_query(query, fields)

    def _makeCustomerStructure(self, structure):
        for customer_structure in structure:
            customer = m.Customer.objects.create(name=customer_structure["name"])
            if "karma" in customer_structure:
                m.CustomerKarma.objects.create(customer=customer, point=customer_structure["karma"]["point"])

    def test__one_to_one__dejoin(self):
        # setup
        structure = [
            {"name": "foo", "karma": {"point": 10}},
            {"name": "bar", "karma": {"point": 1}}
        ]
        self._makeCustomerStructure(structure)
        for msg, before_count, after_count, has_join, qs_filter in [
                ("with select related", 1, 1, True, lambda qs: qs.all().select_related("karma")),
                ("without select related", 1, 1, False, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, has_join=has_join, qs_filter=qs_filter):
                qs = qs_filter(m.Customer.objects)
                fields = ["xxxx", "name", "memo1"]
                optimized = self._callFUT(qs, fields)

                self.assertEqual("JOIN" in str(qs.query), has_join)
                with self.assertNumQueries(before_count):
                    customers = [(c.id, c.name) for c in qs]
                    self.assertEqual(len(customers), 2)
                    self.assertIn("memo3", str(qs.query))

                self.assertNotIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    customers = [(c.id, c.name) for c in optimized]
                    self.assertEqual(len(customers), 2)
                    self.assertNotIn("memo3", str(optimized.query))

    def test__one_to_one__join_by_filter(self):
        # setup
        structure = [
            {"name": "foo", "karma": {"point": 10}},
            {"name": "bar", "karma": {"point": 1}}
        ]
        self._makeCustomerStructure(structure)
        for msg, before_count, after_count, has_join, qs_filter in [
                ("with select related", 1, 1, True, lambda qs: qs.all().filter(karma__point=10).select_related("karma")),
                ("without select related", 1, 1, False, lambda qs: qs.all().filter(karma__point=10)),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, has_join=has_join, qs_filter=qs_filter):
                qs = qs_filter(m.Customer.objects)
                fields = ["xxxx", "name", "memo1"]
                optimized = self._callFUT(qs, fields)

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(before_count):
                    customers = [(c.id, c.name) for c in qs]
                    self.assertEqual(len(customers), 1)
                    self.assertIn("memo3", str(qs.query))

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    customers = [(c.id, c.name) for c in optimized]
                    self.assertEqual(len(customers), 1)
                    self.assertNotIn("memo3", str(optimized.query))

    def test__one_to_one__join_by_selection(self):
        # setup
        structure = [
            {"name": "foo", "karma": {"point": 10}},
            {"name": "bar", "karma": {"point": 1}}
        ]
        self._makeCustomerStructure(structure)
        for msg, before_count, after_count, has_join, qs_filter in [
                ("with select related", 1, 1, True, lambda qs: qs.all().select_related("karma")),
                ("without select related", 1, 1, False, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, has_join=has_join, qs_filter=qs_filter):
                qs = qs_filter(m.Customer.objects)
                fields = ["xxxx", "name", "memo1", "karma__point"]
                optimized = self._callFUT(qs, fields)

                self.assertEqual("JOIN" in str(qs.query), has_join)
                with self.assertNumQueries(before_count):
                    customers = [(c.id, c.name) for c in qs]
                    self.assertEqual(len(customers), 2)
                    self.assertIn("memo3", str(qs.query))

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    customers = [(c.id, c.name) for c in optimized]
                    self.assertEqual(len(customers), 2)

                    # django's limitation
                    self.assertIn("memo3", str(optimized.query))

    def test__one_to_onerel__join_by_selection(self):
        # setup
        structure = [
            {"name": "foo", "karma": {"point": 10}},
            {"name": "bar", "karma": {"point": 1}}
        ]
        self._makeCustomerStructure(structure)
        for msg, before_count, after_count, has_join, qs_filter in [
                ("with select related", 1, 1, True, lambda qs: qs.all().select_related("customer")),
                ("without select related", 3, 1, False, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, has_join=has_join, qs_filter=qs_filter):
                qs = qs_filter(m.CustomerKarma.objects)
                fields = ["xxxx", "point", "memo1", "customer__name"]
                optimized = self._callFUT(qs, fields)
                self.assertEqual("JOIN" in str(qs.query), has_join)
                with self.assertNumQueries(before_count):
                    customers = [(k.id, k.customer.name, k.point) for k in qs]
                    self.assertEqual(len(customers), 2)
                    self.assertIn("memo3", str(qs.query))

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    customers = [(k.id, k.customer.name, k.point) for k in optimized]
                    self.assertEqual(len(customers), 2)
                    self.assertNotIn("memo3", str(optimized.query))

    def test__one_to_onerel__with_join__rel__star(self):
        # setup
        structure = [
            {"name": "foo", "karma": {"point": 10}},
            {"name": "bar", "karma": {"point": 1}}
        ]
        self._makeCustomerStructure(structure)
        for msg, before_count, after_count, has_join, qs_filter in [
                ("with select related", 1, 1, True, lambda qs: qs.all().select_related("customer")),
                ("without select related", 3, 1, False, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, has_join=has_join, qs_filter=qs_filter):
                qs = qs_filter(m.CustomerKarma.objects)
                fields = ["*", "customer__*"]
                optimized = self._callFUT(qs, fields)
                self.assertEqual("JOIN" in str(qs.query), has_join)
                with self.assertNumQueries(before_count):
                    customers = [(k.id, k.customer.name, k.point) for k in qs]
                    self.assertEqual(len(customers), 2)
                    self.assertIn("memo3", str(qs.query))

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    customers = [(k.id, k.customer.name, k.point) for k in optimized]
                    self.assertEqual(len(customers), 2)
                    self.assertIn("memo3", str(optimized.query))


class FromQueryOneToManyTests(TestCase):
    def _callFUT(self, query, fields):
        from django_aggressivequery import from_query
        return from_query(query, fields)

    def _makeOrderStructure(self, structure):
        for order_structure in structure:
            order = m.Order.objects.create(name=order_structure["name"])
            for item_strcture in order_structure.get("items", []):
                m.Item.objects.create(order=order, name=item_strcture["name"])

    def test__one_to_many__prefetch(self):
        # setup
        structure = [
            {"name": "foo", "items": [{"name": "x"}, {"name": "y"}]},
            {"name": "bar", "items": [{"name": "x"}, {"name": "y"}]}
        ]
        self._makeOrderStructure(structure)

        fields = ["xxxx", "name", "memo1", "items__name", "items__memo2", "items__yyyy"]
        for msg, before_count, after_count, qs_filter in [
                ("with prefetch related", 2, 2, lambda qs: qs.all().prefetch_related("items")),
                ("without prefetch related", 3, 2, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, qs_filter=qs_filter):
                qs = qs_filter(m.Order.objects)
                optimized = self._callFUT(qs, fields)

                with self.assertNumQueries(before_count):
                    items = [(o.id, i.id, o.name, i.name) for o in qs for i in o.items.all()]
                    self.assertEqual(len(items), 4)

                with self.assertNumQueries(after_count):
                    items = [(o.id, i.id, o.name, i.name) for o in optimized for i in o.items.all()]
                    self.assertEqual(len(items), 4)

    def test__one_to_many__deprefetch(self):
        # setup
        structure = [
            {"name": "foo", "items": [{"name": "x"}, {"name": "y"}]},
            {"name": "bar", "items": [{"name": "x"}, {"name": "y"}]}
        ]
        self._makeOrderStructure(structure)

        fields = ["xxxx", "name", "memo1"]
        for msg, before_count, after_count, qs_filter in [
                ("with prefetch related", 2, 1, lambda qs: qs.all().prefetch_related("items")),
                ("without prefetch related", 1, 1, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, qs_filter=qs_filter):
                qs = qs_filter(m.Order.objects)
                optimized = self._callFUT(qs, fields)

                with self.assertNumQueries(before_count):
                    orders = [(o.id, o.name) for o in qs]
                    self.assertEqual(len(orders), 2)

                with self.assertNumQueries(after_count):
                    orders = [(o.id, o.name) for o in optimized]
                    self.assertEqual(len(orders), 2)

    def test__one_to_many__with_join(self):
        # setup
        structure = [
            {"name": "foo", "items": [{"name": "x"}, {"name": "y"}]},
            {"name": "bar", "items": [{"name": "x"}, {"name": "y"}]},
            {"name": "boo", "items": [{"name": "y"}]}
        ]
        self._makeOrderStructure(structure)

        fields = ["xxxx", "name", "memo1", "items__name"]
        for msg, before_count, after_count, qs_filter in [
                ("with prefetch related", 2, 2, lambda qs: qs.all().filter(items__name="x").prefetch_related("items")),
                ("without prefetch related", 3, 2, lambda qs: qs.all().filter(items__name="x")),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, qs_filter=qs_filter):
                qs = qs_filter(m.Order.objects)
                optimized = self._callFUT(qs, fields)

                self.assertIn("JOIN", str(qs.query))
                with self.assertNumQueries(before_count):
                    items = [(o.id, i.id, o.name, i.name) for o in qs for i in o.items.all()]
                    self.assertEqual(len(items), 4)

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    items = [(o.id, i.id, o.name, i.name) for o in optimized for i in o.items.all()]
                    self.assertEqual(len(items), 4)

    def test__one_to_many__with_dejoin(self):
        # setup
        structure = [
            {"name": "foo", "items": [{"name": "x"}, {"name": "y"}]},
            {"name": "bar", "items": [{"name": "x"}, {"name": "y"}]},
            {"name": "boo", "items": [{"name": "y"}]}
        ]
        self._makeOrderStructure(structure)

        fields = ["xxxx", "name", "memo1"]
        for msg, before_count, after_count, qs_filter in [
                ("with prefetch related", 2, 1, lambda qs: qs.all().filter(items__name="x").prefetch_related("items")),
                ("without prefetch related", 1, 1, lambda qs: qs.all().filter(items__name="x")),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, qs_filter=qs_filter):
                qs = qs_filter(m.Order.objects)
                optimized = self._callFUT(qs, fields)

                self.assertIn("JOIN", str(qs.query))
                with self.assertNumQueries(before_count):
                    orders = [(o.id, o.name) for o in qs]
                    self.assertEqual(len(orders), 2)

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    orders = [(o.id, o.name) for o in optimized]
                    self.assertEqual(len(orders), 2)


class FromQueryManyToOneTests(TestCase):
    def _callFUT(self, query, fields):
        from django_aggressivequery import from_query
        return from_query(query, fields)

    def _makeCustomerStructure(self, structure):
        for customer_structure in structure:
            customer = m.Customer.objects.create(name=customer_structure["name"])
            for position_structure in customer_structure.get("customerposition_set", []):
                substitute = m.Customer.objects.create(name=position_structure["substitute"]["name"])
                m.CustomerPosition.objects.create(name=position_structure["name"], customer=customer, substitute=substitute)

    def test__many_to_one__same_type_multiple__join_by_filter(self):
        structure = [
            {"name": "foo", "customerposition_set": [
                {"name": "1st", "substitute": {"name": "dummy1"}}, {"name": "2nd", "substitute": {"name": "dummy2"}}, {"name": "3rd", "substitute": {"name": "dummy3"}}
            ]},
            {"name": "bar", "customerposition_set": [
                {"name": "1st", "substitute": {"name": "dummy4"}}, {"name": "2nd", "substitute": {"name": "dummy5"}}
            ]}
        ]
        self._makeCustomerStructure(structure)
        fields = ["xxxx", "name", "memo1", "customer__name", "substitute__name"]

        for msg, before_count, after_count, has_join, qs_filter in [
                ("with select related", 1, 1, True, lambda qs: qs.all().filter(customer__name="foo").select_related("customer")),
                ("without select related", 1, 1, True, lambda qs: qs.all().filter(customer__name="foo")),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, has_join=has_join, qs_filter=qs_filter):
                qs = qs_filter(m.CustomerPosition.objects)
                optimized = self._callFUT(qs, fields)

                self.assertEqual("JOIN" in str(qs.query), has_join)
                with self.assertNumQueries(before_count):
                    positions = [(p.id, p.name) for p in qs]
                    self.assertEqual(len(positions), 3)
                    self.assertIn("memo3", str(qs.query))

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    positions = [(p.id, p.name) for p in optimized]
                    self.assertEqual(len(positions), 3)
                    self.assertNotIn("memo3", str(optimized.query))

    def test__many_to_one__same_type_multiple__join_by_selections(self):
        structure = [
            {"name": "foo", "customerposition_set": [
                {"name": "1st", "substitute": {"name": "dummy1"}}, {"name": "2nd", "substitute": {"name": "dummy2"}}, {"name": "3rd", "substitute": {"name": "dummy3"}}
            ]},
            {"name": "bar", "customerposition_set": [
                {"name": "1st", "substitute": {"name": "dummy4"}}, {"name": "2nd", "substitute": {"name": "dummy5"}}
            ]}
        ]
        self._makeCustomerStructure(structure)
        fields = ["xxxx", "name", "memo1", "customer__name", "substitute__name"]

        for msg, before_count, after_count, has_join, qs_filter in [
                ("with select related", 1, 1, True, lambda qs: qs.all().select_related("customer")),
                ("without select related", 1, 1, False, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, has_join=has_join, qs_filter=qs_filter):
                qs = qs_filter(m.CustomerPosition.objects)
                optimized = self._callFUT(qs, fields)

                self.assertEqual("JOIN" in str(qs.query), has_join)
                with self.assertNumQueries(before_count):
                    positions = [(p.id, p.name) for p in qs]
                    self.assertEqual(len(positions), 5)
                    self.assertIn("memo3", str(qs.query))

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    positions = [(p.id, p.name) for p in optimized]
                    self.assertEqual(len(positions), 5)
                    self.assertNotIn("memo3", str(optimized.query))

    def test__many_to_one__only_one_type(self):
        structure = [
            {"name": "foo", "customerposition_set": [
                {"name": "1st", "substitute": {"name": "dummy1"}}, {"name": "2nd", "substitute": {"name": "dummy2"}}, {"name": "3rd", "substitute": {"name": "dummy3"}}
            ]},
            {"name": "bar", "customerposition_set": [
                {"name": "1st", "substitute": {"name": "dummy4"}}, {"name": "2nd", "substitute": {"name": "dummy5"}}
            ]}
        ]
        self._makeCustomerStructure(structure)
        fields = ["xxxx", "name", "memo1", "substitute__name"]

        for msg, before_count, after_count, has_join, qs_filter in [
                ("with select related", 1, 1, True, lambda qs: qs.all().select_related("substitute")),
                ("without select related", 6, 1, False, lambda qs: qs.all()),
                ("with another select related", 6, 1, True, lambda qs: qs.all().select_related("customer")),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, has_join=has_join, qs_filter=qs_filter):
                qs = qs_filter(m.CustomerPosition.objects)
                optimized = self._callFUT(qs, fields)

                self.assertEqual("JOIN" in str(qs.query), has_join)
                with self.assertNumQueries(before_count):
                    positions = [(p.id, p.name, p.substitute.name) for p in qs]
                    self.assertEqual(len(positions), 5)
                    self.assertIn("memo3", str(qs.query))
                    self.assertNotIn("customer.", str(qs.query))

                self.assertIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    positions = [(p.id, p.name, p.substitute.name) for p in optimized]
                    self.assertEqual(len(positions), 5)
                    self.assertNotIn("customer.", str(optimized.query))


class FromQueryManyToManyTests(TestCase):
    def _callFUT(self, query, fields):
        from django_aggressivequery import from_query
        return from_query(query, fields)

    def _makeOrderCustomersStructure(self, structure):
        orders = [m.Order.objects.create(name=s["name"]) for s in structure["orders"]]
        customers = [m.Customer.objects.create(name=s["name"]) for s in structure["customers"]]
        for xref in structure["relations"]:
            orders[xref["order"]].customers.add(customers[xref["customer"]])
        for o in orders:
            o.save()

    def test__many_to_many__prefetch(self):
        structure = {
            "orders": [
                {"name": "A"},
                {"name": "B"},
            ],
            "customers": [
                {"name": "x"},
                {"name": "y"},
                {"name": "z"},
            ],
            "relations": [
                {"order": 0, "customer": 0}, {"order": 0, "customer": 1}, {"order": 0, "customer": 2},
                {"order": 1, "customer": 0}
            ]
        }
        self._makeOrderCustomersStructure(structure)
        fields = ["xxxx", "name", "memo1", "customers__name", "customers__memo2", "customers__yyyy"]

        for msg, before_count, after_count, qs_filter in [
                ("with prefetch related", 2, 2, lambda qs: qs.all().prefetch_related("customers")),
                ("without prefetch related", 3, 2, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, qs_filter=qs_filter):
                qs = qs_filter(m.Order.objects)
                optimized = self._callFUT(qs, fields)

                self.assertNotIn("JOIN", str(qs.query))
                with self.assertNumQueries(before_count):
                    customers = [(o.id, o.name, p.id, p.name) for o in qs for p in o.customers.all()]
                    self.assertEqual(len(customers), 4)
                    self.assertIn("memo3", str(qs.query))

                self.assertNotIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    customers = [(o.id, o.name, p.id, p.name) for o in optimized for p in o.customers.all()]
                    self.assertEqual(len(customers), 4)
                    self.assertIn("memo3", str(qs.query))

    def test__many_to_many__deprefetch(self):
        structure = {
            "orders": [
                {"name": "A"},
                {"name": "B"},
            ],
            "customers": [
                {"name": "x"},
                {"name": "y"},
                {"name": "z"},
            ],
            "relations": [
                {"order": 0, "customer": 0}, {"order": 0, "customer": 1}, {"order": 0, "customer": 2},
                {"order": 1, "customer": 0}
            ]
        }
        self._makeOrderCustomersStructure(structure)
        fields = ["xxxx", "name", "memo1"]

        for msg, before_count, after_count, qs_filter in [
                ("with prefetch related", 2, 1, lambda qs: qs.all().prefetch_related("customers")),
                ("without prefetch related", 1, 1, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, qs_filter=qs_filter):
                qs = qs_filter(m.Order.objects)
                optimized = self._callFUT(qs, fields)

                self.assertNotIn("JOIN", str(qs.query))
                with self.assertNumQueries(before_count):
                    customers = [(o.id, o.name,) for o in qs]
                    self.assertEqual(len(customers), 2)
                    self.assertIn("memo3", str(qs.query))

                self.assertNotIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    customers = [(o.id, o.name,) for o in optimized]
                    self.assertEqual(len(customers), 2)
                    self.assertIn("memo3", str(qs.query))

    def test__many_to_many__deprefetch__after_filter(self):
        structure = {
            "orders": [
                {"name": "A"},
                {"name": "B"},
            ],
            "customers": [
                {"name": "x"},
                {"name": "y"},
                {"name": "z"},
            ],
            "relations": [
                {"order": 0, "customer": 0}, {"order": 0, "customer": 1}, {"order": 0, "customer": 2},
                {"order": 1, "customer": 0}
            ]
        }
        self._makeOrderCustomersStructure(structure)
        fields = ["xxxx", "name", "memo1"]

        for msg, before_count, after_count, qs_filter in [
                ("with prefetch related", 2, 2, lambda qs: qs.all().prefetch_related("customers")),
                ("without prefetch related", 2, 2, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, qs_filter=qs_filter):
                qs = qs_filter(m.Order.objects)
                optimized = self._callFUT(qs, fields)

                self.assertNotIn("JOIN", str(qs.query))
                with self.assertNumQueries(before_count):
                    qs = qs.filter(customers__name="z")
                    customers = [(o.id, o.name, c.id, c.name) for o in qs for c in o.customers.all()]
                    self.assertEqual(len(customers), 3)
                    self.assertIn("memo3", str(qs.query))

                self.assertNotIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    optimized = optimized.filter(customers__name="z")
                    customers = [(o.id, o.name, c.id, c.name) for o in optimized for c in o.customers.all()]
                    self.assertEqual(len(customers), 3)
                    self.assertIn("memo3", str(qs.query))

    def test__many_to_many__deprefetch__after_filter2(self):
        from django.db.models import Prefetch
        structure = {
            "orders": [
                {"name": "A"},
                {"name": "B"},
            ],
            "customers": [
                {"name": "x"},
                {"name": "y"},
                {"name": "z"},
            ],
            "relations": [
                {"order": 0, "customer": 0}, {"order": 0, "customer": 1}, {"order": 0, "customer": 2},
                {"order": 1, "customer": 0}
            ]
        }
        self._makeOrderCustomersStructure(structure)
        fields = ["xxxx", "name", "memo1"]

        prefetch = Prefetch("customers", queryset=m.Customer.objects.filter(name="z"))
        for msg, before_count, after_count, qs_filter in [
                ("with prefetch related", 2, 2, lambda qs: qs.all().prefetch_related(prefetch)),
                # ("without prefetch related", 2, 2, lambda qs: qs.all()),
        ]:
            with self.subTest(msg=msg, before_count=before_count, after_count=after_count, qs_filter=qs_filter):
                qs = qs_filter(m.Order.objects)
                optimized = self._callFUT(qs, fields).prefetch_filter(customers__name="z")

                self.assertNotIn("JOIN", str(qs.query))
                with self.assertNumQueries(before_count):
                    customers = [(o.id, o.name, c.id, c.name) for o in qs for c in o.customers.all()]
                    self.assertEqual(len(customers), 1)
                    self.assertIn("memo3", str(qs.query))

                # self.assertNotIn("JOIN", str(optimized.query))
                with self.assertNumQueries(after_count):
                    customers = [(o.id, o.name, c.id, c.name) for o in optimized for c in o.customers.all()]
                    print(customers)
                    # self.assertEqual(len(customers), 1)
                    # self.assertIn("memo3", str(qs.query))
