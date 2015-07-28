# encoding=utf-8
from __future__ import unicode_literals

import unittest

from ..models import Attribute

class TestAttribute(unittest.TestCase):
    def test_from_document(self):
        attr = Attribute(default='')
        self.assertEqual('', attr.default)
        self.assertEqual('foo', attr.from_document('foo'))

    def test_to_document(self):
        attr = Attribute(default='')
        self.assertEqual('', attr.default)
        self.assertEqual('foo', attr.to_document('foo'))