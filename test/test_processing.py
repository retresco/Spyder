#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# test_processing.py 10-Jan-2011
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# under the License.
#
#

import unittest

from spyder.core import processing


class SampleFoo(processing.Command):
    def execute(self, context):
        context['foo'] = 'bar'
        return context


class SampleBar(processing.Command):
    def execute(self, context):
        context['bar'] = 'foo'
        return context



class SampleProcessingTest(unittest.TestCase):

    def test_sample_foo(self):
    
        foo = SampleFoo()
        ctx = { 'ha':'ha' }

        newctx = foo.execute( ctx )

        self.assertEqual( { 'ha' : 'ha', 'foo' : 'bar' }, newctx )


    def test_sample_bar(self):
        bar = SampleBar()
        ctx = { 'ha':'ha' }

        newctx = bar.execute( ctx )

        self.assertEqual( { 'ha' : 'ha', 'bar' : 'foo' }, newctx )


    def test_sample_chain(self):

        chain = processing.Chain( [ SampleFoo(), SampleBar() ] )
        ctx = { 'ha':'ha' }

        newctx = chain.execute( ctx )

        self.assertEqual( { 'ha' : 'ha', 'foo' : 'bar', 'bar' : 'foo' }, newctx )

