import unittest

import opentracing
from opentracing.harness.api_check import APICompatibilityCheckMixin

import zipkin_ot.tracer


class OpenzipkinTracerOpenTracingCompatibility(unittest.TestCase, APICompatibilityCheckMixin):
    def setUp(self):
        self._tracer = zipkin_ot.Tracer(
                periodic_flush_seconds=0,
                collector_host='localhost')

    def tracer(self):
        return self._tracer

    def tearDown(self):
        self._tracer.flush()


if __name__ == '__main__':
    unittest.main()
