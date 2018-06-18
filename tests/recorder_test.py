import json
import time
import unittest
import warnings

import jsonpickle

import zipkin_ot.constants
import zipkin_ot.recorder
import zipkin_ot.tracer
import zipkin_ot.recorder
from basictracer.span import BasicSpan
from basictracer.context import SpanContext

from zipkin_ot.thrift import spans_from_bytes

from zipkin_ot import thrift

from collections import namedtuple


class ReportResponse(object):

    def __init__(self):
        self.status_code = 200

    def raise_for_status(self):
        pass


SpanRecord = namedtuple('SpanRecord', 'url, data, headers')


class MockConnection(object):
    """MockConnection is used to debug and test Runtime.
    """
    def __init__(self):
        self.reports = []

    def post(self, url, data, headers):
        """Mimic the Thrift client's report method. Instead of sending report
            requests save them to a list.
        """
        self.reports.append(SpanRecord(url, data, headers))
        return ReportResponse()

    def clear(self):
        self.reports = []


class RecorderTest(unittest.TestCase):
    """Unit Tests
    """
    def setUp(self):
        self.mock_connection = MockConnection()
        self.runtime_args = {'collector_host': 'localhost',
                             'collector_port': 9411,
                             'service_name': 'python/runtime_test',
                             'periodic_flush_seconds': 0,
                             'verbosity': 1}

    def create_test_recorder(self):
        """Returns a Openzipkin Recorder based on self.runtime_args.
        """
        return zipkin_ot.recorder.Recorder(**self.runtime_args)

    # -------------
    # SHUTDOWN TESTS
    # -------------
    def test_send_spans_after_shutdown(self):
        recorder = self.create_test_recorder()

        # Send 10 spans
        for i in range(10):
            recorder.record_span(self.dummy_basic_span(recorder, i))
        self.assertTrue(recorder.flush(self.mock_connection))

        # Check 10 spans
        self.check_spans(self.mock_connection.reports)

        # Delete current logs and shutdown runtime
        self.mock_connection.clear()
        recorder.shutdown()

        # Send 10 spans, though none should get through
        for i in range(10):
            recorder.record_span(self.dummy_basic_span(recorder, i))
        self.assertFalse(recorder.flush(self.mock_connection))
        self.assertEqual(len(self.mock_connection.reports), 0)

    def test_shutdown_twice(self):
        recorder = self.create_test_recorder()
        recorder.shutdown()
        recorder.shutdown()

    # ------------
    # STRESS TESTS
    # ------------
    def test_stress_logs(self):
        recorder = self.create_test_recorder()
        for i in range(1000):
            recorder.record_span(self.dummy_basic_span(recorder, i))
        self.assertTrue(recorder.flush(self.mock_connection))
        self.assertEqual(len(RecorderTest.decode_span_array(self.mock_connection.reports[0].data)), 1000)
        self.check_spans(self.mock_connection.reports)

    def test_stress_spans(self):
        recorder = self.create_test_recorder()
        for i in range(1000):
            recorder.record_span(self.dummy_basic_span(recorder, i))
        self.assertTrue(recorder.flush(self.mock_connection))
        self.assertEqual(len(RecorderTest.decode_span_array(self.mock_connection.reports[0].data)), 1000)
        self.check_spans(self.mock_connection.reports)

    # -------------
    # RUNTIME TESTS
    # -------------

    def test_buffer_limits(self):
        self.runtime_args.update({
            'max_span_records': 88,
        })
        recorder = self.create_test_recorder()

        self.assertEqual(len(recorder._span_records), 0)
        for i in range(0, 10000):
            recorder.record_span(self.dummy_basic_span(recorder, i))
        self.assertEqual(len(recorder._span_records), 88)
        self.assertTrue(recorder.flush(self.mock_connection))

    @staticmethod
    def decode_span_array(data):
        to_object = b'\x0f\x00\x01' + data + b'\x00'

        return spans_from_bytes(to_object).spans

    # ------
    # HELPER
    # ------
    def check_spans(self, reports):
        """Checks spans' name.
        """
        id = 0

        for report in reports:
            spans = RecorderTest.decode_span_array(report.data)

            for span in spans:
                self.assertEqual(span.name, str(id))

                id += 1

    def dummy_basic_span(self, recorder, i):
        return BasicSpan(
            zipkin_ot.tracer._OpenZipkinTracer(recorder),
            operation_name=str(i),
            context=SpanContext(
                trace_id=1000+i,
                span_id=2000+i),
            start_time=time.time())


if __name__ == '__main__':
    unittest.main()
