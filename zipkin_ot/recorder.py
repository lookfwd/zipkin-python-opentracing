"""
OpenZipkin's implementations of the basictracer Recorder API.

https://github.com/opentracing/basictracer-python

See the API definition for comments.
"""

from socket import error as socket_error

import atexit
import contextlib
import jsonpickle
import logging
import pprint
import ssl
import sys
import threading
import time
import traceback
import warnings
import json
import requests

from collections import namedtuple

from basictracer.recorder import SpanRecorder

from zipkin_ot.thrift import annotation_list_builder
from zipkin_ot.thrift import binary_annotation_list_builder
from zipkin_ot.thrift import copy_endpoint_with_new_service_name
from zipkin_ot.thrift import create_span
from zipkin_ot.thrift import thrift_obj_in_bytes

from . import constants, version as tracer_version, util


class Recorder(SpanRecorder):
    """Recorder translates, buffers, and reports basictracer.BasicSpans.

    These reports are sent to a OpenTracing collector at the provided address.

    For parameter semantics, see Tracer() documentation; Recorder() respects
    component_name, collector_host, collector_port,
    tags, max_span_records, periodic_flush_seconds, verbosity,
    and certificate_verification.
    """
    def __init__(self,
                 component_name=None,
                 collector_host='localhost',
                 collector_port=8080,
                 tags=None,
                 max_span_records=constants.DEFAULT_MAX_SPAN_RECORDS,
                 periodic_flush_seconds=constants.FLUSH_PERIOD_SECS,
                 verbosity=0,
                 certificate_verification=True):
        self.verbosity = verbosity

        if certificate_verification is False:
            warnings.warn('SSL CERTIFICATE VERIFICATION turned off. '
                          'ALL FUTURE HTTPS calls will be unverified.')
            ssl._create_default_https_context = ssl._create_unverified_context

        if component_name is None:
            component_name = sys.argv[0]

        # Thrift runtime configuration
        self.guid = util._generate_guid()
        timestamp = util._now_micros()

        python_version = '.'.join(map(str, sys.version_info[0:3]))
        if tags is None:
            tags = {}
        tracer_version_s = tracer_version.OPENZIPKIN_OT_PYTHON_TRACER_VERSION
        tracer_tags = tags.copy()
        tracer_tags.update({
            'zipkin_ot.tracer_platform': 'python',
            'zipkin_ot.tracer_platform_version': python_version,
            'zipkin_ot.tracer_version': tracer_version_s,
            'zipkin_ot.component_name': component_name,
            'zipkin_ot.guid': util._id_to_hex(self.guid),
            })
        # Convert tracer_tags to a list of KeyValue pairs.
        # runtime_attrs = [thrift.KeyValue(k, util._coerce_str(v))
        #                  for (k, v) in tracer_tags.iteritems()]

        # Thrift is picky about the types being correct, so we're explicit here
        # self._runtime = thrift.Runtime(
        #         util._id_to_hex(self.guid),
        #         long(timestamp),
        #         util._coerce_str(component_name),
        #         runtime_attrs)
        # self._finest("Initialized with Tracer runtime: %s", (self._runtime,))
        self._collector_url = util._collector_url_from_hostport(
            collector_host,
            collector_port)
        self._mutex = threading.Lock()
        self._span_records = []
        self._max_span_records = max_span_records

        self._disabled_runtime = False
        atexit.register(self.shutdown)

        self._periodic_flush_seconds = periodic_flush_seconds
        # _flush_thread is created lazily since some
        # Python environments (e.g., Tornado) fork() initially and mess up the
        # reporting machinery up otherwise.
        self._flush_thread = None
        if self._periodic_flush_seconds <= 0:
            warnings.warn(
                'Runtime(periodic_flush_seconds={0}) means we will never'
                ' flush to zipkin_ot unless explicitly requested.'.format(
                    self._periodic_flush_seconds))

    def _maybe_init_flush_thread(self):
        """Start a periodic flush mechanism for this recorder if:

        1. periodic_flush_seconds > 0, and
        2. self._flush_thread is None, indicating that we have not yet
           initialized the background flush thread.

        We do these things lazily because things like `tornado` break if the
        background flush thread starts before `fork()` calls happen.
        """
        if ((self._periodic_flush_seconds > 0) and
           (self._flush_thread is None)):
            self._flush_thread = threading.Thread(
                target=self._flush_periodically,
                name=constants.FLUSH_THREAD_NAME)
            self._flush_thread.daemon = True
            self._flush_thread.start()

    def _fine(self, fmt, args):
        if self.verbosity >= 1:
            print "[Zipkin_OpenTracing Tracer]:", (fmt % args)

    def _finest(self, fmt, args):
        if self.verbosity >= 2:
            print "[Zipkin_OpenTracing Tracer]:", (fmt % args)

    def record_span(self, span):
        """Per BasicSpan.record_span, safely add a span to the buffer.

        Will drop a previously-added span if the limit has been reached.
        """
        if self._disabled_runtime:
            return

        # Lazy-init the flush loop (if need be).
        self._maybe_init_flush_thread()

        # Checking the len() here *could* result in a span getting dropped that
        # might have fit if a report started before the append(). This would
        # only happen if the client lib was being saturated anyway (and likely
        # dropping spans). But on the plus side, having the check here avoids
        # doing a span conversion when the span will just be dropped while also
        # keeping the lock scope minimized.
        with self._mutex:
            if len(self._span_records) >= self._max_span_records:
                return

        SpanRecord = namedtuple('SpanRecord', 'trace_guid, span_guid, span_name, log_records')
        span_record = SpanRecord(
            trace_guid=util._id_to_hex(span.context.trace_id),
            span_guid=util._id_to_hex(span.context.span_id),
            span_name=util._coerce_str(span.operation_name),
            log_records=[])
        # span_record = thrift.SpanRecord(
        #     trace_guid=util._id_to_hex(span.context.trace_id),
        #     span_guid=util._id_to_hex(span.context.span_id),
        #     runtime_guid=util._id_to_hex(self.guid),
        #     span_name=util._coerce_str(span.operation_name),
        #     join_ids=[],
        #     oldest_micros=long(util._time_to_micros(span.start_time)),
        #     youngest_micros=long(util._time_to_micros(span.start_time +
        #     span.duration)),
        #     attributes=[],
        #     log_records=[]
        # )

        # if span.parent_id != None:
        #     span_record.attributes.append(
        #         thrift.KeyValue(
        #             constants.PARENT_SPAN_GUID,
        #             util._id_to_hex(span.parent_id)))
        # if span.tags:
        #     for key in span.tags:
        #         if (key[:len(constants.JOIN_ID_TAG_PREFIX)] ==
        #             constants.JOIN_ID_TAG_PREFIX):
        #             span_record.join_ids.append(
        #  thrift.TraceJoinId(key, util._coerce_str(span.tags[key])))
        #         else:
        #             span_record.attributes.append(
        #  thrift.KeyValue(key, util._coerce_str(span.tags[key])))

        for log in span.logs:
            event = log.key_values.get('event') or ''
            if len(event) > 0:
                # Don't allow for arbitrarily long log messages.
                if sys.getsizeof(event) > constants.MAX_LOG_MEMORY:
                    event = event[:constants.MAX_LOG_LEN]
            payload = log.key_values.get('payload')
            # span_record.log_records.append(thrift.LogRecord(
            #     timestamp_micros=long(util._time_to_micros(log.timestamp)),
            #     stable_name=event,
            #     payload_json=payload))

        with self._mutex:
            if len(self._span_records) < self._max_span_records:
                self._span_records.append(span_record)

    def flush(self, connection=None):
        """Immediately send unreported data to the server.

        Calling flush() will ensure that any current unreported data will be
        immediately sent to the host server.

        If connection is not specified, the report will sent to the server
        passed in to __init__.  Note that custom connections are currently used
        for unit testing against a mocked connection.

        Returns whether the data was successfully flushed.
        """
        if self._disabled_runtime:
            return False

        self._maybe_init_flush_thread()
        return self._flush_worker(connection)

    def shutdown(self, flush=True):
        """Shutdown the Runtime's connection by (optionally) flushing the
        remaining logs and spans and then disabling the Runtime.

        Note: spans and logs will no longer be reported after shutdown is
        called.

        Returns whether the data was successfully flushed.
        """
        # Closing connection twice results in an error. Exit early
        # if runtime has already been disabled.
        if self._disabled_runtime:
            return False

        if flush:
            flushed = self.flush()

        self._disabled_runtime = True

        return flushed

    def _flush_periodically(self):
        """Periodically send reports to the server.

        Runs in a dedicated daemon thread (self._flush_thread).
        """

        # Send data until we get disabled
        while not self._disabled_runtime:
            self._flush_worker()
            time.sleep(self._periodic_flush_seconds)

    def _flush_worker(self, connection=None):
        """Use the given connection to transmit the current logs and spans as a
        report request."""

        # Nothing todo anyway (also makes tests pass by ignoring on last 
        # flush())
        if not self._span_records:
            return True

        report_request = self._construct_report_request()
        try:
            self._finest("Attempting to send report to collector: %s", (
                report_request,))

            for i, raw_span in enumerate(report_request.span_records):
                self._finest("encoded span: %d", (i,))

                # Report to the server.
                # The collector expects a thrift-encoded list of spans. Instead
                # of decoding and re-encoding the already thrift-encoded
                # message, we can just add header bytes that specify that what
                # follows is a list of length 1.
                #'\x0c\x00\x00\x00\x01
                body = json.dumps(raw_span)
                args = {
                    "url": self._collector_url,
                    "data": body,
                    "headers": {'Content-Type': 'application/x-thrift'}
                }
                if connection:
                    r = connection.post(**args)
                else:
                    r = requests.post(**args)

                r.raise_for_status()

                self._finest("Received response from collector: %s",
                             (r.status_code,))

            # Return whether we sent any span data
            return len(report_request.span_records) > 0

        except Exception as e:
            self._fine("Caught exception during report: %s, stack "
                       "trace: %s", (e, traceback.format_exc(e)))
            self._restore_spans(report_request.span_records)
            return False

    def _construct_report_request(self):
        """Construct a report request."""
        report = None
        with self._mutex:
            ReportRequest = namedtuple('ReportRequest', 'span_records')
            report = ReportRequest(self._span_records)
            self._span_records = []
        for span in report.span_records:
            for log in span.log_records:
                index = span.log_records.index(log)
                if log.payload_json is not None:
                    try:
                        log.payload_json = jsonpickle.encode(
                            log.payload_json,
                            unpicklable=False,
                            make_refs=False,
                            max_depth=constants.JSON_MAX_DEPTH)
                    except:
                        log.payload_json = jsonpickle.encode(
                            constants.JSON_FAIL)
                span.log_records[index] = log
        return report

    def _restore_spans(self, span_records):
        """Called after a flush error to move records back into the buffer
        """
        if self._disabled_runtime:
            return

        with self._mutex:
            if len(self._span_records) >= self._max_span_records:
                return
            combined = span_records + self._span_records
            self._span_records = combined[-self._max_span_records:]
