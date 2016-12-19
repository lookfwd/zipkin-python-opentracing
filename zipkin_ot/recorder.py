"""
OpenZipkin's implementations of the basictracer Recorder API.

https://github.com/opentracing/basictracer-python

See the API definition for comments.
"""

import atexit
import ssl
import sys
import threading
import time
import traceback
import warnings
import requests

from basictracer.recorder import SpanRecorder

from zipkin_ot.thrift import annotation_list_builder
from zipkin_ot.thrift import binary_annotation_list_builder
from zipkin_ot.thrift import create_span
from zipkin_ot.thrift import to_thrift_spans
from zipkin_ot.thrift import thrift_obj_in_bytes
from zipkin_ot.thrift import create_endpoint

from . import constants, util


STANDARD_ANNOTATIONS = {
    'client': {'cs':[], 'cr':[]},
    'server': {'ss':[], 'sr':[]},
}
STANDARD_ANNOTATIONS_KEYS = frozenset(STANDARD_ANNOTATIONS.keys())


class Recorder(SpanRecorder):
    """Recorder translates, buffers, and reports basictracer.BasicSpans.

    These reports are sent to a OpenTracing collector at the provided address.

    For parameter semantics, see Tracer() documentation; Recorder() respects
    service_name, collector_host, collector_port,
    max_span_records, periodic_flush_seconds, verbosity,
    and certificate_verification.

    :param port: The port number of the service. Defaults to 0.

    """
    def __init__(self,
                 service_name=None,
                 collector_host='localhost',
                 collector_port=9411,
                 max_span_records=constants.DEFAULT_MAX_SPAN_RECORDS,
                 periodic_flush_seconds=constants.FLUSH_PERIOD_SECS,
                 verbosity=0,
                 include=('client', 'server'),
                 port=0,
                 certificate_verification=True):
        self.verbosity = verbosity

        if certificate_verification is False:
            warnings.warn('SSL CERTIFICATE VERIFICATION turned off. '
                          'ALL FUTURE HTTPS calls will be unverified.')
            ssl._create_default_https_context = ssl._create_unverified_context

        if service_name is None:
            service_name = sys.argv[0]

        self.endpoint = create_endpoint(port, service_name)

        if not set(include).issubset(STANDARD_ANNOTATIONS_KEYS):
            raise Exception(
                'Only %s are supported as annotations' %
                STANDARD_ANNOTATIONS_KEYS
            )
        else:
            # get a list of all of the mapped annotations
            self.annotation_filter = set()
            for include_name in include:
                self.annotation_filter.update(STANDARD_ANNOTATIONS[include_name])

        self._collector_url = util.collector_url_from_hostport(
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

        annotations = {}
        binary_annotations = {}
        annotation_filter = self.annotation_filter

        if span.tags:
            for key in span.tags:
                # You might want to handle key[:len(constants.JOIN_ID_TAG_PREFIX)] ==
                # constants.JOIN_ID_TAG_PREFIX) differently.
                binary_annotations[key] = util.coerce_str(span.tags[key])

        for log in span.logs:
            event = log.key_values.get('event') or ''
            if len(event) > 0:
                # Don't allow for arbitrarily long log messages.
                if sys.getsizeof(event) > constants.MAX_LOG_MEMORY:
                    event = event[:constants.MAX_LOG_LEN]
            payload = log.key_values.get('payload')
            if event == 'include':
                annotation_filter = set()
                for include_name in payload:
                    annotation_filter.update(STANDARD_ANNOTATIONS[include_name])
            else:
                binary_annotations["%s@%s" % (event, str(log.timestamp))] = payload

        # To get a full span we just set cs=sr and ss=cr.
        full_annotations = {
            'cs': span.start_time,
            'sr': span.start_time
        }
        if span.duration != -1:
            full_annotations['ss'] = span.start_time + span.duration
            full_annotations['cr'] = full_annotations['ss']

        # But we filter down if we only want to emit some of the annotations
        filtered_annotations = {}
        for k, v in full_annotations.items():
            if k in annotation_filter:
                filtered_annotations[k] = v

        annotations.update(filtered_annotations)

        thrift_annotations = annotation_list_builder(
            annotations, self.endpoint
        )
        thrift_binary_annotations = binary_annotation_list_builder(
            binary_annotations, self.endpoint
        )

        span_record = create_span(
            util.id_to_hex(span.context.span_id),
            util.id_to_hex(span.parent_id),
            util.id_to_hex(span.context.trace_id),
            util.coerce_str(span.operation_name),
            thrift_annotations,
            thrift_binary_annotations,
        )

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

        span_records = None
        with self._mutex:
            span_records = self._span_records
            self._span_records = []

        try:
            self._finest("Attempting to send records to collector: %s", (
                span_records,))

            self._finest("encoded span: %s", (span_records,))

            # Report to the server.
            # The collector expects a thrift-encoded list of spans. We
            # encode a full struct
            body = thrift_obj_in_bytes(to_thrift_spans(span_records))[3:-1]
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
            return len(span_records) > 0

        except Exception as e:
            self._fine("Caught exception during report: %s, stack "
                       "trace: %s", (e, traceback.format_exc(e)))
            self._restore_spans(span_records)
            return False

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
