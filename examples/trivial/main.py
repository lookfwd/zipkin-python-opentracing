"""Simple example showing several generations of spans in a trace.
"""
import argparse
import contextlib
import sys
import time
import traceback

import opentracing
import zipkin_ot.tracer

def sleep_dot():
    """Short sleep and writes a dot to the STDOUT.
    """
    time.sleep(0.05)
    sys.stdout.write('.')
    sys.stdout.flush()

def add_spans():
    """Calls the opentracing API, doesn't use any OpenZipkin-specific code.
    """
    with opentracing.tracer.start_span(operation_name='trivial/initial_request') as parent_span:
        parent_span.set_tag('url', 'localhost')
        sleep_dot()
        parent_span.log_event('All good here!', payload={'N': 42, 'pi': 3.14, 'abc': 'xyz'})
        parent_span.log_kv({'foo': 'bar'})
        parent_span.set_tag('span_type', 'parent')
        parent_span.set_tag('int_tag', 5)
        parent_span.set_tag('unicode_val', u'non-ascii: \u200b')
        parent_span.set_tag('bool_tag', True)
        parent_span.set_baggage_item('checked', 'baggage')
        sleep_dot()

        # This is how you would represent starting work locally.
        with opentracing.start_child_span(parent_span, operation_name='trivial/child_request') as child_span:
            child_span.set_tag('span_type', 'child')
            # Pretend there was an error
            child_span.set_tag('error', True)
            child_span.log_event('Uh Oh!', payload={'stacktrace': traceback.extract_stack()})
            sleep_dot()

            # Play with the propagation APIs... this is not IPC and thus not
            # where they're intended to be used.
            text_carrier = {}
            opentracing.tracer.inject(child_span.context, opentracing.Format.TEXT_MAP, text_carrier)

            span_context = opentracing.tracer.extract(opentracing.Format.TEXT_MAP, text_carrier)
            with opentracing.tracer.start_span(
                'trivial/remote_span',
                child_of=span_context) as remote_span:
                    remote_span.log_event('Remote!')
                    remote_span.set_tag('span_type', 'remote')
                    sleep_dot()

def zipkin_ot_tracer_from_args():
    """Initializes OpenZipkin from the commandline args.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='The OpenZipkin reporting service host to contact.',
                        default='localhost')
    parser.add_argument('--port', help='The OpenZipkin reporting service port.',
                        type=int, default=9411)
    parser.add_argument('--service_name', help='The OpenZipkin component name',
                        default='TrivialExample')
    args = parser.parse_args()

    return zipkin_ot.Tracer(
            service_name=args.service_name,
            collector_host=args.host,
            collector_port=args.port,
            verbosity=1)


if __name__ == '__main__':
    print 'Hello ',

    # Use OpenZipkin's opentracing implementation
    with zipkin_ot_tracer_from_args() as tracer:
        opentracing.tracer = tracer
        add_spans()

    print ' World!'
