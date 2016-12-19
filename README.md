# openzipkin-tracer-python

[![CircleCI](https://circleci.com/gh/lookfwd/zipkin-python-opentracing.svg?style=svg)](https://circleci.com/gh/lookfwd/zipkin-python-opentracing)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The OpenZipkin OpenTracing (Open)Library for (Open)Python :D

## Installation

```bash
apt-get install python-dev
python setup.py install
#pip install zipkin-python-opentracing
```

## Getting started

Please see the [example programs](examples/) for examples of how to use this library. In particular:

* [Trivial Example](examples/trivial/main.py) shows how to use the library on a single host.

![trivial example](/docs/img/trivial-example.gif)

* [Non-trivial example](examples/nontrivial/main.py) is way more resource intensive multi-threaded example (takes ~5'):.

![non-trivial traces](/docs/img/non-trivial-example.gif)

* [Context in Headers](examples/http/context_in_headers.py) shows how to pass a `TraceContext` through `HTTP` headers.

![context in headers](/docs/img/context-in-headers.gif)

* [Interoperability](examples/interop/main.py) extends on the `HTTP` headers example by starting a node express server that receives a request, proxies it back to Python and then forwards the response.

![context in headers](/docs/img/interop-example.gif)

Or if your python code is already instrumented for OpenTracing, you can simply switch to OpenZipkin's implementation with:

```python
import opentracing
import zipkin_ot

if __name__ == "__main__":
  opentracing.tracer = zipkin_ot.Tracer(
    service_name='your_microservice_name')

  with opentracing.tracer.start_span('TestSpan') as span:
    span.log_event('test message', payload={'life': 42})

  opentracing.tracer.flush()
```

## Acknowledgments

Based (heavily) on and lots of credits to [lightstep](https://github.com/lightstep/lightstep-tracer-python) and [py_zipkin](https://github.com/Yelp/py_zipkin).

This library is the OpenZipkin binding for [OpenTracing](http://opentracing.io/). See the [OpenTracing Python API](https://github.com/opentracing/opentracing-python) for additional detail.

Copyright (c) 2016 The OpenTracing Authors.

