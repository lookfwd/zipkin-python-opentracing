from setuptools import setup, find_packages

setup(
    name='zipkin-python-opentracing',
    version='1.0.0',
    description='OpenZipkin Python OpenTracing Implementation',
    long_description='',
    author='OpenZipkin',
    license='',
    install_requires=['thrift==0.9.2',
                      'jsonpickle',
                      'pytest',
                      'thriftpy',
                      'requests',
                      'basictracer>=2.2,<2.3'],
    tests_require=['sphinx',
                   'sphinx-epytext'],

    classifiers=[
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
    ],

    keywords=[ 'opentracing', 'openzipkin', 'traceguide', 'tracing', 'microservices', 'distributed' ],
    packages=find_packages(exclude=['docs*', 'tests*', 'sample*']),
    package_data={'': ['*.thrift']},
)
