const express = require('express'),
      assert = require('assert'),
      http = require('http'),
      fetch = require('node-fetch'),
      wrapFetch = require('zipkin-instrumentation-fetch');

// Zipkin infrastructure
const {Tracer, ExplicitContext, BatchRecorder} = require('zipkin');
const {HttpLogger} = require('zipkin-transport-http');
const zipkinMiddleware = require('zipkin-instrumentation-express').expressMiddleware;

const ctxImpl = new ExplicitContext();

const logger = new HttpLogger({
  endpoint: 'http://localhost:9411/api/v1/spans',
  httpInterval: 100000 // Effectively disable this. Will do manual flush at the end
});
const recorder = new BatchRecorder({
  logger: logger
});

const tracer = new Tracer({ctxImpl, recorder}); // configure your tracer properly here

// Main app
var app = express()

// Add the Zipkin middleware
app.use(zipkinMiddleware({
  tracer,
  serviceName: 'node app'
}));

const remote = 'back-to-python';
const zipkinFetch = wrapFetch(fetch, {tracer, remoteServiceName: remote});

// Simple app

const my_port = process.argv[2];
const py_port = process.argv[3];

assert(my_port);
assert(py_port);

// Main endpoint. Sends info back to python
app.get('/', function (req, iRes) {

    zipkinFetch('http://localhost:' + py_port + '/')
    .then(function(res) {
        assert(res.ok);
        assert(res.status == 200);
        return res.text();
    })
    .then(function(res){
        iRes.send('Proxy: "' + res + '"')
    })
    .catch(function(err) {
        console.log(err);
    });
});

// This is necessary in order to shut down this server remotely

app.get('/shutdown', function (req, res) {
    res.send("bye");
    res.end();
    
    // Would be nice to have a flush()
    recorder.partialSpans.forEach((span, id) => {
        recorder._writeSpan(id);
    });
    
    if (logger.queue.length > 0) {
         const postBody = JSON.stringify(logger.queue);
         const p = fetch(logger.endpoint, {
           method: 'POST',
           body: postBody,
           headers: {
             Accept: 'application/json',
             'Content-Type': 'application/json'
           }
         })
         .then((response) => {
             if (response.status !== 202) {
               console.error('Unexpected response while sending Zipkin data, status:' +
                 `${response.status}, body: ${postBody}`);
             }
             process.exit();
         }).catch((error) => {
             console.error('Error sending Zipkin data', error);
             process.exit();
         });
     }
});

// The process that spawns this waits for "Started" to know
// that this server is ready

app.listen(my_port, function () {
    console.log('Started')
});



