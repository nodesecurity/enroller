'use strict';

const Assert = require('assert');
const EventEmitter = require('events').EventEmitter;
const PubSub = require('@google-cloud/pubsub');
const Transform = require('stream').Transform;
const Waterfall = require('p-waterfall');

class CloudSubscriber extends EventEmitter {
  constructor(options = {}) {

    super();

    Assert(typeof options.name === 'string', 'Name must be a string');
    Assert(options.name !== '', 'Name cannot be empty');
    this.name = options.name;

    Assert(typeof options.topic === 'string', 'Topic must be a string');
    Assert(options.topic !== '', 'Topic cannot be empty');
    this.topic = options.topic;

    this.batchSize = options.batchSize || Infinity;

    if (options.middleware) {
      options.middleware = [].concat(options.middleware);
      for (const middleware of options.middleware) {
        Assert(typeof middleware === 'function', 'Middleware must be a function');
      }
    }

    Assert(typeof options.processor === 'function', 'Processor must be a function');

    this._queue = new Set();
    this._flushing = false;
    this.middleware = options.middleware;
    this.processor = options.processor;
    this.pubsub = PubSub(options.auth);

    this._handleMessage = this._handleMessage.bind(this);
    this._handleError = this._handleError.bind(this);
  }

  _handleMessage(msg) {

    this.emit('message', msg);
    this._transformer.write(msg);
  }

  _handleError(err) {

    this.emit('error', err);
  }

  start() {

    const self = this;
    const transformer = function (msg, _, next) {

      if (!self._timer) {
        self._timer = setTimeout(() => {

          self._flush();
        }, 1000 * 60 * 9); // 9 minutes
      }

      if (self.middleware) {
        return Waterfall(self.middleware, msg.data).then((result) => {

          this.push({ result, msg });
          return next();
        }).catch((err) => {

          self.emit('error', err);
          msg.skip();
          return next();
        });
      }

      this.push({ result: msg.data, msg });
      return next();
    };

    this._transformer = new Transform({
      transform: transformer,
      objectMode: true
    });

    const batcher = function (msg, _, next) {

      self._queue.add(msg);
      if (self.subscription.maxInProgress === Infinity ||
          self._queue.size >= self.subscription.maxInProgress) {

        return self._flush().then(() => {

          return next();
        }).catch((err) => {

          self.emit('error', err);
          return next();
        });
      }

      return next();
    };

    this._batcher = new Transform({
      transform: batcher,
      objectMode: true
    });

    return this.pubsub.subscribe(this.topic, this.name, { maxInProgress: this.batchSize, ackDeadlineSeconds: 600 }).then(([subscription]) => {

      this.subscription = subscription;
      this._transformer.pipe(this._batcher);
      this._transformer.resume();

      this.subscription.on('message', this._handleMessage);
      this.subscription.on('error', this._handleError);
    });
  }

  stop() {

    this.subscription.removeListener('message', this._handleMessage);
    this.subscription.removeListener('error', this._handleError);
    return this._flush().then(() => {

      this._transformer.end();
    });
  }

  _wait() {

    return new Promise((resolve) => {

      setImmediate(() => {

        if (this._flushing) {
          return this._wait();
        }

        return resolve();
      });
    });
  }

  _flush() {

    if (this._flushing) {
      return this._wait();
    }

    this._flushing = true;

    clearTimeout(this._timer);
    this._timer = undefined;

    if (this._queue.size === 0) {
      return Promise.resolve();
    }

    const queue = Array.from(this._queue);

    return this.processor(queue.map(item => item.result)).then(() => {

      return Promise.all(queue.map((item) => {

        return item.msg.ack().then(() => {

          this._queue.delete(item);
          return Promise.resolve();
        });
      }));
    }).catch((err) => {

      this.emit('error', err);
      return Promise.all(queue.map((item) => {

        item.msg.skip();
        this._queue.delete(item);
        return Promise.resolve();
      }));
    }).then(() => {

      this._flushing = false;
    });
  }
}

module.exports = CloudSubscriber;
