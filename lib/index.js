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
          return msg.skip().then(() => {

            return next();
          });
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

  _flush() {

    clearTimeout(this._timer);
    this._timer = undefined;
    return this.processor(Array.from(this._queue).map(item => item.result)).then(() => {

      return Promise.all(Array.from(this._queue).map((item) => {

        return item.msg.ack().then(() => {

          this._queue.delete(item);
          return Promise.resolve();
        });
      }));
    }).catch((err) => {

      this.emit('error', err);
      return Promise.all(Array.from(this._queue).map((item) => {

        return item.msg.skip().then(() => {

          this._queue.delete(item);
          return Promise.resolve();
        });
      }));
    });
  }
}

module.exports = CloudSubscriber;
