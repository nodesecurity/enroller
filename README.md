Enroller is a module to help simplify the process of subscribing to Google Cloud PubSub events. It also allows for batch processing of messages.

## Usage

### `new Enroller(options)`

Where `options` is an object with the following properties:

- `auth`: (object) authentication options passed through to the `@google-cloud/pubsub` constructor
- `name`: (string) the name of the subscription to create
- `topic`: (string) the name of the topic to subscribe to
- `batchSize`: (number) when defined, the upper limit of messages to pass to the `processor` function. defaults to `Infinity` which causes each message to be passed to the `processor` immediately without batching.
- `processor`: (function) the function to be called when batches are ready (or immediately for each message if `batchSize` is undefined). accepts one argument, an array of values either directly from your topic subscription or the result of your middleware stack. must return a promise.
- `middleware`: (array of functions) an optional set of functions to pass messages through in order to allow individual transformations. accepts a single argument, the data either from the topic subscription or from the result of the previous function in the middleware stack. each function must return a promise.

### `enroller.start()`

Start the subscription flow, returns a promise.

### `enroller.stop()`

Stop the subscription flow, returns a promise.

### `enroller.on(event, fn)`

The enroller object is an event emitter. There are two possible events that can be emitted, `error` and `message`. The `message` event is informational and useful for actions like logging. The `error` event will be emitted any time the subscription itself has an error, any function in the middleware stack fails, or the processor function fails.

## Message flow, acks and skips

Enroller creates a subscription for the defined topic with the defined name. It sets an ack timeout of 10 minutes to facilitate batch processing.

When a message is receieved from Cloud PubSub it immediately passes the data from the message through the middleware stack. The default middleware stack simply passes the message through untouched.

Once the middleware stack is complete, if `batchSize` is unset the result of the middleware stack will be passed to the `processor`. If `batchSize` is set, the message will be placed into a queue to allow the `processor` to receive multiple messages at once.

If any function in the middleware stack fails, the message will be skipped (`message.skip()`) to allow it to be retried at a later time.

Once a message is placed in the queue a timer with a 9 minute duration is started. If the timer runs out before the queue reaches the defined `batchSize` the `processor` will be called with the items that are currently in the queue to prevent the ack timeout from being reached.

Once the `batchSize` is reached, the queue will be passed to the `processor` function.

If the `processor` fails, *every message in the queue* will be skipped. This is to make sure that no message fails without the possibility of retrying. Once the `processor` completes successfully each message in the queue will be acked. In either event, the queue is then cleared and the process starts over.
