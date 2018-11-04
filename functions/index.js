
'use strict';

/**
* HTTP Cloud Function.
*
* @param {Object} req Cloud Function request context.
* @param {Object} res Cloud Function response context.
*/
exports.logEvents = function helloHttp (req, res) {

  const config = require('./config.json');

  // Get a reference to the Pub/Sub component
  const PubSub = require('@google-cloud/pubsub');

  const pubSubClient = new PubSub();

  if (req.method == 'POST') {

    try {

      console.log('Input: ', req.body);

      var entries = req.body;
      
      var publisher = pubSubClient
        .topic(config['TOPIC'])
        .publisher();

      Promise.all(entries.map((entry) => {
        const dataBuffer = Buffer.from(JSON.stringify(entry));
        return publisher.publish(dataBuffer);
      }))
      .then(() => {
        console.log('Logged stuff');
        res.status(200).send();
      })
      .catch((err) => {
        console.error('Internal error: ', err);
        res.status(500).send({ error: err });
      });
    }
    catch (err) {
      console.error('User error: ', err);
      res.status(400).send({ error: 'Malformed input'});
    }
  }
  else
    res.status(405).send({ error: 'logEvents only supports POST' });
};
