
'use strict';

//-
// Handling the response. See <a href="https://developers.google.com/bigquery/troubleshooting-errors">
// Troubleshooting Errors</a> for best practices on how to handle errors.
//-
function insertHandler(err, apiResponse) {
    if (err) {
      // An API error or partial failure occurred.
      console.error("Failed to insert table row(s): "+JSON.stringify(err.errors))
  
      if (err.name === 'PartialFailureError') {
        // Some rows failed to insert, while others may have succeeded.
  
        // err.errors (object[]):
        // err.errors[].row (original row object passed to `insert`)
        // err.errors[].errors[].reason
        // err.errors[].errors[].message
      }
    }
  }
  
  /**
   * Background Cloud Function to be triggered by Pub/Sub.
   * This function is exported by index.js, and executed when
   * the trigger topic receives a message.
   * 
   * gcloud functions deploy helloPubSub --runtime nodejs6 --trigger-resource TOPIC_NAME --trigger-event google.pubsub.topic.publish
   * TOPIC_NAME should be ...
   *
   * @param {object} event The Cloud Functions event.
   * @param {function} callback The callback function.
   */
  exports.saveLogEventsToBigQuery = (event, callback) => {
  
    const config = require('./config.json');
    const {BigQuery} = require('@google-cloud/bigquery');

    const bigQuery = new BigQuery({ projectId: process.env.GCLOUD_PROJECT });
  
    const pubsubMessage = event.data;
    const eventPayload = pubsubMessage.data ? Buffer.from(pubsubMessage.data, 'base64').toString() : null;
    if (eventPayload != null){
        console.log('received event '+eventPayload);
  
        var transformedPayload = JSON.parse(eventPayload); // Copy over all data.
        transformedPayload.Data = JSON.stringify(transformedPayload.Data); // Stringify Data-field so we can log all events to the same primary table.  
        transformedPayload.ReceivedAt =  bigQuery.timestamp(new Date());
    
        const row = transformedPayload;
      
        bigQuery.dataset(config.LOG_EVENT.DATASET_NAME)
          .table(config.LOG_EVENT.TABLE_ID)
          .insert(row, insertHandler);    
    }
    else {
        console.error("Failed to parse pubsub message: "+pubsubMessage.data)
    }

    callback();
  };
  