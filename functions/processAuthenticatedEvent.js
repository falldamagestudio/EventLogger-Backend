
'use strict';

const config = require('./config.json');

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
  

  /*
function PlayerSessionsTableLocation(){
    return process.env.GCLOUD_PROJECT+'.'+config.PLAYER_SESSIONS.DATASET_NAME+'.'+config.PLAYER_SESSIONS.TABLE_ID;
}
*/

/*
function getRowForPlayerId(playerId, bigQuery, callbackInsertPlayerSession, payloadJson){
    const query = 'SELECT * FROM `'+PlayerSessionsTableLocation()+'` LIMIT 10';
    console.log("Querying DB: "+query);
    bigQuery.query(query, function(err, rows) {
        if (!err) {
            // rows is an array of results.
            console.log("Result from query: "+JSON.stringify(rows));
            if (rows.length > 0)
                return callbackInsertPlayerSession(rows[0], bigQuery, payloadJson);
            return callbackInsertPlayerSession(null, bigQuery, payloadJson);
        }
    });
}*/

/*
function deleteRows(withPlayerId, bigQuery){
    var deleteQuery = "DELETE FROM "+PlayerSessionsTableLocation()+" WHERE PlayerId = "+withPlayerId;
    console.log("Delete query: "+deleteQuery);
    bigQuery.query(deleteQuery, function(err, rows) {
        if (!err) {
            // rows is an array of results.
            console.log("Deleted previous entries successfully");
        }
    });
}*/

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
exports.processAuthenticatedEvent = (event, callback) => {
    const pubsubMessage = event.data;
    const eventPayload = pubsubMessage.data ? Buffer.from(pubsubMessage.data, 'base64').toString() : null;
    var payloadJson = JSON.parse(eventPayload); // Copy over all data.
    if (payloadJson.Type == "AuthenticatedEvent")
    {
        console.log("Payload: "+JSON.stringify(payloadJson));
        payloadJson.PlayerId = payloadJson.Data.PlayerId;
        console.log("Authenticated event intercepted, checking if row currently exists");
        const {BigQuery} = require('@google-cloud/bigquery');
        const bigQuery = new BigQuery({ projectId: process.env.GCLOUD_PROJECT });   
    
        const row = {
            PlayerId : payloadJson.PlayerId,
            Session : payloadJson.Session,
            UpdatedAt : bigQuery.timestamp(new Date())
        };

        console.log('Row: '+row+" payload: "+payloadJson);
    
        bigQuery.dataset(config.PLAYER_SESSIONS.DATASET_NAME)
            .table(config.PLAYER_SESSIONS.TABLE_ID)
            .insert(row, insertHandler);
    }
    else {
        console.log("Not Authenticated event, type: "+payloadJson.Type);
    }

    callback();
};
