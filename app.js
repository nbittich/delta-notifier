import { app } from 'mu';
import request from 'request';
import services from '/config/rules.js';
import bodyParser from 'body-parser';

// Also parse application/json as json
app.use( bodyParser.json( { type: function(req) { return /^application\/json/.test( req.get('content-type') ); } } ) );

app.get( '/', function( req, res ) {
  res.status(200);
  res.send("Hello, delta notification is running");
} );

app.post( '/', function( req, res ) {
  if( process.env["LOG_REQUESTS"] ) {
    console.log("Logging request body");
    console.log(req.body);
  }

  const changeSets = req.body.changeSets;

  changeSets.forEach( (change) => {
    change.insert = change.insert || [];
    change.delete = change.delete || [];
  } );

  let allInserts = [];
  let allDeletes = [];

  changeSets.forEach( (change) => {
    allInserts = [...allInserts, ...change.insert];
    allDeletes = [...allDeletes, ...change.delete];
  } );

  // inform watchers
  informWatchers( changeSets, [...allInserts, ...allDeletes], res );

  // push relevant data to interested actors
  res.status(204).send();
} );

function informWatchers( changeSets, changedTriples, res ){
  for( let entry of services ) {
    // for each entity
    if( process.env["DEBUG_DELTA_MATCH"] )
      console.log(`Checking if we want to send to ${entry.callback.uri}`);

    let matchSpec = entry.match;

    let someTripleMatchedSpec = changedTriples.some( (triple) => tripleMatchesSpec( triple, matchSpec ) );


    if( someTripleMatchedSpec ) {
      // inform matching entities
      if( process.env["DEBUG_DELTA_SEND"] )
        console.log(`Going to send ${entry.callback.method} to ${entry.callback.uri}`);

      let requestSendingFunction = () => sendRequest( entry, changeSets );

      if( entry.options && request.options.gracePeriod ) {
        setTimeout( requestSendingFunction, entry.options.gracePeriod );
      } else {
        requestSendingFunction();
      }
    }
  }
}

function tripleMatchesSpec( triple, matchSpec ) {
  // form of triple is {s, p, o}, same as matchSpec
  for( let key in matchSpec ){
    // key is one of s, p, o
    let subMatchSpec = matchSpec[key];
    let subMatchValue = triple[key];

    if( subMatchSpec && !subMatchValue )
      return false;

    for( let subKey in subMatchSpec )
      // we're now matching something like {type: "uri", value: "http..."}
      if( subMatchSpec[subKey] !== subMatchValue[subKey] )
        return false;
  }
  return true; // no false matches found, let's send a response
}


function formatChangesetBody( changeSets, options ) {
  if( options.resourceFormat == "v0.0.0-genesis" ) {
    return JSON.stringify(
      changeSets.map( (change) => {
        return {
          inserts: change.insert,
          deletes: change.delete
        };
      } ) );
    // [{delta: {inserts, deletes}]
  } else {
    throw `Unknown resource format ${options.resourceFormat}`;
  }
}

function sendRequest( entry, changeSets ) {
  let requestObject; // will contain request information

  // construct the requestObject
  let method = entry.callback.method;
  let uri = entry.callback.uri;
  if( entry.options && entry.options.resourceFormat ) {
    // we should send contents
    const body = formatChangesetBody( changeSets, entry.options );

    requestObject = {
      uri, method,
      headers: { "Content-Type": "application/json" },
      body: body
    };
  } else {
    // we should only inform
    requestObject = { uri, method };
  }

  request( requestObject ); // execute request
}
