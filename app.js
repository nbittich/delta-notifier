import { app } from 'mu';
import request from 'request';
import services from '/config/rules.js';
import bodyParser from 'body-parser';
import dns from 'dns';

// Also parse application/json as json
app.use( bodyParser.json( { type: function(req) { return /^application\/json/.test( req.get('content-type') ); } } ) );

// Log server config if requested
if( process.env["LOG_SERVER_CONFIGURATION"] )
  console.log(JSON.stringify( services ));

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

async function informWatchers( changeSets, changedTriples, res ){
  for( let entry of services ) {
    // for each entity
    if( process.env["DEBUG_DELTA_MATCH"] )
      console.log(`Checking if we want to send to ${entry.callback.url}`);

    let matchSpec = entry.match;

    let originFilteredTriples = await filterMatchesForOrigin( changedTriples, entry );

    let someTripleMatchedSpec =
        originFilteredTriples
        .some( (triple) => tripleMatchesSpec( triple, matchSpec ) );

    if( process.env["DEBUG_TRIPLE_MATCHES_SPEC"] )
      console.log(`Triple matches spec? ${someTripleMatchedSpec}`);

    if( someTripleMatchedSpec ) {
      // inform matching entities
      if( process.env["DEBUG_DELTA_SEND"] )
        console.log(`Going to send ${entry.callback.method} to ${entry.callback.url}`);

      if( entry.options && request.options.gracePeriod ) {
        setTimeout(
          () => sendRequest( entry, changeSets ),
          entry.options.gracePeriod );
      } else {
        sendRequest( entry, changeSets );
      }
    }
  }
}

function tripleMatchesSpec( triple, matchSpec ) {
  // form of triple is {s, p, o}, same as matchSpec
  if( process.env["DEBUG_TRIPLE_MATCHES_SPEC"] )
    console.log(`Does ${JSON.stringify(triple)} match ${JSON.stringify(matchSpec)}?`);

  for( let key in matchSpec ){
    // key is one of s, p, o
    let subMatchSpec = matchSpec[key];
    let subMatchValue = triple[key];

    if( subMatchSpec && !subMatchValue )
      return false;

    for( let subKey in subMatchSpec )
      // we're now matching something like {type: "url", value: "http..."}
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
  let url = entry.callback.url;
  if( entry.options && entry.options.resourceFormat ) {
    // we should send contents
    const body = formatChangesetBody( changeSets, entry.options );

    requestObject = {
      url, method,
      headers: { "Content-Type": "application/json" },
      body: body
    };
  } else {
    // we should only inform
    requestObject = { url, method };
  }

  if( process.env["DEBUG_DELTA_SEND"] )
    console.log(`Executing send ${method} to ${url}`);

  request( requestObject ); // execute request
}

async function filterMatchesForOrigin( changedTriples, entry ) {
  if( ! entry.options || !entry.options.ignoreFromSelf ) {
    return changedTriples;
  } else {
    let originIpAddress = await getServiceIp( entry.callback.url );
    return changedTriples.filter( (change) => change.origin != originIpAddress );
  }
}

async function getServiceIp(serviceEndpoint) {
  const hostName = (new URL(serviceEndpoint)).hostName;
  return new Promise( (resolve, reject) => {
    dns.lookup( hostName, { family: 4 }, ( err, address) => {
      if( err )
        reject( err );
      else
        resolve( address );
    } );
  } );
};
