import { app, uuid } from 'mu';
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

  const originalMuCallIdTrail = JSON.parse( req.get('mu-call-id-trail') || "[]" );
  const originalMuCallId = req.get('mu-call-id');
  const muCallIdTrail = JSON.stringify( [...originalMuCallIdTrail, originalMuCallId] );

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
  informWatchers( changeSets, [...allInserts, ...allDeletes], res, muCallIdTrail );

  // push relevant data to interested actors
  res.status(204).send();
} );

async function informWatchers( changeSets, changedTriples, res, muCallIdTrail ){
  services.map( async (entry) => {
    // for each entity
    if( process.env["DEBUG_DELTA_MATCH"] )
      console.log(`Checking if we want to send to ${entry.callback.url}`);

    const matchSpec = entry.match;

    const originFilteredTriples = await filterMatchesForOrigin( changedTriples, entry );
    if( process.env["DEBUG_TRIPLE_MATCHES_SPEC"] )
      console.log(`There are ${originFilteredTriples.length} triples not from ${hostnameForEntry( entry )}`);

    const someTripleMatchedSpec =
        originFilteredTriples
        .some( (triple) => tripleMatchesSpec( triple, matchSpec ) );

    if( process.env["DEBUG_TRIPLE_MATCHES_SPEC"] )
      console.log(`Triple matches spec? ${someTripleMatchedSpec}`);

    if( someTripleMatchedSpec ) {
      // inform matching entities
      if( process.env["DEBUG_DELTA_SEND"] )
        console.log(`Going to send ${entry.callback.method} to ${entry.callback.url}`);

      if( entry.options && entry.options.gracePeriod ) {
        setTimeout(
          () => sendRequest( entry, changeSets, muCallIdTrail ),
          entry.options.gracePeriod );
      } else {
        sendRequest( entry, changeSets, muCallIdTrail );
      }
    }
  } );
}

function tripleMatchesSpec( triple, matchSpec ) {
  // form of triple is {s, p, o}, same as matchSpec
  if( process.env["DEBUG_TRIPLE_MATCHES_SPEC"] )
    console.log(`Does ${JSON.stringify(triple)} match ${JSON.stringify(matchSpec)}?`);

  for( let key in matchSpec ){
    // key is one of s, p, o
    const subMatchSpec = matchSpec[key];
    const subMatchValue = triple[key];

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
  if( options.resourceFormat == "v0.0.1" ) {
    return JSON.stringify(
      changeSets.map( (change) => {
        return {
          inserts: change.insert,
          deletes: change.delete
        };
      } ) );
  }
  if( options.resourceFormat == "v0.0.0-genesis" ) {
    // [{delta: {inserts, deletes}]
    const newOptions = Object.assign({}, options, { resourceFormat: "v0.0.1" });
    const newFormat = JSON.parse( formatChangesetBody( changeSets, newOptions ) );
    return JSON.stringify({
      // graph: Not available
      delta: {
        inserts: newFormat
          .flatMap( ({inserts}) => inserts)
          .map( ({subject,predicate,object}) =>
                ( { s: subject.value, p: predicate.value, o: object.value } ) ),
        deletes: newFormat
          .flatMap( ({deletes}) => deletes)
          .map( ({subject,predicate,object}) =>
                ( { s: subject.value, p: predicate.value, o: object.value } ) )
      }
    });
  } else {
    throw `Unknown resource format ${options.resourceFormat}`;
  }
}

async function sendRequest( entry, changeSets, muCallIdTrail ) {
  let requestObject; // will contain request information

  // construct the requestObject
  const method = entry.callback.method;
  const url = entry.callback.url;
  const headers = { "Content-Type": "application/json", "MU-AUTH-ALLOWED-GROUPS": changeSets[0].allowedGroups, "mu-call-id-trail": muCallIdTrail, "mu-call-id": uuid() };

  if( entry.options && entry.options.resourceFormat ) {
    // we should send contents
    const body = formatChangesetBody( changeSets, entry.options );

    // TODO: we now assume the mu-auth-allowed-groups will be the same
    // for each changeSet.  that's a simplification and we should not
    // depend on it.

    requestObject = {
      url, method,
      headers,
      body: body
    };
  } else {
    // we should only inform
    requestObject = { url, method, headers };
  }

  if( process.env["DEBUG_DELTA_SEND"] )
    console.log(`Executing send ${method} to ${url}`);

  request( requestObject, function( error, response, body ) {
    if( error ) {
      console.log(`Could not send request ${method} ${url}`);
      console.log(error);
      console.log(`NOT RETRYING`); // TODO: retry a few times when delta's fail to send
    }

    if( response ) {
      // console.log( body );
    }
  });
}

async function filterMatchesForOrigin( changedTriples, entry ) {
  if( ! entry.options || !entry.options.ignoreFromSelf ) {
    return changedTriples;
  } else {
    const originIpAddress = await getServiceIp( entry );
    return changedTriples.filter( (change) => change.origin != originIpAddress );
  }
}

function hostnameForEntry( entry ) {
  return (new URL(entry.callback.url)).hostname;
}

async function getServiceIp(entry) {
  const hostName = hostnameForEntry( entry );
  return new Promise( (resolve, reject) => {
    dns.lookup( hostName, { family: 4 }, ( err, address) => {
      if( err )
        reject( err );
      else
        resolve( address );
    } );
  } );
};
