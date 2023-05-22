import { app, uuid } from 'mu';
import fetch from 'node-fetch';
import services from '/config/rules.js';
import bodyParser from 'body-parser';
import dns from 'dns';

const IP_LOOKUP_CACHE = new Map();
const IP_LOOKUP_CACHE_RETRY_TIMEOUT = 15000;

// Also parse application/json as json
app.use( bodyParser.json( {
  type: function(req) {
    return /^application\/json/.test( req.get('content-type') );
  },
  limit: '500mb'
} ) );

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
  const muSessionId = req.get('mu-session-id');

  changeSets.forEach( (change) => {
    change.insert = change.insert || [];
    change.delete = change.delete || [];
  } );

  // inform watchers
    informWatchers( changeSets, res, muCallIdTrail, muSessionId );

  // push relevant data to interested actors
  res.status(204).send();
} );

async function informWatchers( changeSets, res, muCallIdTrail, muSessionId ){
  services.map( async (entry) => {
    // for each entity
    if( process.env["DEBUG_DELTA_MATCH"] )
      console.log(`Checking if we want to send to ${entry.callback.url}`);

    const matchSpec = entry.match;

    const originFilteredChangeSets = await filterMatchesForOrigin( changeSets, entry );
    if( process.env["DEBUG_TRIPLE_MATCHES_SPEC"] && entry.options.ignoreFromSelf )
      console.log(`There are ${originFilteredChangeSets.length} changes sets not from ${hostnameForEntry( entry )}`);

    let allInserts = [];
    let allDeletes = [];

    originFilteredChangeSets.forEach( (change) => {
      allInserts = [...allInserts, ...change.insert];
      allDeletes = [...allDeletes, ...change.delete];
    } );

    const changedTriples = [...allInserts, ...allDeletes];

    const someTripleMatchedSpec =
        changedTriples
        .some( (triple) => tripleMatchesSpec( triple, matchSpec ) );

    if( process.env["DEBUG_TRIPLE_MATCHES_SPEC"] )
      console.log(`Triple matches spec? ${someTripleMatchedSpec}`);

    if( someTripleMatchedSpec ) {
      // inform matching entities
      if( process.env["DEBUG_DELTA_SEND"] )
        console.log(`Going to send ${entry.callback.method} to ${entry.callback.url}`);

      if( entry.options && entry.options.gracePeriod ) {
        setTimeout(
          () => sendRequest( entry, originFilteredChangeSets, muCallIdTrail, muSessionId ),
          entry.options.gracePeriod );
      } else {
        sendRequest( entry, originFilteredChangeSets, muCallIdTrail, muSessionId );
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

async function sendRequest( entry, changeSets, muCallIdTrail, muSessionId ) {
  let requestObject; // will contain request information

  // construct the requestObject
  const method = entry.callback.method;
  const url = entry.callback.url;
  const headers = { "Content-Type": "application/json", "MU-AUTH-ALLOWED-GROUPS": changeSets[0].allowedGroups, "mu-call-id-trail": muCallIdTrail, "mu-call-id": uuid() , "mu-session-id": muSessionId };
  // TODO: we now assume the mu-auth-allowed-groups will be the same
  // for each changeSet.  that's a simplification and we should not
  // depend on it.
  const body = entry.options && entry.options.resourceFormat ? formatChangesetBody( changeSets, entry.options ): null;
 

  if( process.env["DEBUG_DELTA_SEND"] )
    console.log(`Executing send ${method} to ${url}`);

    try {
      const response = await fetch(url, {
        headers,
        method,
        body,
      });
      if(await response) {
        // const respText = await response.text();
        //console.log(respText);
      } 
    } catch(e) {
      console.log(`Could not send request ${method} ${url}`);
      console.log(error);
      console.log(`NOT RETRYING`); // TODO: retry a few times when delta's fail to send
    }
 
}

async function filterMatchesForOrigin( changeSets, entry ) {
  if( ! entry.options || !entry.options.ignoreFromSelf ) {
    return changeSets;
  } else {
    try {
      const originIpAddress = await getServiceIp( entry );
      if(originIpAddress) {
        return changeSets.filter( (changeSet) => changeSet.origin != originIpAddress );
      } else{
         // we couldn't figure what's the ip address, thus we filter everything. not great, but this is just
         // for experimenting 
        return [];
      }
    } catch(e) {
      // handle the case when a service is down and the dns lookup cannot be performed.
      console.log(`something went wrong for changeSets ${changeSets} and entry ${entry} during lookup ${e}`);
      return []; // service is down anyway, don't send.
      
    }
  }
}

function hostnameForEntry( entry ) {
  return (new URL(entry.callback.url)).hostname;
}

async function getServiceIp(entry, retry=false) {

  const hostName = hostnameForEntry( entry );
  if(!retry && IP_LOOKUP_CACHE.has(hostName)) {
    return IP_LOOKUP_CACHE.get(hostName);
  }
  return new Promise( (resolve, reject) => {
    dns.lookup( hostName, { family: 4 }, ( err, address) => {
      if( err ) {
        IP_LOOKUP_CACHE.set(hostName, false);
        setTimeout(async ()=> {
          try {
            await getServiceIp(entry, true);
          }catch (e) {
          }
        }, IP_LOOKUP_CACHE_RETRY_TIMEOUT); // retry to put it in cache after x seconds
        reject( err );
      } else
        IP_LOOKUP_CACHE.set(hostName, address);
        resolve( address );
    } );
  } );
};
