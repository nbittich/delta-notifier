# Delta Notifier

This component receives raw delta messages from mu-authorization and forwards them to interested entities.

## Configuration

Delta's need to be sent from mu-authorization to the delta-notifier.  The delta notifier needs to be configured to send the right information to your microservice.

### Wiring mu-delta-notifier in a mu-semtech stack

The delta-notifier needs to receive messages from mu-authorization and it needs to send messages to other services.  Ideally this would be expressed by links to make the communication paths clear.  The link option in docker-compose creates an alternative name and places a dependency between the services.  We cannot use this as it creates a loop `some-service -> mu-authorization -> delta-notifier -> some-service`.  We do advise documenting which services consume the delta service in docker-compose comments to ensure the application flow is clear.

More information on wiring mu-authorization and the mu-delta-notifier can be found in the documentation of mu-authorization.  At the time of writing, you can add a file in mu-authorization's config (most often at `config/authorization/delta.ex`) and include the following contents:

```elixir
defmodule Delta.Config do
  def targets do
    [ "http://deltanotifier" ]
  end
end
```

### Including the delta-notifier in your stack

Default inclusion of the delta-notifier looks like this:

```yml
  deltanotifier:
    image: semtech/mu-delta-notifier
    volumes:
      - ./config/delta:/config
```

### Receiving delta notifications

Receiving services should be configured in `config/delta/rules.js`.  The format of this file is in flux, yet it is the intention that services consuming these delta messages can consistently receive messages in a specific format.  Use the `resourceFormat` key to select your preferred format.

We first present an example, next we explain each of the properties.  The following is a connection to the resource service in `config/delta/rules.js`.

```js
export default [
  {
    match: {
      // form of element is {subject,predicate,object}
      // predicate: { type: "uri", value: "http://www.semanticdesktop.org/ontologies/2007/03/22/nmo#isPartOf" }
    },
    callback: {
      url: "http://resource/.mu/delta", method: "POST"
    },
    options: {
      resourceFormat: "v0.0.0-genesis",
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  }
]
```

The exported property contains an array of definitions, each linking a match to a callback.

  - `match`: Pattern to match against.  Any supplied key must match, anything unspecified is ignored.
  - `match.subject`: Matches the subject.  Both `type` and `value` may be specified.
  - `match.predicate`: Matches the predicade.  Both `type` and `value` may be specified.
  - `match.object`: Matches the object.  Both `type` and `value` may be specified.
  - `callback`: The place to inform about a matched delta
  - `callback.url`: URL to inform about a match
  - `callback.method`: Method to use when informing about a match
  - `options`: Options describing the match
  - `options.resourceFormat`: Version format describing the format of the contents.  Keys may be added to this format, but they may not be removed.  Filter the properties as needed.
  - `options.gracePeriod`: Only send the response after a certain amount of time.  This will group changes in the future.
  - `options.ignoreFromSelf`: Don't inform about changes that originated from the microservice to be informed (based on the hostname).

## Delta formats

The delta may be offered in multiple formats.  Versions should match the exact string.  Specify `options.resourceFormat` to indicate the specific resourceformat.

### v0.0.1

v0.0.1 is the latest format of the delta messages. It may be extended with authorization rights etc. in the future. The current format looks like:

```json
    [
      { "inserts": [{"subject": { "type": "uri", "value": "http://mu.semte.ch/" },
                     "predicate": { "type": "uri", "value": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" },
                     "object": { "type": "uri", "value": "https://schema.org/Project" }}],
        "deletes": [] }
    ]
```

### v0.0.0-genesis

Genesis format as described by the initial Delta service PoC. It looks like:

```json
    { 
      "delta": {
        "inserts": [{"s": "http://mu.semte.ch/",
                     "p": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                     "o": "https://schema.org/Project"}],
        "deletes": [] }
    }
```

### false or undefined

Any falsy value will currently not send the changed triples to the consuming service.  Use this if you use this as a trigger for checking the new state in the database.

## Debugging

Debugging can be enabled in the service by setting environment variables.  The following may be handy:

  - `DEBUG_DELTA_SEND`: Logs all delta messages that are being sent to clients
  - `DEBUG_DELTA_MATCH`: Logs a check for each target block, indicating a check will occur
  - `DEBUG_TRIPLE_MATCHES_SPEC`: Extensive logging for triples matching a given specification.  Handy when requests are unexpectedly not sent.
  
## Extending

You are encouraged to help figure out how to best extend this service.  Fork this repository.  Run an experiment.  Open an issue or PR describing your experiment.  Feel free to open up an issue if you would like to discuss a possible extension.
