## WIP of CQRS lib:

### Basics
- Everything is split by topic, concurrency is by topic
- Commands are persistent
- Queries are directly handled in-memory
- Commands generate Events (persistent, they change the state)
- Sagas react on Events and publish commands (see commands, persistent)

### Commands 
A command mutates state. Workflow:
1. Actor creates a command and sends it to the commandbus
2. Actor gets back a referenceId
3. Once command is written, the method resolves
4. State of a command can be queried against with the referenceId (see queries below)

### Events
An event follows a state-mutation (a.k.a a command when it was successfully processed). Workflow:
1. A command is issued and processed successfully
2. An event _may_ be generated and published to the event-bus
3. Once the event is written, the publishing resolves
4. Events are pulled from the db and processed by either an readmodel or another saga

### Sagas
A saga reacts on events and generates other commands. Workflow:
1. A saga is instantiated (listening to the event$-stream).
2. Upon consumption a command is generated
3. Once the command is written, the method resolves

### Queries
A query uses the calculated-state of a readmodel to return user-data. It may NOT mutate state. 
It is not strictly neccessary to persist the query, but _can_ be persisted for auditing. Workflow:

1. Query is created and put on the query-bus
2. Querybus finds a handler, invokes it synchronously
3. Upon Success/failure, query is persisted in db for auditing reasons
---
Note to commands: 
A commands processing state can be queried with a referenceId

