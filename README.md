## @figedi/cqrs

Yet another event-bus library facilitating CQ(R)S patterns. Description will follow.

### Idea pile
- Add transient queries as well, e.g. for user-sensitive data with persistence = "pg"

## TODO / BUGS
- when pool-size is too small (e.g. defaults), it just hangs (see the UOWDecorator test)