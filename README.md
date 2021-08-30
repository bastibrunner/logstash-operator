# logstash-operator

Disclaimer: Kind of works, but still missing a lot. Probably not following any best practice, and will certainly fail in many edge cases.

Known issues (for now):
- Logstash autoreload seems not to work
- Service not updating correctly when changing output
- Not found exception thrown when deleting stuff (cause update triggered on pipeline configmap after child was deleted by kubernetes)
