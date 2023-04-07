# Architecture and Internal Design
Charon composed of 2 components: Downloader and Processor. They can be combined as one binary, or run as a single microservice.

## Downloader
The Downloader checks for newly available GAR delta files, and keeps track, of what has been already downloaded. After a successful download, it notifies Processor (or Processors) about it.
Downloader is presented as a simple timer service, which polling interval is configurable through configuration file, env variable and cli argument. Any amount of downloaders can be run in parallel, without task intersection. Currently, only MinIO is supported as an object storage for files because of it's capability to retrieve individual files within uploaded ZIP. That can possibly change in future, but for now, it saves a lot of space.

## Processor
The Processor accepts message from Downloader (or Downloaders), and processes each XML file in a recently downloaded ZIP. It can also be replicated, thanks to all the mechanisms, explained above in the Downloader section. Import process must be defined as a plugin, because currently Charon is not providing any, other then a mock. This will definitely change in future, the default plugin must exist anyway.

## How replicated instances are not conflicting with each other?
### Ring
Rings in Charon are used to share work across several replicas of a component in a consistent way, so that any other component can decide what job needs to be done right now. A simple key-value storage can be used for this. Consul, Etcd, Memberlist are supported.
### Cluster monitoring
Cluster monitor storage is just a table in database, that contains records about all he jobs in cluster, that are being processed right now, or have already been processed in the past. Cluster monitor storage can be locked by any instance to prevent data duplication and task intersection. Each component has its own cluster monitor storage, shared between component's instances in cluster. Only Postgres is supported as cluster monitor storage provider right now.
### Summary
Each instance enters lifetime ring on start. That's how it knows about all the members in cluster. Also, each instance in cluster connects to a shared WAL, in which it saves state and reports about processing and finished job. Instance can steal another members' job only if this member becomes unhealthy in the ring.
### Communication
Nats is used as a message broker for Charon components.