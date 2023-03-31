# charon
## Use case
Let's assume you have a task to download GAR database, and sync it automatically. Charon discards all the boilerplate, you need to write:
- Check for newly available GAR deltas
- Download files
- Import files into your data store

All tasks will be done in parallel without intersecting with each other.

## Overview
Charon is a service, that helps you to keep your GAR databases in synced and actual state. It can start from scratch, or from a certain version. It is not a drop in solution, you need to define import process for yourself by writing a plugin, which will be described later.
Some of the key features of Charon include:
- **Easy to set up:** You just fork this repo, write your own shiny new plugin, inject it and start.
- **Higly scalable and distributed:** Charon is able to run in monolithic or microservices mode. All it's components can be replicated in any amount.
- **Fault tolerant:** Charon's components are stateful, but they are always saving their state.

## Documentation
Here is a list of all available docs:

- Quick start
- Installation
- Architecture