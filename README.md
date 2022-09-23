# Distributed file system

### Overview
We implemented a distributed fault-tolerant file storage using Java. During the
connection, we used a heartbeat mechanism to detect permanent failures in the
storage nodes. Specifically, the directory server would check a specific storage
node before returning its address to a client after receiving a CONNECT request. For
the failure in the directory server, we employed a backup directory node that would
keep the same state as the primary directory node. In order to handle simultaneous
storage nodes failures, we maintained the consistency of files in every storage
node.

In our implementation, both primary and backup DirectoryServer addresses are
known to StorageNodes and Clients. Whenever they are not able to connect to the
primary, they immediately switch to the backup without a hassle.

We implemented a Backup thread runs as a daemon on the primary DirectoryServer.
This thread serializes primary DirectoryServer’s state and send it to the backup
DirectoryServer in every second. This is how we achieved consistency between two
DirectoryServer nodes.

In our implementation, DirectoryServer distributes new files to other StorageNodes
via TCP socket. We did not use a multicast or a broadcast mechanism to notify other
StorageNodes since we cannot be sure about the message delivery with UDP.

Lastly, whenever a new StorageNode gets connected to the system,
DirectoryServer gets all files from other StorageNodes and uploads them onto new
StorageNode.
