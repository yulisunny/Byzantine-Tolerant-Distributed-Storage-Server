## Byzantine Tolerant Distributed Storage Server

A proof-of-concept Java server that can tolerate Byzantine failures, demonstrated by subscribing to topics through the provided client, and alerting when the subscribed data is updated.

## Usage:
Assume you use a similar service to Digital Ocean or AWS where you know the IP addresses of your distributed servers, enter them in the admin.config file.

To build: $ ant

To run the admin client: $ java -jar admin.jar
To initialize the service: initService {#servers} {cacheSize} {cacheStrategy}
To start the service: start

To start the client: $ java -jar client.jar
To connect to a server (any server is fine): connect {IP_addr} {port#}
For the rest of client commands, just type help.

## How Byzantine Failures are handled?
The support for Byzantine failures is defined by the detection and recovery of compromised servers. It involves two types of detection: the detection of “compromised get” and the detection of “compromised put”. A “compromised get” is when a compromised server attempts to return a wrong value to a client’s get request. A “compromised put” is when a compromised server attempts to update key-value pairs (replication process) on other servers that were not initiated by the client or initiated by the client but with the wrong value. 

### Detection and Recovery of Compromised Get Protocol Procedures: 
1. Client initiates a GET request to Server A,
2. Server A will reply to the Client with a Key,Value pair
3. Once the Client has received the Key,Value pair from Server A, it will silently (non-blocking background
operations) send the Key,Value pair to the other two Server Replicas(call them Server B and C) that
also know about this Key,Value pair
4. Server B and Server C, once received the Key, Value pair from Client, will validate the Key,Value pair
using their own persistent storage. If the value is wrong, or the pair does not exist, Server B and Server
C will notify the Client and ECS (Container Orchestration Service).
5. Once ECS receives Both of the messages from Server B and Server C that Server A is compromised,
which means the quorum is reached, it will take action against the Compromised Server A by taking it
down and deleting its data and then bring it back up with the data from its Replicas.
6. Once the Client receives Both of the messages from Server B and Server C that Server A is
compromised and with the actual read value (quorum is needed), it will notify the user in the UI that the previous value that the user got was incorrect and what the actual read value should be.

### Detection and Recovery of Compromised Put Protocol Procedures:
1. Client initiates a PUT request to Server A. Each of our Client Application has a server socket that is
used for the cases where Servers need to contact the Client for validations of PUT request.
2. Server A will reply to the Client with a PUT_SUCCESS message
3. Once Client receives the reply from Server A that the PUT request is successful, it will store the put
request in its action log
4. Server A will proceed to send Server B and Server C (its Replicas) the Client PUT request. In our
protocol, each replication needs not only the Key, Value pair but also the origin of the request (which
Client is the request from).
5. Server B and Server C will attempt to connect to the Client that is provided in the replication message
and send the key, value pair along with Server A’s name(origin of replication process) to the client for
confirmation
6. Once the client receives the messages from Server B and Server C, it will compare the key,value pair
with its action log mentioned in step 3. If such key value pair by Server A put operation is not found in the action log, or the value is wrong, it will message back Server B and Server C that the client has never requested such put operations. And If such key value pair put operation indeed exist and is by Server A, then it will just silently ignore the message.
7. If Server B and Server C receives messages from Client that Server A’s put operation was wrong, they will tell ECS that Server A is Compromised.
8. ECS will take action against Server A by shutting it down and replacing its content with its Replicas’ data and bringing it back up.

## How subscription information is propogated?
Subscription information piggyback on the heartbeat messages that are  sent around the hash ring. When a server receives subscription information from its neighbour. It will compare it with the local subscription information and merge the information together. For example, if server A has subscription info “key1:[127.0.0.1:6000]” and server B
has subscription info “key1:[127.0.0.1:60001], key2:[127.0.0.1:60000]” Then when one server send its subscription information to another, the receiver will update its local subscription information to “key1:[127.0.0.1:6000, 127.0.0.1:60001], key2:[127.0.0.1:60000]”. By doing this, if the server that initially received the subscription request does not immediately crash before sending out any heartbeat, then it’s the subscription will live as long as the service lives, even when new nodes are brought up by ECS, it will be fed the subscription information that is being passed along the hash ring.

Unsubscription is handled by having the client library send the unsubscribe request to any server it knows in the storage service. Then, the receiving server will tell ECS about the request. The ECS will send a message to all active servers to temporarily halt all heartbeats. Then it will broadcast another message to let the servers remove the subscription in question. Finally, it will broadcast to resume the heartbeat process.

