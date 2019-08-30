# Workshop Notes

* Start with the zeromq website open: https://zeromq.org/

Looks like an embeddable networking library but acts like a concurrency
framework

The philosophy of ZeroMQ starts with the zero. The zero is for zero broker
(ZeroMQ is brokerless), zero latency, zero cost (it's free), and zero
administration.

More generally, "zero" refers to the culture of minimalism that permeates the
project. We add power by removing complexity rather than by exposing new
functionality.

**What ZeroMQ does!** Send and recieve message.

## Messages & Sockets

Messages = ZMTP transport layer protocol. Simlar to HTTP!

https://rfc.zeromq.org/spec:23/ZMTP/

* Show rfc page

ZMTP support more protocols like tpc, udp, pgm, ipc, inproc

http://zguide.zeromq.org/java:all#How-It-Began

* Show a terrible accident image from the zguide to introduce patterns

## Patterns

### Hello World

http://zguide.zeromq.org/java:all#Ask-and-Ye-Shall-Receive
* hwserver
* hwclient

### REQ/REP

http://zguide.zeromq.org/java:all#Shared-Queue-DEALER-and-ROUTER-sockets
* rrclient
* rrworker
* rrbroker

### PIPELINE

http://zguide.zeromq.org/java:all#Divide-and-Conquer
* taskevent
* taskwork
* tasksink

### PUB/SUB

http://zguide.zeromq.org/java:all#Getting-the-Message-Out
* wuserver
* wuclient

#### Easy broker with proxy

http://zguide.zeromq.org/java:all#ZeroMQ-s-Built-In-Proxy-Function
* msgqueue

### EXPAIR

http://zguide.zeromq.org/java:all#Signaling-Between-Threads-PAIR-Sockets
* mtrelay

## Handling Multiple Sockets

http://zguide.zeromq.org/java:all#Handling-Multiple-Sockets
* msreader
* mspoller

## High Water Marks

* Explain backpressure!
* How does ZeroMQ deal with backpressure? Depends on socket type either drop or
  block!

## Dafka

https://github.com/zeromq/dafka#design

* Show design images from README.

### Dynamic Discovery

http://zguide.zeromq.org/java:all#The-Dynamic-Discovery-Problem

* Show the pictures and explain how the Dafka tower deamon works!

### Protocols

Discuss the BNF https://github.com/zeromq/dafka/blob/master/src/dafka_proto.bnf

* Demonstrate zproto with Dafka Protocol:
  https://github.com/zeromq/dafka/blob/master/src/dafka_proto.xml

* Show generated Java Code in zeromq-workshop project (Intellij)

### Agents

* Demonstrate with skeleton from zeromq-workshop project (Intellij)
