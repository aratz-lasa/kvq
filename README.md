# kvq
Key-value store that uses external RabbitMQ queue for client-server communication. It makes use of no lock for storing data, instead, it partitions the data in nodes. Nodes act as actors (for more information on what actor means go to [https://en.wikipedia.org/wiki/Actor_model](https://en.wikipedia.org/wiki/Actor_model)), responding to `add`, `get`, `getall`, and `del` requests.

# Installation
1. Install RabbitMQ as indicated in [https://www.rabbitmq.com/download.html](https://www.rabbitmq.com/download.html).

2. Install the application:

```console
foo@bar:~$ go install ./...
```
# Server usage
Run server:
```console
foo@bar:~$ kvq server start
INFO[0000] Server running
```

Scale server:
```console
foo@bar:~$ kvq server scale
Current scale: 4
Scaling to 8...
New scale: 8
```
# Client usage
Add key-value pair:
```console
foo@bar:~$ kvq client add hello world
```

Get value from key:
```console
foo@bar:~$ kvq client add hello world
foo@bar:~$ kvq client get hello
world
```

Get all values:
```console
foo@bar:~$ kvq client add hello world
foo@bar:~$ kvq client add hello moon
foo@bar:~$ kvq client getall
world moon
```

Del value from key:
```console
foo@bar:~$ kvq client add hello world
foo@bar:~$ kvq client del hello
foo@bar:~$ kvq client get hello

```
