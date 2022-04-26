# kvq
Key-value store with external queue

# Installation
```console
go install ./...
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
hello
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
