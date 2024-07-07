# LittleHorse Connect

LH Connect is a _specification_ for docker images that can be run in certain configurations in order to:
* Act as a `Source` Connector, which runs `WfRun`s or posts `ExternalEvent`s.
* Act as a `Task` Connector, which executes `TaskRun`s.

Some useful features about the Connector Specification are:
* **Generality:** A Connector encapsulates the logic needed to interface with an external system, and it can be used in many various configurations.
* **Multi-Tenancy:** a single LH Connector instance can process data for multiple `Tenant`s in LittleHorse.

## Run

Edit `lh-connect-runtime/build.gradle` to point to LHTaskConnector. Then
```
gradle lh-connect-runtime:run --args 'io.littlehorse.connect.example.SayHelloConnector /home/colt/colt-doodles/lh-connect/greet-task.properties'
```

Edit it to point to `LHSourceConnector`. Then:

```
gradle lh-connect-runtime:run --args 'io.littlehorse.connect.example.KafkaSource /home/colt/colt-doodles/lh-connect/kafka-run-wf-connector.properties'
```


Then create a topic called `customers` and produce jsons that look like this:
```
{"firstName": "Obi-Wan", "lastName": "Kenobi"}
{"firstName": "Anakin", "lastName": "Skywalker", "id": "asdfadfw23"}
```
