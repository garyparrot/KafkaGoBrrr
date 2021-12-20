# KafkaGoBrrr

## Build fat-jar

```shell
./gradlew shadowJar
```

## Run

```shell
# run
./gradlew -PmainClass=org.example.App run

# help
./gradlew -PmainClass=org.example.App run --args="--help"
```

## Play Example

```shell
./gradlew -PmainClass=org.example.Play run
```

## Run Consumer Verifier

```shell
./gradlew -PmainClass=org.example.Consumer run --args="topic-name"
```

