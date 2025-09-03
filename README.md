# Digital Twin HowTo

## Introduction

This repo demonstrates how to create a partial update digital twin for a large
number of devices where status updates are sent piecemeal and also can be out of
order. For example, a vehicle might send readings from a subset of sensors in
each update but also update may occasionally arrive out of time.

It shows both the recommended method and two alternative methods that are less
efficient but more obvious to allow comparison of resoure usage and cost.

We are demonstrating this against MongoDB Atlas - like most cloud services costs
for Atlas include outboud data transfer from nodes - the recommended model
minimises network transfers and backup costs.

## Data Model

We are using the idea of a vehicle here as it's well understood, but we will
abstract the names of as many fields as we can to keep this code simple and
universal. Building a realistic model of all the sensors on a specific type of
device adds unwarrented complexity.

We will use Map() classes for data for simplicity and flexibility, in general
these would be explicit classes mapped with Spring/Quarkus or similar

Although the incoming messages are fo the form

```
{
  deviceId: "1234567890",
  deviceName: "Red Ford",
  attributes: [
    { arrtibuteId: "attr001", attrName: "right blinker", value: "on"},
    { arrtibuteId: "attr002", attrName: "left blinker", value: "off"},
    { arrtibuteId: "attr003", attrName: "engine rpm", value: "3200"}
  ]
}
```

in MognoDB we will convert the Array to a Map/Document as it is easier and more
efficient for randomly access values in some cases. Either the database or the
application can transform back to an array if required, and both can be indexes.

Our data model is therefore stored like this. Where we have many attributes we
would modify this to add some depth to the object, for example based on the
number with all values starring with 0 in one
branch and all starting with 1 in another or, for real world attributes where
the id is a name with spaces or capitals, braking on the internal words so
`{ "blinker" : { right: { value: "on" } } }` if the id was `blinker_left` or
`BlinkerLeft`

```
{
  deviceId: "1234567890",
  deviceName: "Red Ford",
  attributes: {
    "attr001": { attrName: "right blinker", value: "on"},
    "attr002": { attrName: "left blinker", value: "off"},
    "attr003": { attrName: "engine/ rpm", value: "3200"}
  }
```

## Build

## Build and Run on AWS**

```
sudo yum install -y java-21 git maven
git clone https://github.com/johnlpage/DigitalTwin.git
export JAVA_HOME="/usr/lib/jvm/java-21-amazon-corretto"
cd DigitalTwin
mvn clean package
```

## Run

Load Data ( for ReadReplace and ServerSide)

```shell
export MONGDB_URI="mongodb+srv://digitwin:ssssss@volkswagendigitwin.qcpeq8.mongodb.net/?retryWrites=true&w=majorit&compressors=snappy&appName=VolkswagenDigiTwin"


java -jar target/MongoTwin-1.0-SNAPSHOT.jar -t 32 -p true -s ReadReplaceStrategy -m 10000000 -d 10000000
```

Test Performance (ReadReplace) 100% working set

```shell

java -jar target/MongoTwin-1.0-SNAPSHOT.jar -t 8 -s ReadReplaceStrategy -m 5000000 -d 10000000

```

Test Performance (ServerSide) 100% working set

```shell

java -jar target/MongoTwin-1.0-SNAPSHOT.jar -t 16 -s ServerSideStrategy  -m 5000000 -d 10000000

```

Load Data for Bloob Strategy

```shell
export MONGDB_URI="mongodb+srv://digitwin:ssssss@volkswagendigitwin.qcpeq8.mongodb.net/?retryWrites=true&w=majorit&compressors=snappy&appName=VolkswagenDigiTwin"

java -jar target/MongoTwin-1.0-SNAPSHOT.jar -t 32 -p true -s BlobStrategy -m 10000000 -d 10000000
```

Test Performance (BlobStrategy) 100% working set

```shell

java -jar target/MongoTwin-1.0-SNAPSHOT.jar -t 32 -s BlobStrategy  -m 5000000 -d 10000000

```

Sharded versiuon

```js
use
digitwin
sh.shardCollection("digitwin.twins", {_id: 1})


let shardNames = [
    'atlas-ee3lna-shard-0',
    'atlas-ee3lna-shard-1',
    'atlas-ee3lna-shard-2',
    'atlas-ee3lna-shard-3'
];

print("Starting split and move operations...");

for (let i = 0; i < 1000; i++) {
    let paddedValue = i.toString().padStart(3, '0');
    let splitKey = "V_" + paddedValue;
    let targetShard = shardNames[i % 4]; // Round-robin distribution  

    try {
        // Split at this key  
        sh.splitAt("digitwin.twins", {_id: splitKey});
        print("Split " + i + " created at: " + splitKey);

        // Small delay to let the split register  
        sleep(100);

        // Move the chunk that was just created (the one with min value = previous split)  
        let chunkMin;
        if (i === 1) {
            // First split - move chunk from MinKey  
            chunkMin = {_id: MinKey};
        } else {
            // Subsequent splits - move chunk from previous split point  
            let prevPaddedValue = (i - 1).toString().padStart(3, '0');
            chunkMin = {_id: "V_" + prevPaddedValue};
        }

        sh.moveChunk("digitwin.twins", chunkMin, targetShard);
        print("Moved chunk " + i + " (min: " + JSON.stringify(chunkMin) + ") to " + targetShard);

    } catch (e) {
        print("Error at iteration " + i + " (splitKey: " + splitKey + "): " + e);
    }

    // Progress update every 50 operations  
    if (i % 50 === 0) {
        print("Progress: " + i + "/999 operations completed");
        sleep(500); // Longer pause every 50 operations  
    }
}

// Move the final chunk (from last split to MaxKey)  
try {
    let finalChunkMin = {_id: "V_999"};
    let finalTargetShard = shardNames[999 % 4];
    sh.moveChunk("digitwin.twins", finalChunkMin, finalTargetShard);
    print("Moved final chunk to " + finalTargetShard);
} catch (e) {
    print("Error moving final chunk: " + e);
}

print("Split and move operations complete!");
print("Final distribution:");
sh.status();


```

**Other Cloud Providers are available as the BBC say