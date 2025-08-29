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

* *Other Cloud Providers are available as the BBC say