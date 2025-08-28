# Digital Twin HowTo

## Introduction

This repo demonstrates how to create a partial update digital twin for a large number of devices where status updates are sent piecemeal and also can be out of order. For example, a vehicle might send readings from a subset of sensors in each update but also update may occasionally arrive out of time.

It shows both the recommended method and two alternative methods that are less efficient but more obvious to allow comparison of resoure usage and cost.

We are demonstrating this against MongoDB Atlas - like most cloud services costs for Atlas include outboud data transfer from nodes - the recommended model minimises network transfers and backup costs.

## Data Model

We are using the idea of a vehicle here as it's well understood, but we will abstract the names of as many fields as we can to keep this code simple and universal. Building a realistic model of all the sensors on a specific type of device adds unwarrented complexity.

We will use Map() classes for data for simplicity and flexibility, in general these would be explicit classes mapped with Spring/Quarkus or similar

## Build 

## Build and Run on AWS**



* *Other Cloud Providers are available as the BBC say