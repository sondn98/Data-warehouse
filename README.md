# Data warehouse
## Overview
#### 1. Introduction
#### 2. Use case
## Requirement analysis
#### 1. Requirement
Let assume that, your boss wants you to build a data warehouse system (DWS) and you a list of system requirements:
* Storage
    * Your DWS should be used to ingest, store and serve the collected data, which may grow gigabytes per day (hundreds of millions record/day) and take up to terabyte storage for all
    * The handled data will be structured, semi-structured (text/wrapped as models) or unstructured (multi-media files as audio, video, ...)
    * Data can come from bounded data source (files) or unbounded data source (realtime monitoring system, logging system, ...) as collected files, web crawling, company's products or event other systems
* Resources limitation
    * Your company has an abundant capital so system resources may not be a problem
* High availability and load balancing
    * System failure always be a big problem in distributed system. Your DWS should have good design to address this issue
    * Components inside your system are also fault tolerance. In case of component fully down, Your system should not be stopped
    * The traffic demand on a single component should not be remarkable higher then other inside your system or components should have horizontal scalability 
* Latency
    * It's easy to see that the business tasks are very diverse, but it only requires 3 kind of serving data: query single record, multi-record or get files
    
* Functions
## Architecture
### 1. Terminologies

## Components
### 1. Data collector
### 2. Batch layer
### 3. Speed layer
### 4. Serving layer
### 5. Other
## Installation and usage
### 1. Pre-required
### 2. Installation step by step
### 3. Guide to use
## Discussion and Contribution
