# App Analytics 

Small utility to download AppExchange App Analytics files. 

Add a .env file that looks like 

```
SF_USER=sf@user.com
SF_PASS=pass+securityToken
SF_ENDPOINT=https://login.salesforce.com/services/Soap/u/50.0
JDBC_DATABASE_URL=jdbc:postgresql://localhost:5432/app-analytics?user=troybo&password=not_telling
SF_SOAP_TRACE=[true|false]
```

## Setup
You will need to have the AppExchange Analytics package enabled for this to work, it will not run on developer orgs, sandboxes etc 
The user that is listed above will need to have access to the relevant objects to query as well as API permissions enabled. 

This project was built using 
- openjdk version "11.0.6"
- Maven 3.6.3
- Postgres 10.12 


## Design
This is a simple java app that will fetch the data described in the [Salesforce ISV Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.packagingGuide.meta/packagingGuide/app_analytics_download_mp_logs.htm)
When downloaded, it will then use PG Copy to insert the data in the retrieved CSV files. 

## Run

There are three files that are invokeable
- com.grax.aus.PackageUsageLog
- com.grax.aus.SubscriberSnapshot
- com.grax.aus.PackageUsageSummary

The PackageUsageLog and SubscriberSnapshot routines are designed to be run daily. 
They will not stop you from running and retrieving files more than once per day, but they will check the database for *ANY* records that have the same date 
- timestamp_derived for the package usage log
- date for the subscriber snapshot

The PackageUsageSummary is designed to be run once per month 
This will not stop you running and downloading files more than once per day, but it wil check the database for *ANY* records that have the same date
- month is the column that is checked for PackageUsageSummary

The short of it is, if the count query on these tables for the particular date returns a non-zero number then the PG Copy will not run.

## Build

Clone this repository and then build using Maven

```
> mvn clean package
```