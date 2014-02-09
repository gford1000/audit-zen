audit-zen
=========

AWS DynamoDB based, data auditing API

The goal of this repo is to create an API that provides all the audit features required 
when data is changed in other APIs, thus creating a common location from which to 
investigate data changes.

The solution stores data to DynamoDB within a specified AWS region.

The solution captures information for organisations, services (APIs) used by the organisation
and details of the changes that these services have submitted.

This allows a single API to answer the questions:

- What data has changed in the services of organisation X?
- Who in organisation X has made the data changes to service Y?
- Was the update performed directly or on-behalf-of another individual?
- How was the data changed?

This approach also simplifies the data storage and data processing of each of the services,
since they only have to store a unique identifier for each change that is passed to this service.

