# AzFunc_PlainJsonCosmosDBWriter
This is an Azure Function able to receive a plain JSON array in the raw body of the POST method and insert it on a specified collection of an Azure Cosmos DB. It is very useful to replace functionality offered by services such as Keen.io without the expensive costs it has. For example it could act as a webhook for SendGrid to send you the status of the email messages you have sent. Then you could review those statuses querying your Cosmos DB.

I have tested this solution with one of my customers and they could leave Keen.io and their USD$500/month cost. Now they are paying under USD$50 a month.

To make sure your function runs fine on your development environment, install the `Microsoft.Azure.DocumentDB.Core` package from Nuget.

If you think your function is not running fine due to a configuration file error, this is a sample of how it should look like: (I¿ve not included the `local.settings.json` in the repo as it will not be a good security practice - I could be publishing endpoint uris, keys, etc. You also should avoid that)

    {
      "IsEncrypted": false,
      "Values": {
        "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "AzureWebJobsDashboard": "UseDevelopmentStorage=true",
        "cosmosDBEndpointUri": "https:/sometest7-dbe0-4-id1-b9ee.documents.azure.com:443/",
        "cosmosDBPrimaryKey": "this7is7not7a7real7keyD25Op84gI4ztfqPJ51DDfOqfaAgQddvOlkjladkfjldsakjf47wdHsavsdBFBdNQ==",
        "cosmosDBName": "yourdbname"
        "cosmosDBCollectionName": "yourcollectionname"
      }
    } 
I've written a [post in my blog](http://warnov.com/@keenioreplacement) with more context info about this project.
Enjoy!
<!--stackedit_data:
eyJoaXN0b3J5IjpbLTEyODgzOTY2NTVdfQ==
-->