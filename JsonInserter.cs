
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Web.Http;

namespace CosmosDBJSonInserter
{
    public static class JsonInserter
    {
        [FunctionName("JsonInserter")]
        public static async Task<IActionResult> Run
            ([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]HttpRequest req,
            ILogger log,
            ExecutionContext context)  //We are gonna need just the post. And observe we are using the new ILogger instead the deprecated tracer
        {

            //This is the proper .net core way to get the config object to access configuration variables
            //If we are in the development environment, app settings will be loaded from local.settings.json
            //In production, these variables will be loaded from the appsettings tab of the app service
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            //Getting the configuration variables (names are self explanatory)
            var cosmosDBEndpointUri = config["cosmosDBEndpointUri"];
            var cosmosDBPrimaryKey = config["cosmosDBPrimaryKey"];
            var cosmosDBName = config["cosmosDBName"];
            var cosmosDBCollectionName = config["cosmosDBCollectionName"];

            //The data sent from the client to Azure Functions is supossed to come
            //in raw json format inside the body of the post message; 
            //specifically, as an array of json documents.
            //That's why we are using the JArray object. To store these documents
            JArray data;
            string requestBody = new StreamReader(req.Body).ReadToEnd();
            try
            {
                data = (JArray)JsonConvert.DeserializeObject(requestBody);
            }
            catch(Exception exc)
            {
                //As we relay on the json info correctly beign parsed, 
                //if we have an exception trying to parse it, then we must finish the execution
                //Exceptions in this functions are handled writing a message in the log
                //so we could trace in which part of the fucntion it was originated
                //And returning the most suitable error message with the exception text
                //as the function response
                log.LogError($"Problem with JSON input: {exc.Message}");
                //Bad request since the format of the message is not as expected
                return new BadRequestObjectResult(exc); 
            }

            //This is going to be the object that allows us to communicate with cosmosDB
            //It comes in the Microsoft.Azure.DocumentDB.Core package, so be sure to include it
            DocumentClient client;
            try
            {
                //Client must be initialized with the cosmosdb uri and key
                client = new DocumentClient(new Uri(cosmosDBEndpointUri), cosmosDBPrimaryKey);
                //If the desired database doesn't exist it is created
                await client.CreateDatabaseIfNotExistsAsync(new Database { Id = cosmosDBName });
                //If the desired collection doesn't exist it is created
                await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(cosmosDBName),
                new DocumentCollection { Id = cosmosDBCollectionName });
            }
            catch (Exception exc)
            {
                log.LogError($"Problem communicating with CosmosDB: {exc.Message}");
                return new ExceptionResult(exc, true);
            }

            //Now that we have the db context we can proceed to insert the json documents in data
            uint successfullInsertions = 0;
            foreach (var jobj in data)
            {
                try
                {
                    await client.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(cosmosDBName, cosmosDBCollectionName),
                        jobj);
                    successfullInsertions++;
                }
                catch(Exception exc)
                {
                    //We don't finish the execution here. If there are errors, the execution continues
                    //with the other documents. So this insertion is not transactional.
                    //To make this exxecution transactional you must create a cosmosdb stored procedure
                    //with the transaction logic as there are no means right now to control transactions
                    //from cosmosdb clients. Just inside the engine.
                    //The failed insertions will be reported in the log, and at the end of the execution,
                    //in the response, it will be noted that not all the documents were inserted.
                    //This could help administrators to check what went wrong.
                    log.LogError($"Problem inserting document: {exc.Message}\n" +
                        $"Content: {jobj.ToString()}");                    
                }                
            }

            //A little report is generated as response if no exceptions were found
            //This report indicates how many documents were inserted and what was the total
            //of documents passed to the function to be processed
            var endingMessage = $"{successfullInsertions} / {data.Count} " +
                $"documents inserted in CosmosDB";
            log.LogInformation(endingMessage);
            return new OkObjectResult(endingMessage);
        }
    }
}
