
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
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

        #region Static Members
        //This function can manage both: connections to SQL Core model for CosmosDB
        //or MongoDB (in CosmosDB or other providers)        

        //Variables for SQL Core Model
        private static string _cosmosDbEndpointUri = string.Empty;
        private static string _cosmosDbPrimaryKey = string.Empty;
        //The data sent from the client to Azure Functions is supossed to come
        //in raw json format inside the body of the post message; 
        //specifically, as an array of json documents.
        //That's why we are using the JArray object.
        private static JArray _cosmosDbData = null;

        //Variables for MongoDB:
        private static string _mongoDbConnectionString = string.Empty;
        //For MongoDB instead of a JArray we use a BsonDocument Array
        private static BsonDocument[] _mongoDbData = null;

        //Common variables for both models
        private static string _dbName = string.Empty;
        private static string _dbCollectionName = string.Empty;
        private static ILogger _log;
        private static uint _successfullInsertions = 0;
        private static string _dbModelName = string.Empty;
        private static int _docCount = 0;
        private static string _requestData = string.Empty;
        private static IConfigurationRoot _config = null;
        private static bool _isBadRequest = false;
        private static bool _useMongoDb = false;


        #endregion

        [FunctionName("JsonInserter")]
        public static async Task<IActionResult> Run
            ([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]HttpRequest req,
            ILogger log,
            ExecutionContext context)
        {

            //Setting up our logger
            _log = log;

            //This is the proper .net core way to get the config object to access configuration variables
            //If we are in the development environment, app settings will be loaded from local.settings.json
            //In production, these variables will be loaded from the appsettings tab of the app service
            _config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();


            //Checking the working model for the function (from app settings)
            //And then inserting
            if (bool.TryParse(_config["useMongoDb"], out _useMongoDb))
            {
                //These parameters are common for both SQL and Mongo
                _dbName = _config["dbName"];
                _dbCollectionName = _config["dbCollectionName"];
                //Extracting data from POST
                _requestData = new StreamReader(req.Body).ReadToEnd();

                try
                {
                    if (_useMongoDb)
                    {
                        await MongoInsert();
                    }
                    else
                    {
                        await SqlInsert();
                    }

                }
                catch (Exception exc)
                {
                    log.LogError(exc.Message);
                    if (_isBadRequest)
                        return new ExceptionResult(exc, true);
                    else return new BadRequestObjectResult(exc.Message);
                }
                //Report the results of the operation
                string endingMessage = ReportResults();

                //Sending the response
                return new OkObjectResult(endingMessage);
            }
            else //useMongoDb parameter value in app settings is invalid
            {
                var msg = "useMongoDb parameter value is invalid";
                log.LogError(msg);
                return new BadRequestObjectResult(msg);
            }
        }


        /// <summary>
        /// Insertion of data in a CosmosDB with SQL Model
        /// </summary>
        private static async Task SqlInsert()
        {
            try
            {
                _cosmosDbData = (JArray)JsonConvert.DeserializeObject(_requestData);
            }
            catch (Exception exc)
            {
                _isBadRequest = true;
                //As we relay on the json info correctly beign parsed, 
                //if we have an exception trying to parse it, then we must finish the execution
                //Exceptions in this functions are handled writing a message in the log
                //so we could trace in which part of the fucntion it was originated
                //And returning the most suitable error message with the exception text
                //as the function response
                throw new Exception(ParamError(exc), exc);
            }
            //Setting up the connection string for SQL Core model
            _dbModelName = "CosmosDB";
            _cosmosDbEndpointUri = _config["cosmosDbEndpointUri"];
            _cosmosDbPrimaryKey = _config["cosmosDbPrimaryKey"];
            _docCount = _cosmosDbData.Count;
            //This is going to be the object that allows us to communicate with cosmosDB
            //It comes in the Microsoft.Azure.DocumentDB.Core package, so be sure to include it
            DocumentClient client;
            try
            {
                //Client must be initialized with the cosmosdb uri and key
                client = new DocumentClient(new Uri(_cosmosDbEndpointUri), _cosmosDbPrimaryKey);
                //If the desired database doesn't exist it is created
                await client.CreateDatabaseIfNotExistsAsync(new Database { Id = _dbName });
                //If the desired collection doesn't exist it is created
                await client.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(_dbName),
                new DocumentCollection { Id = _dbCollectionName });
            }
            catch (Exception exc)
            {
                throw new Exception(DbCommError(exc), exc);
            }

            //Now that we have the db context we can proceed to insert the json documents in data           
            foreach (var jobj in _cosmosDbData)
            {
                try
                {
                    await client.CreateDocumentAsync(
                        UriFactory.CreateDocumentCollectionUri(_dbName, _dbCollectionName),
                        jobj);
                    _successfullInsertions++;
                }
                catch (Exception exc)
                {
                    LogInsertionError(jobj, exc);
                }
            }
        }

        /// <summary>
        /// Insertion of data in a MongoDB (even in CosmosDB with the MongoDB model)
        /// </summary>
        private static async Task MongoInsert()
        {
            try
            {
                _mongoDbData = BsonSerializer.Deserialize<BsonDocument[]>(_requestData);
            }
            catch (Exception exc)
            {
                _isBadRequest = true;
                //As we relay on the json info correctly beign parsed, 
                //if we have an exception trying to parse it, then we must finish the execution
                //Exceptions in this functions are handled writing a message in the log
                //so we could trace in which part of the fucntion it was originated
                //And returning the most suitable error message with the exception text
                //as the function response
                throw new Exception(ParamError(exc), exc);
            }

            _docCount = _mongoDbData.Length;
            //Setting up the connection string for mongoDb model
            _dbModelName = "MongoDB";
            _mongoDbConnectionString = _config["mongoDbConnectionString"];


            //This is going to be the object that allows us to communicate with MongoDB
            //It comes in the MongoDb.Driver package, so be sure to include it   
            IMongoCollection<BsonDocument> collection = null;
            try
            {
                //Client must be initialized with the mongodb connection string
                var client = new MongoClient(_mongoDbConnectionString);
                //If the desired database doesn't exist it is created with the GetDatabase command
                var db = client.GetDatabase(_dbName);
                //If the desired collection doesn't exist it is created with GetCollection command
                collection = db.GetCollection<BsonDocument>(_dbCollectionName);

            }
            catch (Exception exc)
            {
                throw new Exception(DbCommError(exc), exc);
            }

            try
            {
                await collection.InsertManyAsync(_mongoDbData);
            }
            catch (Exception exc)
            {
                throw new Exception($"({DbCommError(exc)} - Inserting", exc);
            }
        }

        /// <summary>
        /// Assembles the error message when the json date from POST is incorrect
        /// </summary>
        /// <param name="exc"></param>
        /// <returns></returns>
        private static string ParamError(Exception exc)
        {
            return $"Problem with JSON input: {exc.Message}";
        }

        /// <summary>
        /// Assembles the error message when a connection with the database cannot be established
        /// </summary>
        /// <param name="exc">The exception originating the message</param>
        /// <returns>The error message when a connection with the database cannot be established </returns>
        private static string DbCommError(Exception exc)
        {
            return $"Problem communicating with {_dbModelName}: {exc.Message}";
        }

        /// <summary>
        /// We don't finish the execution tf there are errors inserting.
        /// The execution continues with the other documents.So this insertion is not transactional.        
        /// The failed insertions will be reported in the log, and at the end of the execution,
        /// in the response, it will be noted that not all the documents were inserted.
        /// This could help administrators to check what went wrong.
        /// </summary>
        /// <param name="jobj">The JObject that couldn't be inserted</param>
        /// <param name="exc">The generated exception</param>
        private static void LogInsertionError(JToken jobj, Exception exc)
        {

            _log.LogError($"Problem inserting document: {exc.Message}\n" +
                $"Content: {jobj.ToString()}");
        }

        /// <summary>
        /// A little report is generated as response if no exceptions were found
        /// This report indicates how many documents were inserted and what was the total
        /// of documents passed to the function to be processed
        /// </summary>
        /// <returns>The report string</returns>
        private static string ReportResults()
        {
            var endingMessage = _useMongoDb ?
                $"{_docCount} documents inserted successfully" :
                $"{_successfullInsertions} / {_docCount} documents inserted";
            endingMessage = $"{endingMessage} in {_dbModelName}: {_dbName}/{_dbCollectionName}";
            _log.LogInformation(endingMessage);
            return endingMessage;
        }

    }
}