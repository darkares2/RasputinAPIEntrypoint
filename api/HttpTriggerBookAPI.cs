using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Threading;

namespace Rasputin.API
{
    public static class HttpTriggerBookAPI
    {
        [FunctionName("HttpTriggerBookAPI")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            [Queue("api-router"),StorageAccount("rasputinstorageaccount_STORAGE")] ICollector<string> msg, 
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            if (req.Method == "POST")
            {
                return await Post(req, msg, log);
            }
            else if (req.Method == "GET")
            {
                return await Get(req, msg, log);
            }
            else
            {
                return new BadRequestObjectResult("Invalid request");
            }

        }

        private static async Task<IActionResult> Get(HttpRequest req, ICollector<string> msg, ILogger log)
        {
            string isbnList = req.Query["isbns"];
            var book = new Books() { ISBN = isbnList };

            string replyQueue = $"tmp-reply-{Guid.NewGuid().ToString()}";
            List<MessageHeader> headers = new List<MessageHeader>();
            headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", Guid.NewGuid().ToString() } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", "ms-books" }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", replyQueue }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", "api-router" } } });
            var cmd = new CmdUpdateBook() { Command = "list", Book = book };
            var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }) };
            log.LogInformation("Sending message to queue");
            msg.Add(JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));

            return new OkObjectResult(await WaitForReply(replyQueue, log));
        }

        private static async Task<string> WaitForReply(string replyQueue, ILogger log)
        {
            var str = Environment.GetEnvironmentVariable("rasputinstorageaccount_STORAGE");
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(str);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = queueClient.GetQueueReference(replyQueue);
            await queue.CreateIfNotExistsAsync();

            log.LogInformation("Waiting for reply");
            // Set up a cancellation token with a timeout of 10 seconds
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            CancellationToken cancellationToken = cancellationTokenSource.Token;

            CloudQueueMessage queueItem = null;
            while(queueItem == null && !cancellationToken.IsCancellationRequested) {
                queueItem = await queue.GetMessageAsync(
                    TimeSpan.FromSeconds(10),
                    new QueueRequestOptions() 
                    { 
                        MaximumExecutionTime = TimeSpan.FromSeconds(10), 
                        ServerTimeout = TimeSpan.FromSeconds(10), 
                        RetryPolicy = new Microsoft.WindowsAzure.Storage.RetryPolicies.ExponentialRetry() 
                    },
                    new OperationContext() { ClientRequestID = Guid.NewGuid().ToString() },
                    cancellationToken);                
            }
            await queue.DeleteIfExistsAsync();
            if (cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("Timeout waiting for reply");
            }
            var message = JsonSerializer.Deserialize<Message>(queueItem.AsString, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });
            return message.Body;
        }

        private static async Task<IActionResult> Post(HttpRequest req, ICollector<string> msg, ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var book = JsonSerializer.Deserialize<Books>(requestBody, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
            List<MessageHeader> headers = new List<MessageHeader>();
            headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", Guid.NewGuid().ToString() } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", "ms-books" }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", "tmp-reply" }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", "api-router" } } });
            var cmd = new CmdUpdateBook() { Command = "create", Book = book };
            var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }) };
            msg.Add(JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));
            return new OkObjectResult("");
        }
    }
}
