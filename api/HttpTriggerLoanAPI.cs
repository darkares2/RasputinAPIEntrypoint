using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Text.Json;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System.Threading;
using System.Text.Json.Serialization;

namespace Rasputin.API
{
    public static class HttpTriggerLoanAPI
    { 
        [FunctionName("HttpTriggerLoanAPI")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "put", "post", Route = null)] HttpRequest req,
            [Queue("api-router"),StorageAccount("rasputinstorageaccount_STORAGE")] ICollector<string> msg, 
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            if (req.Method == "POST")
            {
                return await Post(req, msg, log);
            }
            if (req.Method == "PUT")
            {
                return await Put(req, msg, log);
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
            string isbn = req.Query["isbn"];
            if (isbn != null)
            {
                return await GetLoanHistoryByISBN(isbn, req, msg, log);
            }
            string idList = req.Query["ids"];

            string replyQueue = $"tmp-reply-{Guid.NewGuid().ToString()}";
            List<MessageHeader> headers = new List<MessageHeader>();
            headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", Guid.NewGuid().ToString() } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", "ms-loans" }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", replyQueue }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", "api-router" } } });
            var cmd = new CmdLoan() { Command = "list_active_books_user", Parameter = idList };
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

        private static async Task<IActionResult> GetLoanHistoryByISBN(string isbn, HttpRequest req, ICollector<string> msg, ILogger log)
        {
            log.LogInformation($"Get loan history for ISBN {isbn}");
            string replyQueue = $"tmp-reply-{Guid.NewGuid().ToString()}";
            List<MessageHeader> headers = new List<MessageHeader>();
            headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", Guid.NewGuid().ToString() } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", "ms-loans" }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", replyQueue }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", "api-router" } } });
            var cmd = new CmdLoan() { Command = "list_loan_history_by_isbn", Parameter = isbn };
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
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            CancellationToken cancellationToken = cancellationTokenSource.Token;

            CloudQueueMessage queueItem = null;
            while(queueItem == null && !cancellationToken.IsCancellationRequested) {
                queueItem = await queue.GetMessageAsync(
                    null,
                    new QueueRequestOptions() 
                    { 
                        MaximumExecutionTime = TimeSpan.FromSeconds(20), 
                        ServerTimeout = TimeSpan.FromSeconds(20), 
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
            var loan = JsonSerializer.Deserialize<Loans>(requestBody, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    NumberHandling = JsonNumberHandling.AllowReadingFromString
                });
            loan.Active = true;
            if (loan.LoanTimestamp == DateTime.MinValue)
            {
                loan.LoanTimestamp = DateTime.UtcNow;
            }
            log.LogInformation($"Loan: {loan.ISBN} {loan.UserId} {loan.LoanTimestamp} {loan.Active}");
            string replyQueue = $"tmp-reply-{Guid.NewGuid().ToString()}";
            List<MessageHeader> headers = new List<MessageHeader>();
            headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", Guid.NewGuid().ToString() } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", "ms-loans" }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", replyQueue }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", "api-router" } } });
            var cmd = new CmdLoan() { Command = "loan", Loan = loan };
            var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }) };
            msg.Add(JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));
            return new OkObjectResult(await WaitForReply(replyQueue, log));
        }

        private static async Task<IActionResult> Put(HttpRequest req, ICollector<string> msg, ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var loan = JsonSerializer.Deserialize<Loans>(requestBody, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    NumberHandling = JsonNumberHandling.AllowReadingFromString
                });
            loan.Active = false;
            log.LogInformation($"Loan: {loan.ISBN} {loan.UserId} {loan.LoanTimestamp} {loan.Active}");
            string replyQueue = $"tmp-reply-{Guid.NewGuid().ToString()}";
            List<MessageHeader> headers = new List<MessageHeader>();
            headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", Guid.NewGuid().ToString() } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", "ms-loans" }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", replyQueue }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", "api-router" } } });
            var cmd = new CmdLoan() { Command = "return", Loan = loan };
            var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }) };
            msg.Add(JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));
            return new OkObjectResult(await WaitForReply(replyQueue, log));
        }
    }
}
