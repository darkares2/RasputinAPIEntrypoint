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
using Azure.Messaging.ServiceBus;
using System.Text;
using Azure.Messaging.ServiceBus.Administration;

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
            await MessageHelper.SendMessageAsync(Environment.GetEnvironmentVariable("rasputinServicebus"), "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));
            return new OkObjectResult(await WaitForReplyFromTemporarySbQueue(replyQueue, log));
        }


        private static async Task<string> WaitForReplyFromTemporarySbQueue(string replyQueue, ILogger log)
        {
            
            log.LogInformation($"Waiting for reply on queue {replyQueue}");
            var str = Environment.GetEnvironmentVariable("rasputinServicebus");
            {
                await CreateTmpQueue(replyQueue, str);
            }
            await using var client = new ServiceBusClient(str);
            var receiver = client.CreateReceiver(replyQueue);

            // Set up a cancellation token with a timeout of 10 seconds
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            CancellationToken cancellationToken = cancellationTokenSource.Token;

            log.LogInformation($"Receiving message from queue {replyQueue}");
            try {
                ServiceBusReceivedMessage message = await receiver.ReceiveMessageAsync(null, cancellationToken);
                try
                {
                    // Process the message
                    log.LogInformation($"Received message: {message.Body}");

                    // Mark the message as completed so it is removed from the queue
                    await receiver.CompleteMessageAsync(message);
                }
                catch (Exception ex)
                {
                    // Handle the exception and optionally abandon or defer the message
                    log.LogInformation($"Error processing message: {ex.Message}");
                    await receiver.AbandonMessageAsync(message);
                }
                var response = JsonSerializer.Deserialize<Message>(Encoding.UTF8.GetString(message.Body), new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
                return response.Body;
            } finally {
                await receiver.CloseAsync();
                await DeleteTmpQueue(replyQueue, str);
            }
        }

        private static Task DeleteTmpQueue(string replyQueue, string str)
        {
            var client = new ServiceBusAdministrationClient(str);
            return client.DeleteQueueAsync(replyQueue);
        }

        private static async Task CreateTmpQueue(string replyQueue, string str)
        {
            var client = new ServiceBusAdministrationClient(str);

            if (!await client.QueueExistsAsync(replyQueue))
            {
                await client.CreateQueueAsync(new CreateQueueOptions(replyQueue) { /*RequiresSession = true, AutoDeleteOnIdle = TimeSpan.FromMinutes(5) not available in basic*/ });
            }
        }

        private static async Task<IActionResult> Post(HttpRequest req, ICollector<string> msg, ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var book = JsonSerializer.Deserialize<Books>(requestBody, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
            string replyQueue = $"tmp-reply-{Guid.NewGuid().ToString()}";

            List<MessageHeader> headers = new List<MessageHeader>();
            headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", Guid.NewGuid().ToString() } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", "ms-books" }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", replyQueue }, { "Active", "true" } } });
            headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", "api-router" } } });
            var cmd = new CmdUpdateBook() { Command = "create", Book = book };
            var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }) };
            await MessageHelper.SendMessageAsync(Environment.GetEnvironmentVariable("rasputinServicebus"), "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));
            return new OkObjectResult(await WaitForReplyFromTemporarySbQueue(replyQueue, log));
        }
    }
}
