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

namespace Rasputin.API
{
    public static class HttpTriggerUserAPI
    {
        [FunctionName("HttpTriggerUserAPI")]
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
            string idList = req.Query["ids"];

            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try {
                List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-users", replyQueue);
                var cmd = new CmdUser() { Command = "list", Parameter = idList };
                var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }) };
                log.LogInformation("Sending message to queue");
                await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }));
                return new OkObjectResult(await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log));
            } finally {
               await MessageHelper.DeleteTmpQueue(connectionString, replyQueue);
            }
        }

        private static async Task<IActionResult> Post(HttpRequest req, ICollector<string> msg, ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var user = JsonSerializer.Deserialize<Users>(requestBody, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try {
                List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-users", replyQueue);
                var cmd = new CmdUser() { Command = "create", User = user };
                var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }) };
                await MessageHelper.SendMessageAsync(connectionString, "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }));
                return new OkObjectResult(await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log));
            } finally {
               await MessageHelper.DeleteTmpQueue(connectionString, replyQueue);
            }
        }
    }
}
