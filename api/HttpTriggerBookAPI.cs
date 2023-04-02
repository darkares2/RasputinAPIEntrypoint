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
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            if (req.Method == "POST")
            {
                return await Post(req, log);
            }
            else if (req.Method == "GET")
            {
                return await Get(req, log);
            }
            else
            {
                return new BadRequestObjectResult("Invalid request");
            }
        }

        private static async Task<IActionResult> Get(HttpRequest req, ILogger log)
        {
            string isbnList = req.Query["isbns"];
            var book = new Books() { ISBN = isbnList };

            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try
            {
                List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-books", replyQueue);
                var cmd = new CmdUpdateBook() { Command = "list", Book = book };
                var message = new Message()
                {
                    Headers = headers.ToArray(),
                    Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    })
                };
                log.LogInformation("Sending message to queue");
                await MessageHelper.SendMessageAsync(Environment.GetEnvironmentVariable("rasputinServicebus"), "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                }));
                return new OkObjectResult(await MessageHelper.WaitForReplyFromTemporarySbQueue(connectionString, replyQueue, log));
            }
            finally {
               await MessageHelper.DeleteTmpQueue(connectionString, replyQueue);
            }
        }

        private static async Task<IActionResult> Post(HttpRequest req, ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var book = JsonSerializer.Deserialize<Books>(requestBody, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });
            string connectionString = Environment.GetEnvironmentVariable("rasputinServicebus");
            string replyQueue = await MessageHelper.SetupTemporaryReplyQueue(connectionString);
            try {
                List<MessageHeader> headers = MessageHelper.CreateHeaders("ms-books", replyQueue);
                var cmd = new CmdUpdateBook() { Command = "create", Book = book };
                var message = new Message() { Headers = headers.ToArray(), Body = JsonSerializer.Serialize(cmd, new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                    }) };
                await MessageHelper.SendMessageAsync(Environment.GetEnvironmentVariable("rasputinServicebus"), "api-router", JsonSerializer.Serialize(message, new JsonSerializerOptions
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
