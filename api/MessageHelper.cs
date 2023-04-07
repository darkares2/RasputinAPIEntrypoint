using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;

public class MessageHelper{
    public static async Task SendMessageAsync(string connectionString, string queueName, string message)
    {
        await using var client = new ServiceBusClient(connectionString);
        ServiceBusSender sender = client.CreateSender(queueName);

        var messageBytes = Encoding.UTF8.GetBytes(message);
        ServiceBusMessage messageObject = new ServiceBusMessage(messageBytes);

        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        CancellationToken cancellationToken = cancellationTokenSource.Token;

        await sender.SendMessageAsync(messageObject, cancellationToken);
    }

    public static async Task<string> SetupTemporaryReplyQueue(string connectionString) {
        string replyQueue = $"tmp-reply-{Guid.NewGuid().ToString()}";

        await CreateTmpQueue(replyQueue, connectionString);

        return replyQueue;
    }

    public static List<MessageHeader> CreateHeaders(string destinationQueue, string replyQueue)
    {
        List<MessageHeader> headers = new List<MessageHeader>();
        headers.Add(new MessageHeader() { Name = "id-header", Fields = new Dictionary<string, string>() { { "GUID", Guid.NewGuid().ToString() } } });
        headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", destinationQueue }, { "Active", "true" } } });
        headers.Add(new MessageHeader() { Name = "route-header", Fields = new Dictionary<string, string>() { { "Destination", replyQueue }, { "Active", "true" } } });
        headers.Add(new MessageHeader() { Name = "current-queue-header", Fields = new Dictionary<string, string>() { { "Name", "api-router" }, { "Timestamp", DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ") } } });
        return headers;
    }

    public static async Task<string> WaitForReplyFromTemporarySbQueue(string connectionString, string replyQueue, ILogger log)
    {
        log.LogInformation($"Waiting for reply on queue {replyQueue}");
        await using var client = new ServiceBusClient(connectionString);
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
            await SendLog(response);
            return response.Body;
        } finally {
            await receiver.CloseAsync();
        }
    }

    public static Task DeleteTmpQueue(string connectionString, string replyQueue)
    {
        var client = new ServiceBusAdministrationClient(connectionString);
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

    public static async Task SendLog(Message message)
        {
            var idHeader = message.Headers.FirstOrDefault(x => x.Name.Equals("id-header"));
            var current = message.Headers.FirstOrDefault(x => x.Name.Equals("current-queue-header"));
            LogTimer logTimer = new LogTimer() {
                Id = Guid.Parse(idHeader.Fields["GUID"]),
                Queue = current.Fields["Name"],
                SentTimestamp = DateTime.ParseExact(current.Fields["Timestamp"], "yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal),
                ReceiveTimestamp = DateTime.UtcNow
            };
            await using var client = new ServiceBusClient(Environment.GetEnvironmentVariable("rasputinServicebus"));
            ServiceBusSender sender = client.CreateSender("ms-logtimer");
            string queueMessage = JsonSerializer.Serialize(logTimer, new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase
                });            
            var messageBytes = Encoding.UTF8.GetBytes(queueMessage);
            ServiceBusMessage messageObject = new ServiceBusMessage(messageBytes);

            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            CancellationToken cancellationToken = cancellationTokenSource.Token;

            await sender.SendMessageAsync(messageObject, cancellationToken);
            await sender.CloseAsync();
        }    

}  