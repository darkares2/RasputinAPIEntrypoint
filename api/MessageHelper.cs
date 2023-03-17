using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

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
}  