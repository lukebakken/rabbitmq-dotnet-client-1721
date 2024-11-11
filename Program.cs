using System.Text;
using RabbitMQ.Client;

const int MaxConcurrentCalls = 256;

var cf = new ConnectionFactory();

await using IConnection conn = await cf.CreateConnectionAsync();

var opts = new CreateChannelOptions(
    publisherConfirmationsEnabled: true,
    publisherConfirmationTrackingEnabled: true,
    outstandingPublisherConfirmationsRateLimiter: new ThrottlingRateLimiter(MaxConcurrentCalls)
);

await using IChannel ch = await conn.CreateChannelAsync(opts);

const int BatchSize = MaxConcurrentCalls / 2;
var publishTasks = new List<ValueTask>(BatchSize);

for (int i = 0; i < 65536;)
{
    byte[] msgBody = Encoding.ASCII.GetBytes(i.ToString());
    using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1)))
    {
        for (int j = 0; j < BatchSize; j++)
        {
            ValueTask publishTask = ch.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: string.Empty,
                mandatory: false,
                body: msgBody,
                cancellationToken: cts.Token);
            publishTasks.Add(publishTask);
        }

        foreach (ValueTask pt in publishTasks)
        {
            await pt;
        }
    }

    i += BatchSize;
    Console.WriteLine("{0} [INFO] published batch {1}", DateTime.Now, i);
}
