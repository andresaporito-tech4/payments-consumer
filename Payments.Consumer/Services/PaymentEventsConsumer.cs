using System.Text;
using System.Text.Json;
using Dapper;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Payments.Consumer.Services;

public class PaymentEventsConsumer : BackgroundService
{
    private readonly IConfiguration _config;

    public PaymentEventsConsumer(IConfiguration config)
    {
        _config = config;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory
        {
            HostName = _config["RabbitMq:Host"],
            UserName = _config["RabbitMq:User"],
            Password = _config["RabbitMq:Pass"]
        };

        var queue = _config["RabbitMq:Queue"];

        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.QueueDeclareAsync(
            queue: queue,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        Console.WriteLine($"[Consumer] Escutando fila: {queue}");

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (s, e) =>
        {
            var json = Encoding.UTF8.GetString(e.Body.ToArray());
            Console.WriteLine($"[Consumer] Mensagem recebida: {json}");

            await SaveEvent(json);

            await channel.BasicAckAsync(e.DeliveryTag, false);
        };

        await channel.BasicConsumeAsync(queue, autoAck: false, consumer);

        await Task.CompletedTask;
    }

    private async Task SaveEvent(string payload)
    {
        var connectionString = _config.GetConnectionString("DefaultConnection");

        await using var con = new NpgsqlConnection(connectionString);
        await con.OpenAsync();

        var sql = """
        INSERT INTO payment_events_consumed (id, event_type, payload, received_at)
        VALUES (@id, @eventType, @payload, @ts)
    """;

        await con.ExecuteAsync(sql, new
        {
            id = Guid.NewGuid(),
            eventType = "PaymentRequested",
            payload,
            ts = DateTime.UtcNow
        });
    }

}
