using Payments.Consumer.Services;
using Dapper;
using Npgsql;


var builder = WebApplication.CreateBuilder(args);

// ----------------------------------------------------------------------
// 1. Registrar o serviço de consumo do RabbitMQ
// ----------------------------------------------------------------------
builder.Services.AddHostedService<PaymentEventsConsumer>();

// ----------------------------------------------------------------------
// 2. Registrar Npgsql como connection factory (opcional, mas recomendado)
// ----------------------------------------------------------------------
builder.Services.AddSingleton<NpgsqlDataSource>(sp =>
{
    var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
        ?? throw new Exception("Connection string 'DefaultConnection' não encontrada!");

    return NpgsqlDataSource.Create(connectionString);
});

var app = builder.Build();

// ----------------------------------------------------------------------
// 3. Garantir que a tabela de eventos do consumidor existe
// ----------------------------------------------------------------------
await EnsureConsumerTables(app.Services, builder.Configuration);

// ----------------------------------------------------------------------
// 4. Healthcheck básico
// ----------------------------------------------------------------------
app.MapGet("/health", () => Results.Ok("payments-consumer-ok"));

app.Run();


// ======================================================================
// Funções auxiliares
// ======================================================================

async Task EnsureConsumerTables(IServiceProvider services, IConfiguration config)
{
    var connString = config.GetConnectionString("DefaultConnection");

    await using var con = new NpgsqlConnection(connString);
    await con.OpenAsync();

    var sql = """
        CREATE TABLE IF NOT EXISTS payment_events_consumed (
            id UUID PRIMARY KEY,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            received_at TIMESTAMPTZ NOT NULL
        );
    """;

    await con.ExecuteAsync(sql);

    Console.WriteLine("✔ Tabela payment_events_consumed verificada/criada.");
}
