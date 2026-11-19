using Npgsql;

namespace ReliableJobs.Api.Database;

public sealed class DbBootstrapper(NpgsqlDataSource dataSource)
{
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await using var stream = typeof(DbBootstrapper).Assembly.GetManifestResourceStream(
            "ReliableJobs.Api.Database.schema.sql")
            ?? throw new InvalidOperationException("Embedded schema.sql resource not found.");
        using var reader = new StreamReader(stream);
        var sql = await reader.ReadToEndAsync(cancellationToken);
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
