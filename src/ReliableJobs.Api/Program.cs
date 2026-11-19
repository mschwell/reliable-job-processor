using Npgsql;
using ReliableJobs.Api.Database;
using ReliableJobs.Api.Jobs;
using ReliableJobs.Api.Worker;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Jobs")
    ?? throw new InvalidOperationException(
        "Set ConnectionStrings__Jobs to a PostgreSQL connection string (see README).");

builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddSingleton<DbBootstrapper>();
builder.Services.AddSingleton<JobRepository>();
builder.Services.Configure<WorkerOptions>(builder.Configuration.GetSection("Worker"));
builder.Services.AddHostedService<JobWorker>();

var app = builder.Build();

// Safe for this small demo's CREATE IF NOT EXISTS schema. Production: versioned migrations.
await app.Services.GetRequiredService<DbBootstrapper>().InitializeAsync();

app.MapGet("/health", async (JobRepository jobs, CancellationToken ct) =>
{
    try { return await jobs.CheckHealthAsync(ct) ? Results.Ok(new { status = "healthy" })
                                                    : Results.StatusCode(503); }
    catch (Exception) { return Results.StatusCode(503); }
});

app.MapPost("/jobs", async (EnqueueRequest request, JobRepository jobs, CancellationToken ct) =>
{
    var issue = JobPolicy.Validate(request);
    if (issue is not null) return Results.BadRequest(new { error = issue });
    var queued = await jobs.EnqueueAsync(request, ct);
    if (queued.Conflict)
        return Results.Conflict(new { error = "Idempotency key already used for a different request.", queued.Id });
    return queued.Existing
        ? Results.Ok(new { queued.Id, reused = true, location = $"/jobs/{queued.Id}" })
        : Results.Created($"/jobs/{queued.Id}", new { queued.Id, reused = false });
});

app.MapGet("/jobs/{id:guid}", async (Guid id, JobRepository jobs, CancellationToken ct) =>
    await jobs.GetAsync(id, ct) is { } job ? Results.Ok(job) : Results.NotFound());

app.MapGet("/jobs", async (string? status, int? limit, JobRepository jobs, CancellationToken ct) =>
{
    if (status is not null && status is not ("queued" or "running" or "succeeded" or "dead"))
        return Results.BadRequest(new { error = "status must be queued, running, succeeded, or dead." });
    if (limit is < 1 or > 100) return Results.BadRequest(new { error = "limit must be 1 to 100." });
    return Results.Ok(await jobs.ListAsync(status, limit ?? 20, ct));
});

app.MapGet("/jobs/{id:guid}/attempts", async (Guid id, JobRepository jobs, CancellationToken ct) =>
{
    if (await jobs.GetAsync(id, ct) is null) return Results.NotFound();
    return Results.Ok(await jobs.HistoryAsync(id, ct));
});

app.MapPost("/jobs/{id:guid}/retry", async (Guid id, JobRepository jobs, CancellationToken ct) =>
{
    var existing = await jobs.GetAsync(id, ct);
    if (existing is null) return Results.NotFound();
    return await jobs.RetryDeadAsync(id, ct)
        ? Results.Accepted($"/jobs/{id}", new { id, message = "Granted 3 more attempts." })
        : Results.Conflict(new { error = "Only dead jobs can be retried." });
});

app.Run();

// Exposed only to allow later HTTP integration testing with WebApplicationFactory.
public partial class Program { }
