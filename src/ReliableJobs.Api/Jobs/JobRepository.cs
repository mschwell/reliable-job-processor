using System.Text.Json;
using Npgsql;
using NpgsqlTypes;

namespace ReliableJobs.Api.Jobs;

// The database is the source of truth. No in-memory queue means restarts do not lose work.
public sealed class JobRepository(NpgsqlDataSource dataSource)
{
    public async Task<QueueResult> EnqueueAsync(EnqueueRequest request, CancellationToken ct = default)
    {
        var id = Guid.NewGuid();
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        const string insert = """
            INSERT INTO jobs (id, kind, payload, idempotency_key, max_attempts, scheduled_for, available_at)
            VALUES ($1, $2, $3::jsonb, $4, $5, $6, coalesce($6, now()))
            ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
            RETURNING id;
            """;
        await using (var cmd = new NpgsqlCommand(insert, conn))
        {
            cmd.Parameters.AddWithValue(id);
            cmd.Parameters.AddWithValue(request.Kind);
            cmd.Parameters.AddWithValue(NpgsqlDbType.Jsonb, request.Payload.GetRawText());
            cmd.Parameters.Add(Text(request.IdempotencyKey));
            cmd.Parameters.AddWithValue(request.MaxAttempts);
            cmd.Parameters.Add(Timestamp(request.ScheduledFor));
            if (await cmd.ExecuteScalarAsync(ct) is Guid createdId)
                return new QueueResult(createdId, Existing: false, Conflict: false);
        }

        // A concurrent producer with the same key either submitted the same request
        // (idempotent replay) or a different request (409 Conflict).
        const string lookup = """
            SELECT id,
              (kind = $2 AND payload = $3::jsonb AND max_attempts = $4
                AND scheduled_for IS NOT DISTINCT FROM $5) AS is_match
            FROM jobs WHERE idempotency_key = $1;
            """;
        await using var check = new NpgsqlCommand(lookup, conn);
        check.Parameters.Add(Text(request.IdempotencyKey));
        check.Parameters.AddWithValue(request.Kind);
        check.Parameters.AddWithValue(NpgsqlDbType.Jsonb, request.Payload.GetRawText());
        check.Parameters.AddWithValue(request.MaxAttempts);
        check.Parameters.Add(Timestamp(request.ScheduledFor));
        await using var reader = await check.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
            throw new InvalidOperationException("Idempotency conflict not visible; retry the request.");
        return new QueueResult(reader.GetGuid(0), Existing: true, Conflict: !reader.GetBoolean(1));
    }

    public async Task<ClaimedJob?> ClaimNextAsync(int leaseSeconds, CancellationToken ct = default)
    {
        var token = Guid.NewGuid();
        const string sql = """
            WITH candidate AS (
                SELECT id FROM jobs
                WHERE status = 'queued' AND available_at <= now()
                ORDER BY available_at, created_at
                LIMIT 1 FOR UPDATE SKIP LOCKED
            ), claimed AS (
                UPDATE jobs j SET
                    status = 'running', attempts = j.attempts + 1,
                    lease_token = $1, lease_expires_at = now() + ($2 * interval '1 second'),
                    updated_at = now()
                FROM candidate c WHERE j.id = c.id
                RETURNING j.id, j.kind, j.payload::text AS payload,
                          j.attempts, j.max_attempts, j.lease_token
            ), history AS (
                INSERT INTO job_attempts (job_id, attempt_no, outcome)
                SELECT id, attempts, 'running' FROM claimed
                RETURNING job_id
            )
            SELECT c.id, c.kind, c.payload, c.attempts, c.max_attempts, c.lease_token
            FROM claimed c INNER JOIN history h ON h.job_id = c.id;
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(token);
        cmd.Parameters.AddWithValue(leaseSeconds);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct)) return null;
        return new ClaimedJob(
            reader.GetGuid(0), reader.GetString(1),
            JsonSerializer.Deserialize<JsonElement>(reader.GetString(2)),
            reader.GetInt32(3), reader.GetInt32(4), reader.GetGuid(5));
    }

    public async Task<int> RecoverExpiredAsync(CancellationToken ct = default)
    {
        // Locks prevent two recovery workers from reclaiming the same expired job.
        // The attempt history update and job status update are atomic (one statement).
        const string sql = """
            WITH expired AS (
                SELECT id, attempts, max_attempts FROM jobs
                WHERE status = 'running' AND lease_expires_at <= now()
                ORDER BY lease_expires_at
                LIMIT 25 FOR UPDATE SKIP LOCKED
            ), history AS (
                UPDATE job_attempts a
                SET outcome = 'lease_expired', completed_at = now(),
                    error = 'Lease expired; previous worker may have crashed.'
                FROM expired e
                WHERE a.job_id = e.id AND a.attempt_no = e.attempts
                  AND a.outcome = 'running'
                RETURNING a.job_id
            ), recovered AS (
                UPDATE jobs j SET
                    status = CASE WHEN e.attempts >= e.max_attempts THEN 'dead' ELSE 'queued' END,
                    available_at = now() + (LEAST(60, power(2, LEAST(e.attempts, 6))::integer) * interval '1 second'),
                    lease_token = NULL, lease_expires_at = NULL,
                    updated_at = now(),
                    last_error = 'Lease expired; requeued or dead-lettered.'
                FROM expired e WHERE j.id = e.id
                RETURNING j.id
            )
            SELECT count(*) FROM recovered;
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    public async Task<bool> RecordEffectAsync(Guid id, Guid leaseToken, string outputJson,
        CancellationToken ct = default)
    {
        // The unique PK(job_id) prevents duplicate DB effects on replay.
        // Real external APIs additionally need their own idempotency keys.
        const string sql = """
            INSERT INTO job_effects (job_id, output)
            SELECT j.id, $3::jsonb FROM jobs j
            WHERE j.id = $1 AND j.status = 'running'
              AND j.lease_token = $2 AND j.lease_expires_at > now()
            ON CONFLICT (job_id) DO NOTHING;
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(id);
        cmd.Parameters.AddWithValue(leaseToken);
        cmd.Parameters.AddWithValue(NpgsqlDbType.Jsonb, outputJson);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<bool> CompleteAsync(Guid id, Guid leaseToken, CancellationToken ct = default)
    {
        const string sql = """
            WITH updated AS (
                UPDATE jobs SET status = 'succeeded', lease_token = NULL,
                    lease_expires_at = NULL, updated_at = now(), completed_at = now(),
                    last_error = NULL
                WHERE id = $1 AND status = 'running' AND lease_token = $2
                  AND lease_expires_at > now()
                RETURNING id, attempts
            ), history AS (
                UPDATE job_attempts a SET outcome = 'succeeded', completed_at = now()
                FROM updated u WHERE a.job_id = u.id AND a.attempt_no = u.attempts
                RETURNING a.job_id
            )
            SELECT count(*) FROM history;
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(id);
        cmd.Parameters.AddWithValue(leaseToken);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)) == 1;
    }

    public async Task<bool> FailAsync(Guid id, Guid leaseToken, string error, int delaySeconds,
        CancellationToken ct = default)
    {
        const string sql = """
            WITH updated AS (
                UPDATE jobs SET
                    status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
                    available_at = now() + ($4 * interval '1 second'),
                    lease_token = NULL, lease_expires_at = NULL,
                    updated_at = now(), last_error = $3
                WHERE id = $1 AND status = 'running' AND lease_token = $2
                  AND lease_expires_at > now()
                RETURNING id, attempts
            ), history AS (
                UPDATE job_attempts a
                SET outcome = 'failed', completed_at = now(), error = $3
                FROM updated u WHERE a.job_id = u.id AND a.attempt_no = u.attempts
                RETURNING a.job_id
            )
            SELECT count(*) FROM history;
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(id);
        cmd.Parameters.AddWithValue(leaseToken);
        cmd.Parameters.AddWithValue(error);
        cmd.Parameters.AddWithValue(delaySeconds);
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)) == 1;
    }

    public async Task<JobSummary?> GetAsync(Guid id, CancellationToken ct = default)
    {
        const string sql = """
            SELECT j.id, j.kind, j.status, j.attempts, j.max_attempts,
                   j.available_at, j.created_at, j.completed_at, j.last_error,
                   e.output::text
            FROM jobs j LEFT JOIN job_effects e ON e.job_id = j.id
            WHERE j.id = $1;
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(id);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadSummary(reader) : null;
    }

    public async Task<IReadOnlyList<JobSummary>> ListAsync(string? status, int limit,
        CancellationToken ct = default)
    {
        const string sql = """
            SELECT j.id, j.kind, j.status, j.attempts, j.max_attempts,
                   j.available_at, j.created_at, j.completed_at, j.last_error,
                   e.output::text
            FROM jobs j LEFT JOIN job_effects e ON e.job_id = j.id
            WHERE ($1::text IS NULL OR j.status = $1)
            ORDER BY j.created_at DESC LIMIT $2;
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.Add(Text(status));
        cmd.Parameters.AddWithValue(limit);
        var jobs = new List<JobSummary>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) jobs.Add(ReadSummary(reader));
        return jobs;
    }

    public async Task<IReadOnlyList<JobAttempt>> HistoryAsync(Guid id, CancellationToken ct = default)
    {
        const string sql = """
            SELECT attempt_no, outcome, started_at, completed_at, error
            FROM job_attempts WHERE job_id = $1 ORDER BY attempt_no;
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(id);
        var history = new List<JobAttempt>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            history.Add(new JobAttempt(
                reader.GetInt32(0), reader.GetString(1), Utc(reader.GetDateTime(2)),
                reader.IsDBNull(3) ? null : Utc(reader.GetDateTime(3)),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        return history;
    }

    public async Task<bool> RetryDeadAsync(Guid id, CancellationToken ct = default)
    {
        // Attempts remain monotonic, so (job_id, attempt_no) audit keys stay unique.
        // A manual retry grants 3 additional attempts, preserving the complete history.
        const string sql = """
            UPDATE jobs SET status = 'queued', max_attempts = attempts + 3,
                available_at = now(), scheduled_for = NULL, updated_at = now(),
                completed_at = NULL, last_error = NULL
            WHERE id = $1 AND status = 'dead';
            """;
        await using var cmd = dataSource.CreateCommand(sql);
        cmd.Parameters.AddWithValue(id);
        return await cmd.ExecuteNonQueryAsync(ct) == 1;
    }

    public async Task<bool> CheckHealthAsync(CancellationToken ct = default)
    {
        await using var cmd = dataSource.CreateCommand("SELECT 1;");
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct)) == 1;
    }

    private static JobSummary ReadSummary(NpgsqlDataReader r) => new(
        r.GetGuid(0), r.GetString(1), r.GetString(2), r.GetInt32(3), r.GetInt32(4),
        Utc(r.GetDateTime(5)), Utc(r.GetDateTime(6)),
        r.IsDBNull(7) ? null : Utc(r.GetDateTime(7)),
        r.IsDBNull(8) ? null : r.GetString(8),
        r.IsDBNull(9) ? null : JsonSerializer.Deserialize<JsonElement>(r.GetString(9)));

    private static DateTimeOffset Utc(DateTime dateTime) => new(DateTime.SpecifyKind(dateTime, DateTimeKind.Utc));

    private static NpgsqlParameter Text(string? value) => new()
    {
        NpgsqlDbType = NpgsqlDbType.Text,
        Value = (object?)value ?? DBNull.Value
    };

    private static NpgsqlParameter Timestamp(DateTimeOffset? value) => new()
    {
        NpgsqlDbType = NpgsqlDbType.TimestampTz,
        Value = value.HasValue ? (object)value.Value.UtcDateTime : DBNull.Value
    };
}
