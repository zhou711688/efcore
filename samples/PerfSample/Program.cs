using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace PerfSample
{
    class Program
    {
        private const int DefaultThreadCount = 32;
        private const int DefaultExecutionTimeSeconds = 5;
        private const int WarmupTimeSeconds = 3;
        private const string ConnectionString = "Data Source=PerfSample.db";
        public const string TestQuery = "SELECT id, message FROM fortune";

        private static int _counter;

        public static void IncrementCounter() => Interlocked.Increment(ref _counter);

        private static int _running;

        public static bool IsRunning => _running == 1;

        static async Task Main()
        {
#if DEBUG
            Console.WriteLine("WARNING! Using DEBUG build.");
#endif

            if (File.Exists("PerfSample.db"))
            {
                File.Delete("PerfSample.db");
            }

            // SqliteConnection.EnablePooling = false;

            using (var connection = new SqliteConnection(ConnectionString))
            {
                connection.Open();

                using (var createCommand = connection.CreateCommand())
                {
                    createCommand.CommandText =
                    @"
                        create table fortune (
	                        id int primary key,
	                        message nvarchar(2048)
                        );

                        INSERT INTO Fortune (id, message) VALUES (1, 'fortune: No such file or directory');
                        INSERT INTO Fortune (id, message) VALUES (2, 'A computer scientist is someone who fixes things that aren''t broken.');
                        INSERT INTO Fortune (id, message) VALUES (3, 'After enough decimal places, nobody gives a damn.');
                        INSERT INTO Fortune (id, message) VALUES (4, 'A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1');
                        INSERT INTO Fortune (id, message) VALUES (5, 'A computer program does what you tell it to do, not what you want it to do.');
                        INSERT INTO Fortune (id, message) VALUES (6, 'Emacs is a nice operating system, but I prefer UNIX. — Tom Christaensen');
                        INSERT INTO Fortune (id, message) VALUES (7, 'Any program that runs right is obsolete.');
                        INSERT INTO Fortune (id, message) VALUES (8, 'A list is only as strong as its weakest link. — Donald Knuth');
                        INSERT INTO Fortune (id, message) VALUES (9, 'Feature: A bug with seniority.');
                        INSERT INTO Fortune (id, message) VALUES (10, 'Computers make very fast, very accurate mistakes.');
                        INSERT INTO Fortune (id, message) VALUES (11, '<script>alert(""This should not be displayed in a browser alert box."");</script>');
                        INSERT INTO Fortune(id, message) VALUES(12, 'フレームワークのベンチマーク');
                    ";
                    createCommand.ExecuteNonQuery();
                }
            }

            var totalTransactions = 0;
            var results = new List<double>();
            DateTime startTime = default, stopTime = default;

            var threadCount = DefaultThreadCount;
            var time = DefaultExecutionTimeSeconds;

            IEnumerable<Task> CreateTasks()
            {
                yield return Task.Run(
                    async () =>
                    {
                        Console.Write($"Warming up for {WarmupTimeSeconds}s...");

                        await Task.Delay(TimeSpan.FromSeconds(WarmupTimeSeconds));

                        Console.SetCursorPosition(0, Console.CursorTop);

                        Interlocked.Exchange(ref _counter, 0);

                        startTime = DateTime.UtcNow;
                        var lastDisplay = startTime;

                        while (IsRunning)
                        {
                            await Task.Delay(200);

                            var now = DateTime.UtcNow;
                            var tps = (int)(_counter / (now - lastDisplay).TotalSeconds);
                            var remaining = (int)(time - (now - startTime).TotalSeconds);

                            results.Add(tps);

                            Console.Write($"{tps} tps, {remaining}s                     ");
                            Console.SetCursorPosition(0, Console.CursorTop);

                            lastDisplay = now;
                            totalTransactions += Interlocked.Exchange(ref _counter, 0);
                        }
                    });

                yield return Task.Run(
                    async () =>
                    {
                        await Task.Delay(TimeSpan.FromSeconds(WarmupTimeSeconds + time));

                        Interlocked.Exchange(ref _running, 0);

                        stopTime = DateTime.UtcNow;
                    });

                foreach (var task in Enumerable.Range(0, threadCount)
                    .Select(_ => Task.Factory.StartNew(DoWorkAsync, TaskCreationOptions.LongRunning).Unwrap()))
                {
                    yield return task;
                }
            }

            Interlocked.Exchange(ref _running, 1);

            await Task.WhenAll(CreateTasks());

            var totalTps = (int)(totalTransactions / (stopTime - startTime).TotalSeconds);

            results.Sort();
            results.RemoveAt(0);
            results.RemoveAt(results.Count - 1);

            double CalculateStdDev(ICollection<double> values)
            {
                var avg = values.Average();
                var sum = values.Sum(d => Math.Pow(d - avg, 2));

                return Math.Sqrt(sum / values.Count);
            }

            var stdDev = CalculateStdDev(results);

            Console.SetCursorPosition(0, Console.CursorTop);
            Console.WriteLine($"{threadCount:D2} Threads, tps: {totalTps:F2}, stddev(w/o best+worst): {stdDev:F2}");

            // Dispose all the pooled connections
            SqliteConnection.FreeHandles();

            File.Delete("PerfSample.db");
        }

        public static async Task DoWorkAsync()
        {
            while (Program.IsRunning)
            {
                var results = new List<Fortune>();

                using (var connection = new SqliteConnection(ConnectionString))
                {
                    await connection.OpenAsync();

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = Program.TestQuery;
                        command.Prepare();

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                results.Add(
                                    new Fortune
                                    {
                                        Id = reader.GetInt32(0),
                                        Message = reader.GetString(1)
                                    });
                            }
                        }
                    }
                }

                CheckResults(results);

                Program.IncrementCounter();
            }
        }

        protected static void CheckResults(ICollection<Fortune> results)
        {
            if (results.Count != 12)
            {
                throw new InvalidOperationException($"Unexpected number of results! Expected 12 got {results.Count}");
            }
        }

        public class Fortune
        {
            public int Id { get; set; }
            public string Message { get; set; }
        }
    }
}
