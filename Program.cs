﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using MoreLinq;
using Serilog;
using StackExchange.Redis;

// ReSharper disable UnusedMember.Local
// ReSharper disable UnusedType.Global

namespace RedisMirror
{
    internal class Program
    {
        static Program()
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
        }

        private static async Task Main(string sourceHost, int sourceDbIndex, string targetHost, int targetDbIndex,
                                       int readTimeout = 30, int batchSize = 100, int keyPageSize = 2000,
                                       bool noConfirm = false)
        {
            if (string.IsNullOrEmpty(sourceHost))
            {
                Log.Logger.Fatal("Invalid source host. Value = {SourceHost}", sourceHost);
                return;
            }

            if (string.IsNullOrEmpty(targetHost))
            {
                Log.Logger.Fatal("Invalid target host. Value = {TargetHost}", targetHost);
                return;
            }

            var sourceConf =
                $"{sourceHost},defaultDatabase={sourceDbIndex},syncTimeout={TimeSpan.FromSeconds(readTimeout).TotalMilliseconds}";
            var targetConf = $"{targetHost},defaultDatabase={targetDbIndex}";

            if (!noConfirm)
            {
                Log.Logger.Information("Press any keys confirm these parameters: {NewLine}{Params}",
                    Environment.NewLine, new
                    {
                        sourceHost,
                        sourceDbIndex,
                        targetHost,
                        targetDbIndex,
                        batchSize
                    });

                Console.ReadKey(true);
            }

            using var targetMultiplexer = await ConnectionMultiplexer.ConnectAsync(targetConf);
            var targetDb = targetMultiplexer.GetDatabase(targetDbIndex);

            Log.Logger.Debug("Target multiplexer connected!");

            using var sourceMultiplexer = await ConnectionMultiplexer.ConnectAsync(sourceConf);
            var sourceDb = sourceMultiplexer.GetDatabase(sourceDbIndex);

            Log.Logger.Debug("Source multiplexer connected!");

            var server = sourceMultiplexer.GetServer(sourceMultiplexer.GetEndPoints().Single());
            var keys = server.Keys(pageSize: keyPageSize, database: sourceDbIndex).ToList();

            Log.Logger.Information("{Count} keys gathered from server {Server}.", keys.Count, server.EndPoint);

            foreach (var (batchIndex, keysBatch) in keys.Batch(batchSize).Index())
            {
                Log.Logger.Debug("Batch {batchIndex} started.", batchIndex);

                var startNew = Stopwatch.StartNew();

                await foreach (var (key, value) in ReadValues(keysBatch, sourceDb))
                {
                    await targetDb.KeyRestoreAsync(key, value, flags: CommandFlags.FireAndForget);
                }

                startNew.Stop();

                Log.Logger.Debug("Batch {batchIndex} took {time}.", batchIndex, startNew.Elapsed);
            }

            Log.Logger.Information("Done!");
        }

        private static async IAsyncEnumerable<KeyValuePair<RedisKey, RedisValue>> ReadValues(
            IEnumerable<RedisKey> keys, IDatabaseAsync redisDb)
        {
            var remaining = new HashSet<Task<KeyValuePair<RedisKey, RedisValue>>>(keys.Select(i =>
                redisDb.KeyDumpAsync(i).ContinueWith(t => new KeyValuePair<RedisKey, RedisValue>(i, t.Result))));

            while (remaining.Count != 0)
            {
                var task = await Task.WhenAny(remaining);
                remaining.Remove(task);
                yield return await task;
            }
        }
    }
}