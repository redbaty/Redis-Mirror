using System.Collections.Generic;

namespace RedisMirror
{
    internal static class LinqExtensions
    {
        public static async IAsyncEnumerable<T[]> Batch<T>(this IAsyncEnumerable<T> source, int batchSize)
        {
            var currentBatchBuffer = new Queue<T>(batchSize);

            await foreach (var item in source)
            {
                currentBatchBuffer.Enqueue(item);

                if (currentBatchBuffer.Count >= batchSize)
                {
                    yield return currentBatchBuffer.ToArray();
                    currentBatchBuffer.Clear();
                }
            }

            yield return currentBatchBuffer.ToArray();
        }
    }
}