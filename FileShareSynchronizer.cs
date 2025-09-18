namespace afssync
{
    using Azure.Storage;
    using Azure.Storage.Files.Shares;
    using Azure.Storage.Sas;
    using System.Threading.Channels;

    public class FileShareSynchronizer(Uri srcShareUri, Uri dstShareUri, StorageSharedKeyCredential srcSharedKey, StorageSharedKeyCredential dstSharedKey)
    {
        private readonly ShareClient _srcShare = new(srcShareUri, srcSharedKey);
        private readonly StorageSharedKeyCredential _srcKey = srcSharedKey;
        private readonly ShareClient _dstShare = new(dstShareUri, dstSharedKey);

        // Global concurrency limit for all file operations
        private const int MaxGlobalConcurrency = 32;

        public async Task SyncAsync()
        {
            if (!_srcShare.Exists())
            {
                throw new InvalidOperationException("Source share does not exist.");
            }

            await _dstShare.CreateIfNotExistsAsync();

            var srcRoot = _srcShare.GetRootDirectoryClient();
            var dstRoot = _dstShare.GetRootDirectoryClient();

            // Use a channel to queue file copy/delete operations
            var fileOpChannel = Channel.CreateUnbounded<Func<Task>>(new UnboundedChannelOptions { SingleReader = false, SingleWriter = false });
            var semaphore = new SemaphoreSlim(MaxGlobalConcurrency);

            // Start worker tasks
            var workers = Enumerable.Range(0, MaxGlobalConcurrency)
                .Select(_ => Task.Run(async () =>
                {
                    while (await fileOpChannel.Reader.WaitToReadAsync())
                    {
                        while (fileOpChannel.Reader.TryRead(out var op))
                        {
                            await semaphore.WaitAsync();
                            try { await op(); }
                            finally { semaphore.Release(); }
                        }
                    }
                }))
                .ToArray();

            // Traverse and enqueue all work
            await TraverseAndEnqueueAsync(srcRoot, dstRoot, "", fileOpChannel.Writer);

            fileOpChannel.Writer.Complete();
            await Task.WhenAll(workers);
        }

        private async Task TraverseAndEnqueueAsync(
            ShareDirectoryClient srcDir,
            ShareDirectoryClient dstDir,
            string relPath,
            ChannelWriter<Func<Task>> opWriter)
        {
            // Build a set of destination names for deletion check
            var dstNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            await foreach (var dstItem in dstDir.GetFilesAndDirectoriesAsync())
            {
                dstNames.Add(dstItem.Name);
            }

            // Process source items
            await foreach (var srcItem in srcDir.GetFilesAndDirectoriesAsync())
            {
                dstNames.Remove(srcItem.Name);

                if (srcItem.IsDirectory)
                {
                    var srcSub = srcDir.GetSubdirectoryClient(srcItem.Name);
                    var dstSub = dstDir.GetSubdirectoryClient(srcItem.Name);
                    await dstSub.CreateIfNotExistsAsync();
                    // Recurse for subdirectory
                    await TraverseAndEnqueueAsync(srcSub, dstSub, CombinePath(relPath, srcItem.Name), opWriter);
                }
                else
                {
                    var srcFile = srcDir.GetFileClient(srcItem.Name);
                    var dstFile = dstDir.GetFileClient(srcItem.Name);
                    var filePath = CombinePath(relPath, srcItem.Name);

                    // Enqueue file copy operation
                    await opWriter.WriteAsync(async () =>
                    {
                        bool shouldCopy = true;
                        try
                        {
                            var dstProps = await dstFile.GetPropertiesAsync();
                            var srcProps = await srcFile.GetPropertiesAsync();
                            if (srcProps.Value.ContentLength == dstProps.Value.ContentLength)
                            {
                                shouldCopy = false;
                            }
                        }
                        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
                        {
                            shouldCopy = true;
                        }

                        Console.WriteLine(shouldCopy ? $"Copying {filePath}" : $"Skipping {filePath}");

                        if (shouldCopy)
                        {
                            var sourceUri = srcFile.Uri;
                            if (_srcShare.CanGenerateSasUri)
                            {
                                var fileSas = new ShareSasBuilder
                                {
                                    ShareName = _srcShare.Name,
                                    FilePath = filePath,
                                    Resource = "f",
                                    ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(30)
                                };
                                fileSas.SetPermissions(ShareFileSasPermissions.Read);

                                var sasUri = new UriBuilder(srcFile.Uri)
                                {
                                    Query = fileSas.ToSasQueryParameters(_srcKey).ToString()
                                }.Uri;
                                sourceUri = sasUri;
                            }
                            await dstFile.StartCopyAsync(sourceUri);
                        }
                    });
                }
            }

            // Enqueue delete operations for extra destination items
            foreach (var extraName in dstNames)
            {
                var dstItem = dstDir.GetFileClient(extraName);
                var dstSub = dstDir.GetSubdirectoryClient(extraName);
                await opWriter.WriteAsync(async () =>
                {
                    try
                    {
                        var props = await dstItem.GetPropertiesAsync();
                        // It's a file
                        Console.WriteLine($"Deleting file {CombinePath(relPath, extraName)} in destination share.");
                        await dstItem.DeleteIfExistsAsync();
                    }
                    catch (Azure.RequestFailedException ex) when (ex.Status == 404)
                    {
                        // Not a file, try as directory
                        Console.WriteLine($"Deleting folder {CombinePath(relPath, extraName)} in destination share.");
                        await DeleteDirectoryRecursiveAsync(dstSub);
                    }
                });
            }
        }

        private static async Task DeleteDirectoryRecursiveAsync(ShareDirectoryClient dir)
        {
            await foreach (var item in dir.GetFilesAndDirectoriesAsync())
            {
                if (item.IsDirectory)
                {
                    var subDir = dir.GetSubdirectoryClient(item.Name);
                    Console.WriteLine($"Deleting folder {subDir.Name} in destination share.");
                    await DeleteDirectoryRecursiveAsync(subDir);
                }
                else
                {
                    var file = dir.GetFileClient(item.Name);
                    Console.WriteLine($"Deleting file {file.Name} in destination share.");
                    await file.DeleteIfExistsAsync();
                }
            }
            await dir.DeleteIfExistsAsync();
        }

        private static string CombinePath(string left, string right)
            => string.IsNullOrEmpty(left) ? right : $"{left.TrimEnd('/')}/{right.TrimStart('/')}";
    }
}