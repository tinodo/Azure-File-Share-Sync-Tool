namespace afssync
{
    using Azure.Storage;

    internal class Program
    {
        static async Task Main(string[] _)
        {
            var sourceStorageAccountName = "sourcestorageaccount";
            var sourceStorageAccountKey = "sourcestorageaccountaccesskey";
            var sourceFileShareName = "shourceshare";

            var destinationStorageAccountName = "destinationstorageaccount";
            var destinationStorageAccountKey = "destinationstorageaccountaccesskey";
            var destinationFileShareName = "destinationshare";

            var source = new Uri($"https://{sourceStorageAccountName}.file.core.windows.net/{sourceFileShareName}");
            var sourceKey = new StorageSharedKeyCredential(sourceStorageAccountName, sourceStorageAccountKey);
            var destination = new Uri($"https://{destinationStorageAccountName}.file.core.windows.net/{destinationFileShareName}");
            var destinationKey = new StorageSharedKeyCredential(destinationStorageAccountName, destinationStorageAccountKey);

            FileShareSynchronizer mirror = new(source, destination, sourceKey, destinationKey);
            await mirror.SyncAsync();
            Console.WriteLine("Finished.");
            Console.ReadLine();
        }
    }
}
