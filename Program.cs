using Microsoft.Extensions.Configuration;
using System.IO;

namespace MongoToPostgresMigration
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Load configuration
            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            string mongoConnection = config.GetConnectionString("MongoDb");
            string mongoDbName = config.GetSection("ConnectionStrings:MongoDbName").Value;
            string postgresConnection = config.GetConnectionString("Postgres");

            var migrationService = new MigrationService(mongoConnection, mongoDbName, postgresConnection);
            await migrationService.MigrateAllCollectionsAsync();

            Console.WriteLine("✅ Migration completed.");
        }
    }

    
}
