using MongoDB.Bson;
using MongoDB.Driver;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace MongoToPostgresMigration
{
    public class MigrationService
    {
        private readonly IMongoDatabase _mongoDb;
        private readonly string _postgresConnection;

        public MigrationService(string mongoConnection, string mongoDbName, string postgresConnection)
        {
            var mongoClient = new MongoClient(mongoConnection);
            _mongoDb = mongoClient.GetDatabase(mongoDbName);
            _postgresConnection = postgresConnection;
        }

        public async Task MigrateAllCollectionsAsync()
        {
            var collectionsCursor = await _mongoDb.ListCollectionNamesAsync();
            var collections = await collectionsCursor.ToListAsync();

            foreach (var collectionName in collections)
            {
                Console.WriteLine($"📂 Migrating collection: {collectionName}");
                await MigrateCollectionAsync(collectionName);
            }
        }

        private async Task MigrateCollectionAsync(string collectionName)
        {
            var collection = _mongoDb.GetCollection<BsonDocument>(collectionName);
            var documents = await collection.Find(new BsonDocument()).ToListAsync();

            if (documents.Count == 0)
            {
                Console.WriteLine($"⚠️ No documents found in {collectionName}");
                return;
            }

            using var conn = new NpgsqlConnection(_postgresConnection);
            await conn.OpenAsync();

            // -------------------------
            // 1. Schema discovery
            // -------------------------
            foreach (var doc in documents)
            {
                await DiscoverSchemaAndCreateTablesAsync(conn, collectionName, doc, null);
            }

            // -------------------------
            // 2. Insert phase
            // -------------------------
            foreach (var doc in documents)
            {
                await InsertDocumentAsync(conn, collectionName, doc, null);
            }
        }

        // 🔍 Recursively discover schema and create tables
        private async Task DiscoverSchemaAndCreateTablesAsync(NpgsqlConnection conn, string tableName, BsonDocument doc, string parentTable)
        {
            // Create base table
            string createSql = $@"CREATE TABLE IF NOT EXISTS ""{tableName}"" (
                Id SERIAL PRIMARY KEY
            );";
            using (var cmd = new NpgsqlCommand(createSql, conn))
                await cmd.ExecuteNonQueryAsync();

            // Add ParentId if child
            if (parentTable != null)
            {
                string parentColSql = $@"ALTER TABLE ""{tableName}"" 
                                         ADD COLUMN IF NOT EXISTS ""ParentId"" INTEGER REFERENCES ""{parentTable}""(Id);";
                using var parentCmd = new NpgsqlCommand(parentColSql, conn);
                await parentCmd.ExecuteNonQueryAsync();
            }

            // Walk document fields
            foreach (var element in doc.Elements)
            {
                string colName = SanitizeColumnName(element.Name);

                if (element.Value.BsonType == BsonType.Array)
                {
                    string childTable = $"{tableName}_{colName}";
                    foreach (var item in element.Value.AsBsonArray)
                    {
                        if (item.BsonType == BsonType.Document)
                        {
                            await DiscoverSchemaAndCreateTablesAsync(conn, childTable, item.AsBsonDocument, tableName);
                        }
                        else
                        {
                            await EnsureScalarArrayTableExistsAsync(conn, childTable, tableName);
                        }
                    }
                }
                else if (element.Value.BsonType == BsonType.Document)
                {
                    string childTable = $"{tableName}_{colName}";
                    await DiscoverSchemaAndCreateTablesAsync(conn, childTable, element.Value.AsBsonDocument, tableName);
                }
                else
                {
                    string colType = GetPostgresType(element.Value);
                    string alterSql = $@"ALTER TABLE ""{tableName}"" 
                                         ADD COLUMN IF NOT EXISTS ""{colName}"" {colType};";
                    using var alterCmd = new NpgsqlCommand(alterSql, conn);
                    await alterCmd.ExecuteNonQueryAsync();
                }
            }
        }

        // 📝 Insert documents (recursively)
        private async Task InsertDocumentAsync(NpgsqlConnection conn, string tableName, BsonDocument doc, string parentTable, int? parentId = null)
        {
            // Scalars only
            var scalarElements = doc.Elements.Where(e => e.Value.BsonType != BsonType.Array && e.Value.BsonType != BsonType.Document).ToList();
            var scalarCols = scalarElements.Select(e => SanitizeColumnName(e.Name)).ToList();

            var colNames = string.Join(",", scalarCols.Select(c => $"\"{c}\""));
            var values = string.Join(",", scalarCols.Select((c, i) => $"@p{i}"));

            if (parentTable != null) { colNames = "\"ParentId\"," + colNames; values = "@pid," + values; }

            var insertSql = $@"INSERT INTO ""{tableName}"" ({colNames}) VALUES ({values}) RETURNING Id;";

            using var cmd = new NpgsqlCommand(insertSql, conn);

            int i = 0;
            foreach (var element in scalarElements)
            {
                var converted = ConvertBsonValue(element.Value);
                if (converted is Npgsql.NpgsqlParameter npgParam)
                {
                    npgParam.ParameterName = $"p{i}";
                    cmd.Parameters.Add(npgParam);
                }
                else
                {
                    cmd.Parameters.AddWithValue($"p{i}", converted ?? DBNull.Value);
                }
                i++;
            }

            if (parentTable != null)
                cmd.Parameters.AddWithValue("pid", parentId ?? (object)DBNull.Value);

            var newId = (int)await cmd.ExecuteScalarAsync();

            // Handle arrays + nested docs
            foreach (var element in doc.Elements)
            {
                string colName = SanitizeColumnName(element.Name);

                if (element.Value.BsonType == BsonType.Array)
                {
                    string childTable = $"{tableName}_{colName}";
                    foreach (var item in element.Value.AsBsonArray)
                    {
                        if (item.BsonType == BsonType.Document)
                        {
                            await InsertDocumentAsync(conn, childTable, item.AsBsonDocument, tableName, newId);
                        }
                        else
                        {
                            var scalarInsert = $@"INSERT INTO ""{childTable}"" (""ParentId"", ""Value"") VALUES (@pid, @val);";
                            using var scalarCmd = new NpgsqlCommand(scalarInsert, conn);
                            scalarCmd.Parameters.AddWithValue("pid", newId);
                            scalarCmd.Parameters.AddWithValue("val", item.ToString());
                            await scalarCmd.ExecuteNonQueryAsync();
                        }
                    }
                }
                else if (element.Value.BsonType == BsonType.Document)
                {
                    string childTable = $"{tableName}_{colName}";
                    await InsertDocumentAsync(conn, childTable, element.Value.AsBsonDocument, tableName, newId);
                }
            }
        }

        // 🏗️ Helper: scalar array table
        private async Task EnsureScalarArrayTableExistsAsync(NpgsqlConnection conn, string tableName, string parentTable)
        {
            string createSql = $@"CREATE TABLE IF NOT EXISTS ""{tableName}"" (
                Id SERIAL PRIMARY KEY
            );";
            using var cmd = new NpgsqlCommand(createSql, conn);
            await cmd.ExecuteNonQueryAsync();

            string parentColSql = $@"ALTER TABLE ""{tableName}"" 
                                     ADD COLUMN IF NOT EXISTS ""ParentId"" INTEGER REFERENCES ""{parentTable}""(Id);";
            using var parentCmd = new NpgsqlCommand(parentColSql, conn);
            await parentCmd.ExecuteNonQueryAsync();

            string valueColSql = $@"ALTER TABLE ""{tableName}"" 
                                    ADD COLUMN IF NOT EXISTS ""Value"" TEXT;";
            using var valueCmd = new NpgsqlCommand(valueColSql, conn);
            await valueCmd.ExecuteNonQueryAsync();
        }

        private string SanitizeColumnName(string name) =>
            name.Replace(".", "_").Replace(" ", "_").Replace("$", "dollar");

        private string GetPostgresType(BsonValue value) =>
            value.BsonType switch
            {
                BsonType.Int32 => "INTEGER",
                BsonType.Int64 => "BIGINT",
                BsonType.Double => "DOUBLE PRECISION",
                BsonType.Boolean => "BOOLEAN",
                BsonType.DateTime => "TIMESTAMP",
                BsonType.Document => "JSONB",
                BsonType.Array => "JSONB",
                _ => "TEXT"
            };

        private object? ConvertBsonValue(BsonValue value) =>
            value.BsonType switch
            {
                BsonType.Null => DBNull.Value,
                BsonType.Int32 => value.AsInt32,
                BsonType.Int64 => value.AsInt64,
                BsonType.Double => value.AsDouble,
                BsonType.Boolean => value.AsBoolean,
                BsonType.DateTime => value.ToUniversalTime(),
                BsonType.Document => new Npgsql.NpgsqlParameter
                {
                    Value = value.ToJson(),
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Jsonb
                },
                BsonType.Array => new Npgsql.NpgsqlParameter
                {
                    Value = value.ToJson(),
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Jsonb
                },
                _ => value.ToString()
            };
    }
}
