using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;

namespace ConsoleApp2
{
    class Program
    {
        static void Main(string[] args)
        {
            CopyActivities();
        }

        private static void CopyActivities()
        {
            //Change this from
            var fromConnectionString = "mongodb://admin:admin_123@localhost:27017";
            var toConnectionString = "mongodb://admin:admin_123@localhost:27017";

            var sourceClient = new MongoClient(fromConnectionString);
            IMongoDatabase sourceDb = sourceClient.GetDatabase("C_MasterData");
            var sourceDocs = sourceDb.GetCollection<BsonDocument>("EquipmentStatus");

            var destClient = new MongoClient(toConnectionString);
            IMongoDatabase destDb = destClient.GetDatabase("C_MasterData");
            var destDocs = destDb.GetCollection<BsonDocument>("EquipmentStatus");

            int currentPage = 1, pageSize = 2000;
            double totalDocuments = sourceDocs.CountDocuments(new BsonDocument());
            var totalPages = Math.Ceiling((totalDocuments / pageSize));
            Console.WriteLine("Started for total pages-" + totalPages);

            for (int i = currentPage; i <= totalPages; i++)
            {
                Console.WriteLine("Getting " + currentPage);
                var docs = sourceDocs.Find(new BsonDocument()).SortBy(bson => bson["_id"]).Skip((currentPage - 1) * pageSize).Limit(pageSize).ToList();
                destDocs.InsertMany(docs);
                Console.WriteLine("Completed " + currentPage);
                currentPage = currentPage + 1;
            }
            Console.WriteLine("Finished!");
        }

        private static void CopyUserProfile()
        {
            //Change this from
            var fromConnectionString = "mongodb://admin:admin_123@localhost:27017";
            var toConnectionString = "mongodb://admin:admin_123@localhost:27017";

            var dbClient = new MongoClient(fromConnectionString);
            IMongoDatabase db = dbClient.GetDatabase("C_User");
            var Users = db.GetCollection<BsonDocument>("UserProfile");


            var dbClient1 = new MongoClient(toConnectionString);
            IMongoDatabase db1 = dbClient1.GetDatabase("C_User");
            var Users1 = db1.GetCollection<BsonDocument>("UserProfile");

            int currentPage = 12, pageSize = 2000;
            double totalDocuments = 1596536;
            var totalPages = Math.Ceiling((totalDocuments / pageSize));
            Console.WriteLine("Started-" + totalPages);

            for (int i = currentPage; i < totalPages; i++)
            {
                Console.WriteLine("Getting " + currentPage);
                var docs = Users.Find(new BsonDocument()).SortBy(bson => bson["_id"]).Skip((currentPage - 1) * pageSize).Limit(pageSize).ToList();
                Users1.InsertMany(docs);
                Console.WriteLine("Completed " + currentPage);
                currentPage = currentPage + 1;
            }
            Console.WriteLine("Finished!");
        }
    }
}
