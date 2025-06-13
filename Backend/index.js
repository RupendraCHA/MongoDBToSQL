import express from "express";
import { MongoClient } from "mongodb";
import mysql from "mysql2/promise";
import "dotenv/config.js";
import cookieParser from "cookie-parser";

const app = express();
app.use(express.json());
app.use(cookieParser());

const MongoDB_URI = process.env.MongoDB_URI;
// const MongoDB_URI = "mongodb+srv://rupendrachandaluri:R9912192624r@cluster0.iqrea.mongodb.net/HANElytics_Clients?retryWrites=true&w=majority&appName=Cluster0";
const MongoDB_Database = process.env.MongoDB_Database;
const MongoDB_Collection = process.env.MongoDB_Collection;

const mySQLConfig = {
  host: process.env.SQL_Host,
  user: process.env.SQL_User,
  password: process.env.SQL_Password,
  database: process.env.SQL_Database,
};

const SQL_Table_Name = process.env.SQL_Table_Name;

async function transferDataFromMongoDBToSQL() {
  try {
    // Connect to MongoDB
    const mongoClient = new MongoClient(MongoDB_URI);

    await mongoClient.connect();

    const db = mongoClient.db(MongoDB_Database);
    const collection = db.collection(MongoDB_Collection);

    const mongoDataOfLeads = await collection.find({}).toArray();

    const partOfLeads = mongoDataOfLeads.slice(0, 5);
    console.log(
      `Fetched MongoDB Data & No.Of.Documents - ${mongoDataOfLeads.length}`
    );

    if (partOfLeads.length === 0) {
      console.log("No data found.");
      return;
    }


    const cleanedData = mongoDataOfLeads.map(doc => {
        delete doc._id
        delete doc.updatedAt
        delete doc.createdAt
        delete doc.__v
        return doc
    })

    const keys = Object.keys(cleanedData[0])
    console.log(keys)

    // Connect to MySQL

    const mysqlConn = await mysql.createConnection(mySQLConfig)

    const columns = keys.map(key => `\`${key}\` TEXT`).join(", ")
    const createQuery = `CREATE TABLE IF NOT EXISTS \`${SQL_Table_Name}\` (${columns})`;
    await mysqlConn.execute(createQuery)
    console.log(`Table ${SQL_Table_Name} `)

    await mysqlConn.execute(`DELETE FROM \`${SQL_Table_Name}\``);

    // 6. Insert data
    for (const row of cleanedData) {
  const placeholders = keys.map(() => '?').join(', ');
  const values = keys.map(k => row[k] === undefined ? null : row[k]);
  const insertQuery = `INSERT INTO \`${SQL_Table_Name}\` (${keys.map(k => `\`${k}\``).join(', ')}) VALUES (${placeholders})`;
  await mysqlConn.execute(insertQuery, values);
}

    console.log(`✅ Inserted ${cleanedData.length} rows into MySQL`);

    await mysqlConn.end();
    await mongoClient.close();
  } catch (error) {
    console.log("Error occurred while Fetching", error);
  }
}

transferDataFromMongoDBToSQL();

// const { MongoClient } = require('mongodb');

// Replace with your actual MongoDB connection string
// const uri = "mongodb+srv://<username>:<password>@<cluster>.mongodb.net";

// const MongoDB_URI = "mongodb+srv://rupendrachandaluri:R9912192624r@cluster0.iqrea.mongodb.net/HANElytics_Clients?retryWrites=true&w=majority&appName=Cluster0";

// async function listDatabases() {
//   const client = new MongoClient(MongoDB_URI);

//   try {
//     await client.connect();
//     const databasesList = await client.db().admin().listDatabases();

//     console.log("Databases:");
//     databasesList.databases.forEach(db => console.log(` - ${db.name}`));
//   } catch (err) {
//     console.error("Error listing databases:", err);
//   } finally {
//     await client.close();
//   }
// }

// listDatabases();

app.listen(3000, () => {
  console.log("Server running on port 3000 Successfully!");
});
