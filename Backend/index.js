import express from "express";
import { MongoClient } from "mongodb";
import mysql from "mysql2/promise";
import "dotenv/config.js";
import cookieParser from "cookie-parser";

const app = express();
app.use(express.json());
app.use(cookieParser());

const MongoDB_URI = process.env.MongoDB_URI;
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


  } catch (error) {
    console.log("Error occurred while Fetching", error);
  }
}

transferDataFromMongoDBToSQL();

app.listen(3000, () => {
  console.log("Server running on port 3000 Successfully!");
});
