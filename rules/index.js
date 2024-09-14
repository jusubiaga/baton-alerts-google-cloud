const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const uuid = require("uuid");
const express = require("express");
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PROJECTID = "cald-ads-qa";
const DATA_SOURCE = "Alerts";
const TABLE_RUlES = "rules";

const bigquery = new BigQuery({ projectId: PROJECTID });

const getData = async () => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RUlES}`;

  const query = `SELECT *
   FROM ${table}`;

  console.log(query);

  const options = {
    query: query,
    location: "US",
  };

  // create query
  const [job] = await bigquery.createQueryJob(options);
  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  return rows;
};

app.get("/", async function (req, res) {
  try {
    // const { user, integrationType } = req.query;
    const data = await getData();
    res.status(200).json(data);
  } catch (error) {
    res.send(error);
    // res.status(500).json({ error: "Internal Server Error" });
  }
});

functions.http("rules", app);
