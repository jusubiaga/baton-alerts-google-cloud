const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const uuid = require("uuid");
const express = require("express");
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PROJECTID = "cald-ads-qa";
const DATA_SOURCE = "Alerts";
const TABLE_RUNLOG = "runLog";

const bigquery = new BigQuery({ projectId: PROJECTID });

const getData = async () => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RUNLOG}`;

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

const getDataByUser = async (user) => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RUNLOG}`;

  const query = `SELECT *
   FROM ${table}
   WHERE user = "${user}"`;

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
  const { user } = req.query;

  try {
    // const { user, integrationType } = req.query;
    if (user) {
      const data = await getDataByUser(user);
      res.status(200).json(data);
    } else {
      const data = await getData();
      res.status(200).json(data);
    }
  } catch (error) {
    console.error("runlog: ", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

app.get("/:id/detail", async function (req, res) {
  const { user } = req.query;

  try {
    // const { user, integrationType } = req.query;
    if (user) {
      const data = await getDataByUser(user);
      res.status(200).json(data);
    } else {
      const data = await getData();
      res.status(200).json(data);
    }
  } catch (error) {
    console.error("runlog: ", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

functions.http("runlog", app);
