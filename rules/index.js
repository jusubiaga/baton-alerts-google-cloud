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
const TABLE_BOTS = "bots";

const bigquery = new BigQuery({ projectId: PROJECTID });

const getDataByWorkspace = async (workspace) => {
  const tableRules = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RUlES}`;
  const tableBots = `${PROJECTID}.${DATA_SOURCE}.${TABLE_BOTS}`;

  const query = `WITH workspace_bots AS (
  SELECT rule
  FROM ${tableBots}
  WHERE workspace = "${workspace}"
)

SELECT 
  r.id, 
  r.name, 
  r.available, 
  r.avatar, 
  CASE 
    WHEN ub.rule IS NOT NULL THEN TRUE 
    ELSE FALSE 
  END AS installed
FROM ${tableRules} r
LEFT JOIN workspace_bots ub 
  ON r.id = ub.rule;`;

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
    const { workspace, user } = req.query;
    if (workspace) {
      const dataByWorkspace = await getDataByWorkspace(workspace);
      res.status(200).json(dataByWorkspace);
    } else {
      const data = await getData();
      res.status(200).json(data);
    }
  } catch (error) {
    res.send(error);
    // res.status(500).json({ error: "Internal Server Error" });
  }
});

functions.http("rules", app);
