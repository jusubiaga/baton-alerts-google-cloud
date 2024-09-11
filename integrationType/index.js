const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const uuid = require("uuid");
const express = require("express");
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PROJECTID = "cald-ads-qa";
const DATA_SOURCE = "Alerts";
const TABLE = "integrationType";

const bigquery = new BigQuery({ projectId: PROJECTID });

const getData = async () => {
  const query = `
SELECT id, name, description, logo 
FROM \`cald-ads-qa.Alerts.integrationType\``;

  const options = {
    query: query,
    location: "US",
  };

  // create query
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  console.log("Rows:");
  rows.forEach((row) => {
    console.log(row);
  });

  return rows;
};

app.get("/", async function (req, res) {
  try {
    const { user, integrationType } = req.query;
    const data = await getData(user, integrationType);
    res.status(200).json(data);
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
  // return res.sendStatus(200);
});

functions.http("integrationType", app);

// functions.http("integrationType", async (req, res) => {
//   if (req.method === "GET") {
//     console.log("Waiting ...");

//     try {
//       const data = await getIntegrationType();

//       res.status(200).json(data);
//     } catch (error) {
//       res.send(`ERROR ${error}`);
//     }
//   } else {
//     res.status(404).json({});
//   }
// });
