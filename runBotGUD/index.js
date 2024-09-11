const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const { v4 } = require("uuid");

const PROJECTID = "cald-ads-qa";
const DATA_SOURCE = "Alerts";
const TABLE = "runLog";
const TABLE_RUNLOG_DETAIL = "runLogDetail";

const DATA_SOURCE_CAMPAGIN = "Alerts";
const TABLE_CAMPAGIN = "CampaingAssets";

const bigquery = new BigQuery({ projectId: PROJECTID });

const addRunLog = async (data) => {
  const query = `
  INSERT INTO  ${PROJECTID}.${DATA_SOURCE}.${TABLE}
  (id, createdAt, user, rule, botId, status, error)
  VALUES ("${data.id}",
      "${data.createdAt}",
      "${data.user}",
      "${data.rule}",
      "${data.botId}",
      "${data.status}",
      "${data.error}")`;

  const options = {
    query: query,
    location: "US",
  };

  // create query
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Insert data`);

  // Wait for the query to finish
  await job.getQueryResults();
};
const updateRunLog = async (id, status) => {
  const query = `
  UPDATE ${PROJECTID}.${DATA_SOURCE}.${TABLE}
  SET status = "${status}"
  WHERE id = "${id}"`;

  // create query
  const [job] = await bigquery.createQueryJob({
    query: query,
    location: "US",
  });
  console.log(`Update status`);

  // Wait for the query to finish
  await job.getQueryResults();
};

const addRunLogDetail = async (id, rows) => {
  if (rows.length > 0) {
    let insert = `INSERT INTO ${PROJECTID}.${DATA_SOURCE}.${TABLE_RUNLOG_DETAIL}
    (id, campagin_id, campagin_name, group_id, gruop_name, count) VALUES `;

    let values = "";
    rows.forEach((row, index) => {
      values += `("${id}", "${row.campagin_id}", "${row.campagin_name}", "${row.group_id}", "${row.gruop_name}", ${row.headline})`;
      index < rows.length - 1 ? (values += ",") : (values += "");
    });

    insert += values;
    console.log(insert);

    const [i] = await bigquery.createQueryJob({ query: insert, location: "US" });
    await i.getQueryResults();
    console.log(`Inserted RUNLOG DETAILS`);
  }
};

const runProcess = async (id) => {
  try {
    const query = `
    SELECT campagin_id, MAX(\` campagin_name\`) as campagin_name, group_id, MAX(group_name) as gruop_name, count(*) as headline 
    FROM \`cald-ads-qa.AssetTest.CampaingAssets\`
    WHERE asset_type = "Headline"
    GROUP BY campagin_id, group_id
    ORDER BY campagin_id, group_id`;

    const options = {
      query: query,
      location: "US",
    };

    // create query
    const [job] = await bigquery.createQueryJob(options);
    console.log(`Job ${job.id} started.`);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();

    addRunLogDetail(id, rows);

    updateRunLog(id, "DONE");

    console.log(rows);
  } catch (error) {
    console.log(error);
  }
};

functions.cloudEvent("runBotGUD", async (cloudEvent) => {
  // The Pub/Sub message is passed as the CloudEvent's data payload.
  const base64name = cloudEvent.data.message.data;

  const data = base64name ? Buffer.from(base64name, "base64").toString() : "{}";

  try {
    const dataJson = JSON.parse(data);
    const id = v4();
    await addRunLog({
      id,
      createdAt: bigquery.timestamp(new Date()).value,
      user: dataJson?.user,
      rule: dataJson?.rule,
      botId: dataJson?.id,
      status: "RUNNING",
      error: "",
    });

    await runProcess(id);
    console.log("DONE!");
  } catch (error) {
    console.log("ERROR! ", error);
  }
});

// functions.http("runBotGUD", async (req, res) => {
//   if (req.method === "GET") {
//     console.log("Waiting ...");

//     try {
//       const id = v4();
//       const data = await addRunLog({
//         id,
//         createdAt: bigquery.timestamp(new Date()).value,
//         user: "dataJson?.user",
//         rule: "dataJson?.rule",
//         botId: "dataJson?.id",
//         status: "RUNNING",
//         error: "",
//       });

//       await runProcess(id);
//       res.status(200).json(data);
//     } catch (error) {
//       res.send(`ERROR ${error}`);
//     }
//   } else {
//     res.status(404).json({});
//   }
// });
