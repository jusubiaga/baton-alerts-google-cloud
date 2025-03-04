const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const { v4 } = require("uuid");

const PROJECTID = "cald-ads-qa";
const DATA_SOURCE = "Alerts";
const TABLE = "runLog";
const TABLE_RUNLOG_DETAIL = "runLogDetail";
const TABLE_BOT = "bots";

const DATA_SOURCE_CAMPAGIN = "Alerts";
const TABLE_CAMPAGIN = "CampaingAssets";

const bigquery = new BigQuery({ projectId: PROJECTID });

const addRunLog = async (data) => {
  const query = `
  INSERT INTO  ${PROJECTID}.${DATA_SOURCE}.${TABLE}
  (id, idLabel, createdAt, user, rule, botId, status, error)
  VALUES ("${data.id}",
      "${data.idLabel}", 
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
const updateRunLog = async (id, status, error) => {
  const query = `
  UPDATE ${PROJECTID}.${DATA_SOURCE}.${TABLE}
  SET status = "${status}",
      error = "${error}"
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
    console.log(`Insert RUNLOG DETAILS ...`, rows);
    let insert = `INSERT INTO ${PROJECTID}.${DATA_SOURCE}.${TABLE_RUNLOG_DETAIL}
    (id, campagin_id, campagin_name, group_id, group_name, count, diff, status, resource_id ) VALUES `;

    let values = "";
    rows.forEach((row, index) => {
      values += `("${id}", "${row.campagin_id}", "${row.campagin_name}", "${row.group_id}", "${row.group_name}", ${row.headline}, ${row?.diff}, "${row?.status}", "${row?.resource_id}")`;
      index < rows.length - 1 ? (values += ",") : (values += "");
    });

    insert += values;
    console.log(insert);

    const [i] = await bigquery.createQueryJob({ query: insert, location: "US" });
    await i.getQueryResults();
    console.log(`Inserted RUNLOG DETAILS`);
  }
};

const generateNextLogIdLabel = async (user, rule) => {
  const tableBot = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;

  const query = `
    SELECT count(*) as id 
    FROM ${tableBot} as logs
    where rule = "${rule}" and
          user = "${user}"`;

  console.log(query);

  const options = {
    query: query,
    location: "US",
  };

  // create query
  const [job] = await bigquery.createQueryJob(options);
  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  return rows[0].id + 1 ?? null;
};

const getDataById = async (id) => {
  const tableBot = `${PROJECTID}.${DATA_SOURCE}.${TABLE_BOT}`;

  const query = `
    SELECT id, rule, minimumNumber  
    FROM ${tableBot} as bots
    WHERE bots.id = "${id}"`;

  console.log(query);

  const options = {
    query: query,
    location: "US",
  };

  // create query
  const [job] = await bigquery.createQueryJob(options);
  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  return rows[0] ?? null;
};

const runProcess = async (id, botId) => {
  const bot = await getDataById(botId);

  console.log("RUNPROCESS: ", bot);
  const minimumNumber = bot && bot?.minimumNumber ? bot?.minimumNumber : 0;

  // SELECT campagin_id, MAX(\` campagin_name\`) as campagin_name, group_id, MAX(group_name) as gruop_name, count(*) as headline, count(*) - ${minimumNumber} as diff, IF(count(*)>${minimumNumber}, "Headlines missing","No Issues") as status
  //   FROM \`cald-ads-qa.AssetTest.CampaingAssets\`
  //   WHERE asset_type = "Headline"
  //   GROUP BY campagin_id, group_id
  //   ORDER BY campagin_id, group_id`;

  try {
    const query = `
    SELECT date, group_id, MAX(group_name) as group_name, campaign_id, MAX(campaign_name) as campaign_name, customer_id , asset_type, MAX(group_asset_resource_id) as resource_id, count(*) as headline, ${minimumNumber} - count(*) as diff,  IF(${minimumNumber} > count(*), "FOUND_ISSUES","NO_FOUND_ISSUES") as status 
    FROM \`cald-ads-qa.AssetTest.assets-view\`
    where  date = "2025-01-13" and
           asset_type = "HEADLINE"
    group by group_id, campaign_id, asset_type, customer_id,  date
    order by date, group_id, campaign_id, asset_type desc`;

    const options = {
      query: query,
      location: "US",
    };

    // create query
    const [job] = await bigquery.createQueryJob(options);
    console.log(`Job ${job.id} started.`);

    // Wait for the query to finish
    const [rows] = await job.getQueryResults();

    await addRunLogDetail(id, rows);

    // update status
    let issuesCount = 0;
    rows.forEach((row) => (issuesCount += row.diff > 0 ? 1 : 0));
    const issuesMsg = issuesCount === 0 ? "NO_FOUND_ISSUES" : "FOUND_ISSUES";
    await updateRunLog(id, issuesMsg, issuesCount);

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

    const logId = await generateNextLogIdLabel(dataJson?.user, dataJson?.rule);
    const idLabel = dataJson?.rule + "-" + logId.toString().padStart(4, "0");
    await addRunLog({
      id,
      idLabel,
      createdAt: bigquery.timestamp(new Date()).value,
      user: dataJson?.user,
      rule: dataJson?.rule,
      botId: dataJson?.id,
      status: "RUNNING",
      error: "",
    });

    await runProcess(id, dataJson?.id);
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
//         user: "clzotyc8e0010l7wtqbtgz168",
//         rule: "GUP",
//         botId: "9988a522-49a6-437a-97a4-637e7ba37682",
//         status: "RUNNING",
//         error: "",
//       });

//       await runProcess(id, "9988a522-49a6-437a-97a4-637e7ba37682");
//       res.status(200).json(data);
//     } catch (error) {
//       res.send(`ERROR ${error}`);
//     }
//   } else {
//     res.status(404).json({});
//   }
// });
