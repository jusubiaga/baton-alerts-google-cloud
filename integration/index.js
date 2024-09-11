const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const uuid = require("uuid");
const express = require("express");
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PROJECTID = "cald-ads-qa";
const DATA_SOURCE = "Alerts";
const TABLE_INTEGRATION = "integration";
const TABLE_INTEGRATION_TYPE = "integrationType";

const bigquery = new BigQuery({ projectId: PROJECTID });

const getDataById = async (id) => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE_INTEGRATION}`;

  const query = `SELECT *
   FROM ${table}
   WHERE id = "${id}"`;

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

const existsRecord = async (user, integrationType) => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE_INTEGRATION}`;

  const query = `SELECT *
   FROM ${table}
   WHERE user = "${user}" AND integrationType = "${integrationType}"`;

  console.log(query);

  const options = {
    query: query,
    location: "US",
  };

  // create query
  const [job] = await bigquery.createQueryJob(options);
  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  console.log("existsRecord: ", rows);
  return rows.length === 0 ? false : true;
};

const updateData = async (id, data) => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE_INTEGRATION}`;

  const query = `UPDATE ${table} SET 
   campaignPrefix = "${data?.campaignPrefix}",
   clientSecret= "${data?.clientSecret}",
   clientId = "${data?.clientId}"
  WHERE id = "${id}"`;

  console.log(query);

  const options = {
    query: query,
    location: "US",
  };

  // create query
  const [job] = await bigquery.createQueryJob(options);
  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  return data;
};

const getData = async (user = "", integrationType = "") => {
  const tableIntegration = `${PROJECTID}.${DATA_SOURCE}.${TABLE_INTEGRATION}`;
  const tableIntegrationType = `${PROJECTID}.${DATA_SOURCE}.${TABLE_INTEGRATION_TYPE}`;

  let filter = [];

  if (user) {
    filter.push(`user = "${user}"`);
  }

  if (integrationType) {
    filter.push(`integrationType = "${integrationType}"`);
  }

  let whereFilter = "";
  filter.forEach((item, index) => {
    if (index === 0) {
      whereFilter = " WHERE ";
      whereFilter += item;
    } else {
      whereFilter += " AND ";
      whereFilter += item;
    }
  });

  // const query = `SELECT *
  //  FROM ${table}
  //  ${whereFilter}`;

  const query = `
SELECT
  t1.id,
  t1.user,
  t2.id as integrationType,
  t1.clientId,
  t1.clientSecret,
  t1.campaignPrefix,
  t1.createdAt,
  IFNULL(t1.status, "NOT_CONFIGURE") as status,
  t2.name,
  t2.description,
  t2.logo
FROM
  ${tableIntegration} t1
RIGHT  JOIN
  ${tableIntegrationType} t2
ON
  t1.integrationType = t2.id
AND
  t1.user = "${user}"  
  `;

  console.log(query);

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

app.post("/", async function (req, res) {
  try {
    const { body } = req;

    console.log(body);

    if (!!body.user === false || !!body.integrationType === false) {
      return res.status(400).json({
        status: false,
        error: "bad requests",
      });
    }

    if (await existsRecord(body.user, body.integrationType)) {
      return res.status(409).json({
        status: false,
        error: "Conflict",
      });
    }

    let status = "NOT_CONFIGURE";
    if (!!body.clientId && !!body.clientSecret) {
      status = "CONFIGURED";
    }

    const data = {
      id: uuid.v4(),
      user: body?.user,
      integrationType: body?.integrationType,
      clientId: body?.clientId ?? "",
      clientSecret: body?.clientSecret ?? "",
      campaignPrefix: body?.campaignPrefix ?? "",
      status,
      createAt: bigquery.timestamp(new Date()).value,
    };

    const query = `
    INSERT INTO  ${PROJECTID}.${DATA_SOURCE}.${TABLE_INTEGRATION}
    (
      id, user, integrationType, clientId, clientSecret, campaignPrefix, status, createdAt
    ) VALUES (
      '${uuid.v4()}',
      '${data.user}',
      '${data.integrationType}',
      '${data.clientId}',
      '${data.clientSecret}',
      '${data.campaignPrefix}',
      '${data.status}',
      '${data.createAt}'
    )`;

    console.log(query);
    const options = {
      query: query,
      location: "US",
    };

    // create query
    const [job] = await bigquery.createQueryJob(options);
    console.log(`Insert data`);

    // Wait for the query to finish
    await job.getQueryResults();

    res.status(200).json({});
    // const data = {
    //   id: uuid.v4(),
    //   user: body?.user,
    //   integrationType: body?.integrationType,
    //   clientId: body?.clientId ?? "",
    //   clientSecret: body?.clientSecret ?? "",
    //   campaignPrefix: body?.campaignPrefix ?? "",
    //   status,
    // };

    // console.log(data);
    // await bigquery.dataset(DATA_SOURCE).table(TABLE).insert(data);

    // res.status(200).json(data);
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
  // return res.sendStatus(200);
});

app.patch("/:id", async function (req, res) {
  // try {
  const { id } = req.params;
  const { body } = req;

  console.log(body);
  console.log(id);

  let status = "NOT_CONFIGURE";
  if (!!body?.clientId && !!body?.clientSecret) {
    status = "CONFIGURED";
  }

  const data = await getDataById(id);
  if (data) {
    console.log(data);
    const uData = await updateData(id, body);
    console.log(uData);
    res.status(200).json(uData);
  } else {
    res.sendStatus(404);
  }
  //   const data = {
  //     id: uuid.v4(),
  //     user: body?.user,
  //     integrationType: body?.integrationType,
  //     clientId: body?.clientId ?? "",
  //     clientSecret: body?.clientSecret ?? "",
  //     campaignPrefix: body?.campaignPrefix ?? "",
  //     status,
  //   };

  //   console.log(data);
  //   await bigquery.dataset(DATA_SOURCE).table(TABLE).insert(data);

  //   res.status(200).json(data);
  // } catch (error) {
  //   res.send(`ERROR ${error}`);
  // }
  // return res.sendStatus(200);
});

functions.http("integration", app);

// async (req, res) => {
//   if (req.method === "GET") {
//     console.log("getting data ...");

//     try {
//       const data = await getData();

//       res.status(200).json(data);
//     } catch (error) {
//       res.send(`ERROR ${error}`);
//     }
//   } else {
//     res.status(404).json({});
//   }
// }
