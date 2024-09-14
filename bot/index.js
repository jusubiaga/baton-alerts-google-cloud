const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const { FunctionServiceClient } = require("@google-cloud/functions").v2;
const { CloudSchedulerClient } = require("@google-cloud/scheduler").v1;
const { adapt, managedwriter } = require("@google-cloud/bigquery-storage");
const { WriterClient, JSONWriter } = managedwriter;
const uuid = require("uuid");
const express = require("express");
const cors = require("cors");
const { v4 } = require("uuid");

const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());

// CONFIG
const PROJECTID = "cald-ads-qa";
const LOCATION = "us-central1";
const DATA_SOURCE = "Alerts";
const TABLE = "bots";
const TABLE_RULES = "rules";
const TOPIC = "bot-execution";

const bigquery = new BigQuery({ projectId: PROJECTID });
const schedulerClient = new CloudSchedulerClient();
const functionsClient = new FunctionServiceClient();

// FUNCTIONS
const getData = async () => {
  const tableBot = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;
  const tableRules = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RULES}`;

  const query = `
SELECT bots.id as id, rule, frequency, user, name, description, avatar, job 
FROM ${tableBot} as bots
LEFT JOIN ${tableRules} as rules ON
bots.rule = rules.id`;

  // SELECT *
  //  FROM ${table}`;

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

const getDataById = async (id) => {
  const tableBot = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;
  const tableRules = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RULES}`;

  const query = `
SELECT bots.id as id, rule, frequency, user, name, description, avatar, job 
FROM ${tableBot} as bots
LEFT JOIN ${tableRules} as rules ON
bots.rule = rules.id
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

const add = async (data) => {
  const query = `
  INSERT INTO  ${PROJECTID}.${DATA_SOURCE}.${TABLE}
  (id, createdAt, user, rule, frequency, job)
  VALUES ("${data.id}",
      "${data.createdAt}",
      "${data.user}",
      "${data.rule}",
      "${data.frequency}",
      "${data.job}")`;

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

const getFuntion = async (name) => {
  console.log("getFuntion ...");
  const request = {
    name,
  };
  const response = await functionsClient.getFunction(request);
  console.log(response?.name);
};

const createJob = async (jobName, frequency, jobDataJson) => {
  if (!jobName || !frequency || !jobDataJson) {
    return "";
  }

  console.log("createJob", jobName, frequency, jobDataJson);
  // Construct request
  const job = {
    name: `projects/${PROJECTID}/locations/${LOCATION}/jobs/${jobName}`,
    description: "Job created",
    schedule: frequency,
    time_zone: "UTC",
    pubsubTarget: {
      topicName: `projects/${PROJECTID}/topics/${TOPIC}`,
      data: Buffer.from(JSON.stringify(jobDataJson)),
      attributes: {
        project: PROJECTID,
      },
    },
  };

  console.log("createJob ...");
  const request = {
    parent: `projects/${PROJECTID}/locations/${LOCATION}`,
    job,
  };

  // Run request
  const response = await schedulerClient.createJob(request);
  console.log(response[0].name);
  return response[0].name;
};

const deleteBot = async (id) => {
  const tableBot = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;
  const bot = await getDataById(id);
  if (!bot) {
    return null;
  }
  // delete the schedurer

  if (bot.job) {
    console.log("deleteBot:  delete the schedurer");
    // Construct request
    const request = {
      name: bot.job,
    };

    // Run request
    try {
      await schedulerClient.deleteJob(request);
    } catch (error) {
      console.warn(error);
    }
  }

  // delete bot
  const query = `
DELETE ${tableBot}
WHERE id = "${id}"`;

  console.log(query);

  const options = {
    query: query,
    location: "US",
  };

  // create query
  console.log("deleteBot:  delete bot");
  const [job] = await bigquery.createQueryJob(options);
  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  return rows[0] ?? null;
};

const updateData = async (id, data) => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;

  console.log("updateData: ", data);
  const query = `UPDATE ${table} SET 
   job = "${data?.job}",
   frequency = "${data?.frequency}"
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

const updateBot = async (id, data) => {
  const bot = await getDataById(id);

  if (!bot) {
    return null;
  }

  let newJobName = "";
  const { frequency } = data;

  console.log("updateBot: ", bot);

  if (!bot?.job) {
    console.log("updateBot: creating job");
    const jobDataJson = { id: bot.id, user: bot.user, rule: bot.rule };
    const jobName = `${bot.user}-${bot.rule}`;
    const { frequency } = data;
    newJobName = await createJob(jobName, frequency, jobDataJson);
    // await updateData(id, { job, frequency: schedule });

    // return job;
  } else {
    console.log("updateBot: updating job");

    //schedulerClient.jobPath(PROJECTID, LOCATION, bot?.job);

    const request = {
      job: {
        name: bot?.job,
        schedule: frequency,
      },
      updateMask: {
        paths: ["schedule"],
      },
    };
    const [response] = await schedulerClient.updateJob(request);
    console.log(`Updated job: ${response.name}`);
    // return response;
  }

  const rep = await updateData(id, { job: !bot?.job ? newJobName : bot?.job, frequency });
  return rep;
};

const enabledJob = async (jobId, enabled) => {
  // Construct request
  const job = schedulerClient.jobPath(PROJECTID, LOCATION, jobId);
  const request = {
    name: job,
  };

  // Run request
  let response;

  if (!enabled) {
    response = await schedulerClient.pauseJob(request);
  } else {
    response = await schedulerClient.resumeJob(request);
  }

  return response;
};

const runBot = async (id) => {
  const bot = await getDataById(id);
  if (!bot) {
    return null;
  }

  // Construct request
  let response = {
    code: 0,
    error: "",
    data: {},
  };

  try {
    // const job = schedulerClient.jobPath(PROJECTID, LOCATION, jobId);
    const { job } = bot;
    const request = {
      name: job,
    };

    console.log("RUN: ", job);

    return { data: await schedulerClient.runJob(request) };
  } catch (error) {
    console.log("RUN ERROR: ", job);
    return { ...response, error: error.details, code: error.code };
  }
};

// ROUTES
app.get("/", async function (req, res) {
  try {
    const data = await getData();
    res.status(200).json(data);
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
});

app.get("/:id", async function (req, res) {
  console.log("Param: ", req.params);

  const id = req.params.id;

  try {
    const data = await getDataById(id);
    res.status(200).json(data);
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
});

app.post("/", async function (req, res) {
  const { rule, user, frequency } = req.body;

  console.log("Waiting ...");

  try {
    // await getFuntion(`projects/${PROJECTID}/locations/${LOCATION}/functions/${rule}`);

    const id = v4();

    const jobDataJson = { id, user, rule };

    const jobName = `${user}-${rule}`;
    const job = await createJob(jobName, frequency, jobDataJson);

    const createdAt = bigquery.timestamp(new Date()).value;
    const data = { id, createdAt, ...req.body, job };
    await add(data);

    res.status(200).json(data);
  } catch (error) {
    res.status(404).json({ error });
  }

  console.log("done !");
});

app.patch("/:id", async function (req, res) {
  console.log("body: ", req.body);
  console.log("Param: ", req.params);

  const id = req.params.id;

  const bot = await updateBot(id, req.body);
  res.status(200).json({ bot });
});

app.delete("/:id", async function (req, res) {
  const { id } = req.params;
  const { body } = req;

  const response = await deleteBot(id);
  res.status(200).json(response);
});

app.post("/run/:id", async function (req, res) {
  console.log("BODY: ", req.body);
  console.log("Param: ", req.params);

  const { id } = req.params;

  const response = await runBot(id);
  console.log(response);
  if (response) {
    res.status(200).json(response?.data[0]);
  } else {
    res.status(404).json({});
  }
});

functions.http("bot", app);
