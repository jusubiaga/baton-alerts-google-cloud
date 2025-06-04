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
SELECT bots.id as id, rule, frequency, user, name, description, avatar, job, minimumNumber, workspace  
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

const getDataByUser = async (user) => {
  const tableBot = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;
  const tableRules = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RULES}`;

  const query = `
    SELECT bots.id as id, rule, frequency, user, name, description, avatar, job, minimumNumber, workspace 
    FROM ${tableBot} as bots
    JOIN ${tableRules} as rules ON
    bots.rule = rules.id AND
    bots.user = "${user}"`;

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

  const result = await Promise.all(
    rows.map(async (row) => {
      const jobData = await getJob(row.job);
      const data = { ...row, ...jobData };
      console.log("DATA: ", data);
      return data;
    })
  );

  return result;
};

getDataByWorkspace = async (workspace) => {
  const tableBot = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;
  const tableRules = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RULES}`;

  const query = `
    SELECT bots.id as id, rule, frequency, user, name, description, avatar, job, minimumNumber, workspace 
    FROM ${tableBot} as bots
    JOIN ${tableRules} as rules ON
    bots.rule = rules.id AND
    bots.workspace = "${workspace}"`;

  console.log(query);

  const options = {
    query: query,
    location: "US",
  };

  // create query
  const [job] = await bigquery.createQueryJob(options);
  // Wait for the query to finish
  const [rows] = await job.getQueryResults();

  const result = await Promise.all(
    rows.map(async (row) => {
      const jobData = await getJob(row.job);
      const data = { ...row, ...jobData };
      console.log("DATA: ", data);
      return data;
    })
  );

  return result;
};

const getDataById = async (id) => {
  const tableBot = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;
  const tableRules = `${PROJECTID}.${DATA_SOURCE}.${TABLE_RULES}`;

  const query = `
    SELECT bots.id as id, rule, frequency, user, name, description, avatar, job, minimumNumber, workspace  
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

const addBot = async (data) => {
  console.log("DATA:", data);
  const query = `
  INSERT INTO  ${PROJECTID}.${DATA_SOURCE}.${TABLE}
  (id, createdAt, user, workspace, rule, frequency, job)
  VALUES ("${data.id}",
      "${data.createdAt}",
      "${data.user}",
      "${data.workspace}",
      "${data.rule}",
      "${data?.schedule ?? ""}",
      "${data?.job ?? ""}")`;

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
  const [response] = await schedulerClient.createJob(request);
  return {
    job: response.name,
    nextRunDate: getScheduleTime(response.scheduleTime),
    lastRunDate: getScheduleTime(response.lastAttemptTime),
    state: response.state,
    schedule: response.schedule,
  };
};

const getScheduleTime = (scheduleTime) => {
  if (scheduleTime) {
    const { seconds, nanos } = scheduleTime;
    const timestamp = parseInt(seconds) * 1000 + Math.floor(nanos / 1000000);
    const date = new Date(timestamp);
    return date.toISOString();
  }
  return "";
};

const getJob = async (jobId) => {
  let jobData = { nextRunDate: null, lastRunDate: null, state: null, schedule: null };
  if (jobId) {
    // Construct request
    const request = {
      name: jobId,
    };

    // Run request
    try {
      const job = await schedulerClient.getJob(request);
      jobData = {
        ...jobData,
        nextRunDate: getScheduleTime(job[0].scheduleTime),
        lastRunDate: getScheduleTime(job[0].lastAttemptTime),
        state: job[0].state,
        schedule: job[0].schedule,
      };
    } catch (error) {
      console.warn(error);
    }
  }

  return jobData;
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
    frequency = "${data?.schedule}",
    minimumNumber = ${data?.minimumNumber}
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

  const { frequency, minimumNumber } = data;
  let dataJob = {};

  // create new job
  if (!bot?.job) {
    console.log("updateBot: creating job");
    const jobDataJson = { id: bot.id, user: bot.user, workspace: bot.workspace, rule: bot.rule };
    const jobName = `${bot.workspace}-${bot.user}-${bot.rule}`;
    dataJob = await createJob(jobName, frequency, jobDataJson);
  } else {
    // update job
    console.log("updateBot: updating job");
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
    dataJob = {
      job: response.name,
      nextRunDate: getScheduleTime(response.scheduleTime),
      lastRunDate: getScheduleTime(response.lastAttemptTime),
      state: response.state,
      schedule: response.schedule,
    };

    console.log("Updated job: ", dataJob);
    // return response;
  }

  // const rep = await updateData(id, { job: !bot?.job ? newJobName : bot?.job, frequency });
  const rep = await updateData(id, { ...dataJob, minimumNumber });
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
  const { user } = req.query;
  const { workspace } = req.query;

  let data;
  try {
    if (user) {
      data = await getDataByUser(user);
    }
    if (workspace) {
      data = await getDataByWorkspace(workspace);
    } else {
      data = await getData();
    }

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
  const { rule, user, workspace, frequency } = req.body;

  console.log("Waiting ...");

  try {
    const id = v4();

    const jobDataJson = { id, user, workspace, rule };

    const jobName = `${workspace}-${user}-${rule}`;
    const job = await createJob(jobName, frequency, jobDataJson);

    const createdAt = bigquery.timestamp(new Date()).value;
    const data = { id, createdAt, ...req.body, ...job };
    await addBot(data);

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
