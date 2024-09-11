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
const TOPIC = "bot-execution";

const bigquery = new BigQuery({ projectId: PROJECTID });
const schedulerClient = new CloudSchedulerClient();
const functionsClient = new FunctionServiceClient();

// FUNCTIONS
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

  // const destinationTable = `projects/${PROJECTID}/datasets/${DATA_SOURCE}/tables/${TABLE}`;
  // const writeClient = new WriterClient({ PROJECTID });

  // try {
  //   const writeStream = await writeClient.getWriteStream({
  //     streamId: `${destinationTable}/streams/_default`,
  //     view: "FULL",
  //   });
  //   const protoDescriptor = adapt.convertStorageSchemaToProto2Descriptor(writeStream.tableSchema, "root");

  //   const connection = await writeClient.createStreamConnection({
  //     streamId: managedwriter.DefaultStream,
  //     destinationTable,
  //   });
  //   const streamId = connection.getStreamId();

  //   const writer = new JSONWriter({
  //     streamId,
  //     connection,
  //     protoDescriptor,
  //   });

  //   let rows = [];
  //   const pendingWrites = [];

  //   rows.push(data);

  //   // Send batch.
  //   let pw = writer.appendRows(rows);
  //   pendingWrites.push(pw);

  //   const results = await Promise.all(pendingWrites.map((pw) => pw.getResult()));
  //   console.log("Write results:", results);
  // } catch (err) {
  //   console.log(err);
  // } finally {
  //   writeClient.close();
  // }
};

const getFuntion = async (name) => {
  console.log("getFuntion ...");
  const request = {
    name,
  };
  const response = await functionsClient.getFunction(request);
  console.log(response?.name);
};

const createJob = async (job) => {
  // Construct request
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

const updateJob = async (jobId, data) => {
  const job = schedulerClient.jobPath(PROJECTID, LOCATION, jobId);

  const { schedule } = data;

  const request = {
    job: {
      name: job,
      schedule,
    },
    updateMask: {
      paths: ["schedule"],
    },
  };
  const [response] = await schedulerClient.updateJob(request);
  console.log(`Updated job: ${response.name}`);
  return response;
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

const runJob = async (jobId) => {
  // Construct request
  let response = {
    code: 0,
    error: "",
    data: {},
  };

  try {
    const job = schedulerClient.jobPath(PROJECTID, LOCATION, jobId);
    const request = {
      name: job,
    };

    console.log("RUN: ", jobId);

    return { data: await schedulerClient.runJob(request) };
  } catch (error) {
    console.log("RUN ERROR: ", jobId);
    return { ...response, error: error.details, code: error.code };
  }
};

// ROUTES
app.get("/", async function (req, res) {
  try {
    res.status(200).json(data);
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
});

app.get("/:id", async function (req, res) {
  try {
    res.status(200).json(data);
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
});

app.post("/", async function (req, res) {
  const { rule, user, frequency } = req.body;

  console.log("Waiting ...");

  try {
    await getFuntion(`projects/${PROJECTID}/locations/${LOCATION}/functions/${rule}`);

    const id = v4();
    const jobDataJson = { id, user };

    const jobName = `${user}-${rule}`;
    const job = await createJob({
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
    });

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

  const jobID = req.params.id;

  const job = await updateJob(jobID, req.body);
  res.status(200).json({ job });
});

app.delete("/:id", async function (req, res) {
  const { id } = req.params;
  const { body } = req;

  console.log(body);
  console.log(id);
});

app.post("/run/:id", async function (req, res) {
  console.log("BODY: ", req.body);
  console.log("Param: ", req.params);

  const { id } = req.params;

  const response = await runJob(id);
  console.log(response);
  if (response.code === 0) {
    res.status(200).json({ response });
  } else {
    res.status(404).json({ response });
  }
});

functions.http("bot", app);
