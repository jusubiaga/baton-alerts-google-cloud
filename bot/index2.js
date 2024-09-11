const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const { FunctionServiceClient } = require("@google-cloud/functions").v2;
const { CloudSchedulerClient } = require("@google-cloud/scheduler").v1;
const { adapt, managedwriter } = require("@google-cloud/bigquery-storage");
const { WriterClient, JSONWriter } = managedwriter;

const cors = require("cors");

const PROJECTID = "cald-ads-qa";
const LOCATION = "us-central1";
const DATA_SOURCE = "Alerts";
const TABLE = "bots";

const schedulerClient = new CloudSchedulerClient();
const functionsClient = new FunctionServiceClient();
const bigquery = new BigQuery({ projectId: PROJECTID });

const add = async (data) => {
  const destinationTable = `projects/${PROJECTID}/datasets/${DATA_SOURCE}/tables/${TABLE}`;
  const writeClient = new WriterClient({ PROJECTID });

  try {
    const writeStream = await writeClient.getWriteStream({
      streamId: `${destinationTable}/streams/_default`,
      view: "FULL",
    });
    const protoDescriptor = adapt.convertStorageSchemaToProto2Descriptor(writeStream.tableSchema, "root");

    const connection = await writeClient.createStreamConnection({
      streamId: managedwriter.DefaultStream,
      destinationTable,
    });
    const streamId = connection.getStreamId();

    const writer = new JSONWriter({
      streamId,
      connection,
      protoDescriptor,
    });

    let rows = [];
    const pendingWrites = [];

    rows.push(data);

    // Send batch.
    let pw = writer.appendRows(rows);
    pendingWrites.push(pw);

    const results = await Promise.all(pendingWrites.map((pw) => pw.getResult()));
    console.log("Write results:", results);
  } catch (err) {
    console.log(err);
  } finally {
    writeClient.close();
  }

  // await bigquery.dataset(DATA_SOURCE).table(TABLE).insert(data);
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

const POST = async (req, res) => {};

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
  const job = schedulerClient.jobPath(PROJECTID, LOCATION, jobId);
  const request = {
    name: job,
  };

  const response = await schedulerClient.runJob(request);

  return response;
};

functions.http("addBot", async (req, res) => {
  // corsHandler(async (req, res) => {
  res.header("Access-Control-Allow-Origin", "http://localhost:3000");
  res.header(
    "Access-Control-Allow-Headers",
    "Authorization, X-API-KEY, Origin, X-Requested-With, Content-Type, Accept, Access-Control-Allow-Request-Method"
  );
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE");
  res.header("Allow", "GET, POST, OPTIONS, PUT, DELETE");

  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }
  if (req.method === "POST") {
    console.log("Waiting ...");

    try {
      await getFuntion(`projects/${PROJECTID}/locations/${LOCATION}/functions/${req.body.rule}`);

      const jobDataJson = { user: req.body.user };

      const jobName = `${req.body.user}-${req.body.rule}`;
      const job = await createJob({
        name: `projects/${PROJECTID}/locations/${LOCATION}/jobs/${jobName}`,
        description: "Job created",
        schedule: req.body.frequency,
        time_zone: "UTC",
        pubsubTarget: {
          topicName: `projects/${PROJECTID}/topics/bot-execution`,
          data: Buffer.from(JSON.stringify(jobDataJson)),
          attributes: {
            project: PROJECTID,
          },
        },
      });

      const data = { ...req.body, job };
      await add(data);
      res.status(200).json(data);
    } catch (error) {
      res.status(500).json({ error });
    }

    console.log("done !");
    return;
  }

  if (req.method === "PATCH") {
    console.log("BODY: ", req.body);
    console.log("Param: ", req.params);

    const jobID = req.params[0];

    const job = await updateJob(jobID, req.body);
    // const job = await runJob(jobID);
    res.status(200).json({ job });
  } else {
    res.status(404).json({});
  }

  return;
});
