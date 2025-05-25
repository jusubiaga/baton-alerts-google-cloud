const functions = require("@google-cloud/functions-framework");
const { BigQuery } = require("@google-cloud/bigquery");
const uuid = require("uuid");
const express = require("express");
const app = express();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PROJECTID = "cald-ads-qa";
const DATA_SOURCE = "Alerts";
const TABLE = "workspace";

const bigquery = new BigQuery({ projectId: PROJECTID });

const getDataById = async (id) => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;

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

const getData = async (user) => {
  const query = `
    SELECT id, name, description, icon user, ARRAY_TO_STRING(workspaceMembers, ',') AS workspaceMembers, createdAt
    FROM \`cald-ads-qa.Alerts.workspace\`
    WHERE '${user}' IN UNNEST(workspaceMembers)
    ORDER BY createdAt DESC`;

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

const deleteWorkspace = async (id, user) => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;

  const query = `DELETE FROM ${table}
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

  return rows;
};

app.get("/", async function (req, res) {
  try {
    const { user } = req.query;
    const data = await getData(user);
    res.status(200).json(data);
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
});

app.post("/", async function (req, res) {
  try {
    const { body } = req;

    console.log(body);

    if (!!body.user === false) {
      return res.status(400).json({
        status: false,
        error: "bad requests",
      });
    }

    console.log("Workspace Create...", body?.workspaceMembers ?? []);
    const data = {
      id: uuid.v4(),
      name: body.name,
      description: body?.description ?? "",
      icon: body?.icon ?? "",
      user: body.user,
      workspaceMembers: body?.workspaceMembers ?? "".replace(/\s+/g, "").split(","),
      createAt: bigquery.timestamp(new Date()).value,
    };

    console.log("DATA:", data);
    const query = `
  INSERT INTO  ${PROJECTID}.${DATA_SOURCE}.${TABLE}
  (id, name, description, icon, user, workspaceMembers)
  VALUES ("${data.id}",
      "${data.name}",
      "${data.description}",
      "${data.icon}",
      "${data.user}",
       [${data.workspaceMembers.map((member) => `"${member}"`).join(", ")}])`;

    const options = {
      query: query,
      location: "US",
    };

    // create query
    const [job] = await bigquery.createQueryJob(options);

    // Wait for the query to finish
    await job.getQueryResults();

    res.status(201).json(); // Devolver los datos insertados
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
});

const updateData = async (id, data) => {
  const table = `${PROJECTID}.${DATA_SOURCE}.${TABLE}`;

  console.log("updateData: ", data);
  const workspaceMembers = data?.workspaceMembers.split(",") ?? [];
  const query = `UPDATE ${table} SET 
    name = "${data?.name}",
    description = "${data?.description ?? ""}",
    icon = "${data?.icon ?? ""}",
    workspaceMembers = [${workspaceMembers.map((member) => `"${member}"`).join(", ")}]
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

app.put("/:id", async function (req, res) {
  try {
    const { id } = req.params;
    const { body } = req;
    console.log(body);
    console.log(id);
    const exists = await getDataById(id);
    if (exists) {
      console.log(exists);
      const uData = await updateData(id, body);
      console.log(uData);
      res.status(200).json(uData);
    } else {
      res.sendStatus(404);
    }
  } catch (error) {
    res.send(`ERROR ${error}`);
  }
});

app.delete("/:id", async function (req, res) {
  const { id } = req.params;
  const response = await deleteWorkspace(id);
  res.status(200).json(response);
});

functions.http("workspace", app);

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
