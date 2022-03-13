const express = require('express');
const app = express();
const http = require("http");
const server = http.createServer(app);

const swaggerJsdoc = require('swagger-jsdoc');
const openapiSpecificationOptions = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'API',
      version: '1.0.0',
    },
  },
  apis: ['./*.js']
};
const swaggerUi = require('swagger-ui-express');
const swaggerDocument = swaggerJsdoc(openapiSpecificationOptions);

const AWS = require('aws-sdk');

const Logger = require('logplease');

Logger.setLogLevel(Logger.LogLevels.DEBUG)

const logger = Logger.create('logger', {
  useColors: true,
  color: Logger.Colors.White,
  showTimestamp: true,
  useLocalTime: false,
  showLevel: true,
  //filename: "application.log",
  appendFile: true
});

app.set("port", process.env.PORT || 3000);

app.use(express.json({ limit: process.env.JSON_BODY_LIMIT || '20mb' }));
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument));

let credentials = new AWS.Credentials({ accessKeyId: process.env.AWS_ACCESS_KEY_ID || "local", secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "local" });

AWS.config.credentials = credentials;

AWS.config.logger = logger;

AWS.config.update({ region: process.env.AWS_DEFAULT_REGION || 'eu-west-1' });

const endpoint = new AWS.Endpoint(process.env.AWS_ENDPOINT || "http://localhost:4566");

const dynamoDB = new AWS.DynamoDB({ endpoint });

app.get('/', async (req, res) => {

  res.redirect("/api-docs");
});

/**
 * @openapi
 * /upload-json:
 *   post:
 *     description: Load data to your table of DynamoDB!
 *     requestBody: { content : { "application/json" : { schema: { type: "object", properties: { content: { type: "object", properties : { Items : { type : "array", items: { type : "object", properties : { "AttributeName" : { type : "object" , properties : { "AttributeDataType" } } } } }, Count : { type : "integer" }, ScannedCount : { type : "integer" }, ConsumedCapacity : { type : "object" } } }, table_name : { type: "string" }, partition_key_name : { type: "string" } } } } } }
 *     responses:
 *       200:
 *         description: Returns response used array in DynamoDB's query
 *         content: { "application/json" : { schema : { type : "array", items : { type : "object", properties : { RequestItems : { type : "object", properties : { "TABLE_NAME" : { type : "array", items : { type : "object", properties : { "PutRequest" : { type : "object", properties : { "Item" : { type : "object", properties : { "AttributeName" : { type : "object", properties : { "AttributeDataType" } } } } } } } } } } }  } } } } }
 */
app.post('/upload-json', async (req, res) => {
  let { content, table_name, partition_key_name } = req.body;

  let inputsValidation = isValidInput(content, table_name, partition_key_name);

  if (inputsValidation != null) {
    return res.json(inputsValidation).status(400);
  }

  let batchRecordsAllowed = 25;

  let promisesGroupsAllowed = 4;

  let temporalItems = [];

  let promises = [];

  let responseAPI = [];

  let batchEjecutionsNumber = 1;

  let firstItem = true;

  let totalRecords = content.Items.length;

  let totalBatchGroups = Math.ceil(totalRecords / batchRecordsAllowed);

  let totalBatchGroupsCalls = Math.ceil(totalBatchGroups / promisesGroupsAllowed);

  logger.info(`Estimated records to save: ${totalRecords}`)

  logger.info(`Estimated batch groups of ${batchRecordsAllowed} records to run: ${totalBatchGroups}`)

  logger.info(`Estimated DynamoDB's calls of ${promisesGroupsAllowed} groups to run: ${totalBatchGroupsCalls}`)

  for await (let item of content.Items) {

    if (firstItem) {
      firstItem = false;
      continue
    }

    temporalItems.push(item);

    if (temporalItems.length % batchRecordsAllowed === 0) {

      logger.info(`Batch ejecution number: ${batchEjecutionsNumber}`);

      let promise = getBatchWriteItemPromise(partition_key_name, table_name, temporalItems);

      promises.push(promise);

      if (promises.length % promisesGroupsAllowed === 0) {

        logger.info(`Batch ejecution number: ${batchEjecutionsNumber}, waiting batchWriteItem process...`);

        await Promise.all(promises);

        promises = [];
      }

      temporalItems = [];

      batchEjecutionsNumber++;
    }

  }

  if (temporalItems.length > 0) {
    logger.info(`Batch final ejecution number: ${batchEjecutionsNumber}`);

    promises.push(getBatchWriteItemPromise(partition_key_name, table_name, temporalItems));
  }

  if (promises.length > 0) {
    logger.info(`Batch final ejecution number: ${batchEjecutionsNumber}, waiting batchWriteItem process...`);

    await Promise.all(promises);

  }

  logger.info("¡fin!")

  res.json(responseAPI).status(200);
});

let isValidInput = (content, table_name, partition_key_name) => {
  if (content == null) {
    return { "message": "content field can't be null." };
  }

  if (table_name == null) {
    return { "message": "table_name field can't be null." };
  }

  if (partition_key_name == null) {
    return { "message": "partition_key_name field can't be null." };
  }

  if (content.Items == null) {
    return { "message": "content.Items field can't be null." };
  }

  return null;
}

let getBatchWriteItemPromise = async (partition_key_name, table_name, items) => {

  let putRequests = items.map((item) => {
    return {
      "PutRequest": {
        "Item": {
          [partition_key_name]: item[partition_key_name],
          ...item
        }
      }
    };
  });

  let params = {
    "RequestItems": { [table_name]: putRequests }
  };

  await dynamoDB.batchWriteItem(params).promise();
};

server.listen(app.get("port"), () => {
  logger.info(`Server running on port ${app.get("port")}`);
});
