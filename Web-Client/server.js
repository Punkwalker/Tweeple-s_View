const express = require('express');
const bodyParser = require('body-parser');
const request = require('request');
const app = express()
const {PubSub} = require('@google-cloud/pubsub');
const {BigQuery} = require('@google-cloud/bigquery');

const {v1:uuidv1} = require('uuid')

app.use(express.static('public'));
app.use(bodyParser.urlencoded({ extended: true }));
app.set('view engine', 'ejs')

app.get('/', function (req, res) {
  res.render('index2',{error:null,doneBQ:null,positive:null,nuetral:null,negative:null});
})

process.env.GOOGLE_APPLICATION_CREDENTIALS = "tweeple-sentiment-ac73f4e6b7b4.json";

const publisher1 = new PubSub();    //publisher client for get-topic
const subscriber = new PubSub();    //subscriber client  for done-bq
const bigquery = new BigQuery();    //BigQuery client

async function publishMessage(topic,requestId) {
  
  const dataBuffer = Buffer.from(topic);

  const attributes = {
    id: requestId
  };

  try {
    const messageId = await publisher1.topic('get-topic').publish(dataBuffer,attributes);
    console.log(`Message ${messageId} published.`);
    console.log(`Waiting for BQ insertion!!`);
  } catch (error) {
    console.error(`Received error while publishing: ${error.message}`);
    process.exitCode = 1;
  }
}


//Query for Postive Tweets
async function queryBQ1(requestId) {
  const query1 = `SELECT score
    FROM \`tweeple-1.cloudproject.results\`
    WHERE score>0 AND id = '`+requestId+`'`;


  const options = {
    query: query1,
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

  // Wait for the query to finish
  const [rows] = await job.getQueryResults();
  return new Promise ((resolve, reject) =>{
    

  // Print the results
  
  resolve(rows.length);
  // console.log('Rows:');
  // rows.forEach(row => console.log(row));
  }
  );
}


//Query for Nuetral Tweets
async function queryBQ2(requestId) {
  const query2 = `SELECT score
  FROM \`tweeple-1.cloudproject.results\`
  WHERE score=0 AND id = '`+requestId+`'`;


const options = {
  query: query2,
};

// Run the query as a job
const [job] = await bigquery.createQueryJob(options);
console.log(`Job ${job.id} started.`);

// Wait for the query to finish
const [rows] = await job.getQueryResults();

  return new Promise ((resolve, reject) =>{
  // Print the results
  
  resolve(rows.length);
  // console.log('Rows:');
  // rows.forEach(row => console.log(row));
  }
  );
}


//Query for Negative Tweets
async function queryBQ3(requestId) {
  const query3 = `SELECT score
    FROM \`tweeple-1.cloudproject.results\`
    WHERE score<0 AND id = '`+requestId+`'`;


  const options = {
    query: query3,
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started.`);

  // Wait for the query to finish
  const [rows] = await job.getQueryResults();
  return new Promise ((resolve, reject) =>{
    

  // Print the results
  
  resolve(rows.length);
  // console.log('Rows:');
  // rows.forEach(row => console.log(row));
  }
  );
}

function listenForMessages(requestId) {
  
  return new Promise ((resolve, reject) =>{
    const subscription = subscriber.subscription('done-bq-sub');

  // Create an event handler to handle messages
 
  const messageHandler = message => {
    console.log(`Received message ${message.id}:`);
    console.log(`\tData: ${message.data}`); 
    message.ack();
    resolve(message.data);
  };

  // Listen for new messages until timeout is hit
   subscription.on('message', messageHandler);
  }
  );
}


app.post('/',  async function (req, res) {
  let topic = req.body.topic;
  let requestId = uuidv1();
  let doneBQ = null;
  publishMessage(topic,requestId);
  // res.render('index2',{setTopic:1,error:null,doneBQ:null,positive:null,nuetral:null,negative:null});
  await listenForMessages(requestId);
  console.log(`Applying query on BQ Data`);
  const positive = await queryBQ1(requestId);
  console.log(positive);
  const nuetral = await queryBQ2(requestId);
  console.log(nuetral);
  const negative = await queryBQ3(requestId);
  console.log(negative);
  console.log(`Done Querying BQ Data`);
  res.render('index2',{error:null,doneBQ:1,positive:positive,nuetral:nuetral,negative:negative});
  res.end();
})

app.listen(3000, function () {
  console.log('Tweeple\'s View app listening on port 3000!')
})
