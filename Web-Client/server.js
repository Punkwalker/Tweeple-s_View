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
  res.render('index2',{error:null,doneBQ:null,positive:null,nuetral:null,negative:null,topic:null});
  res.end();
})

process.env.GOOGLE_APPLICATION_CREDENTIALS = "tweeple-sentiment-ac73f4e6b7b4.json";

async function publishMessage(topic,requestId) {
  const publisher1 = new PubSub();    //publisher client for get-topic
  const dataBuffer = Buffer.from(topic);

  const attributes = {
      id: requestId
    };
  const messageId = await publisher1.topic('get-topic').publish(dataBuffer,attributes);
  return new Promise ((resolve, reject) =>{
    
    // try {
      
    resolve(`Message ${messageId} published.`);
    // } catch (error) {
    //   resolve(`Received error while publishing: ${error.message}`);
    //   process.exitCode = 1;
    // }
  }
  );
  // const dataBuffer = Buffer.from(topic);

  // const attributes = {
  //   id: requestId
  // };

  // try {
  //   const messageId = await publisher1.topic('get-topic').publish(dataBuffer,attributes);
  //   console.log(`Message ${messageId} published.`);
  // } catch (error) {
  //   console.error(`Received error while publishing: ${error.message}`);
  //   process.exitCode = 1;
  // }
}


//Query for Postive Tweets
async function queryBQ1(requestId) {
  const bigquery = new BigQuery();    //BigQuery client
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
  const bigquery = new BigQuery();    //BigQuery client
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
  const bigquery = new BigQuery();    //BigQuery client
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
  const subscriber = new PubSub();    //subscriber client  for done-bq
  return new Promise ((resolve, reject) =>{
    const subscription = subscriber.subscription('done-bq-sub');

  // Create an event handler to handle messages
 
  const messageHandler = message => {
    console.log(`Received message ${message.id}:`);
    console.log(`Recieved Data: ${message.data}`);
    message.ack();
    subscription.removeListener('message', messageHandler);
    resolve();
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
  let publish = await publishMessage(topic,requestId);
  console.log(publish);
  console.log(`Waiting for BQ insertion!!`);
  await listenForMessages(requestId);
  console.log(`Applying query on BQ Data`);
  const positive = await queryBQ1(requestId);
  console.log(`Waiting for 1st Query to Finish`);
  console.log(positive);
  const nuetral = await queryBQ2(requestId);
  console.log(`Waiting for 2nd Query to Finish`);
  console.log(nuetral);
  const negative = await queryBQ3(requestId);
  console.log(`Waiting for 3rd Query to Finish`);
  console.log(negative);
  console.log(`Done Querying BQ Data`);
  res.render('index2',{error:null,doneBQ:1,positive:positive,nuetral:nuetral,negative:negative,topic:topic});
  res.end();
})

app.listen(3000, function () {
  console.log('Tweeple\'s View app listening on port 3000!')
})
