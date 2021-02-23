/**
* @param {Object} event - API Gateway Lambda Proxy Input Format
 * @param {Object} context
 * @returns {Object} object - API Gateway Lambda Proxy Output Format
 */

 
const aws = require('aws-sdk');
const ddb = new aws.DynamoDB.DocumentClient({maxRetries: 5, retryDelayOptions: {base: 300} });
const sqs = new aws.SQS({apiVersion: '2012-11-05'});
const iplocate = require("node-iplocate");
const apiKey = process.env.IP_LOCATE_API_KEY;
const linkClickSqsUrl = process.env.SQS_LINK_CLICK_QUEUE_URL;

exports.LinkConsumerHandler = async (event, context, callback) => {
  let linkId = event.pathParameters.proxy;

 let clickAgent = event['requestContext']['identity']['userAgent'];
 let clickIP = event['requestContext']['identity']['sourceIp'];
  
  try {
    let link = get(linkParams(linkId));
    console.log("LINK", JSON.stringify(link));
    return redirectResponse(keyword.Items[0].url);
  } catch(e) {
    console.log(`ERROR: ${e}`);
  };
};

/******************************* IP LOCATE ***********************************/

const getIpLocation = (ip) => {
  return new Promise(function(resolve, reject) {
    //null will be replaced with apiKey when limit exceeded
    iplocate(ip, null, function(err, data) {
          if (err) reject(err);
          else resolve(data);
        })
    });
};

//EXAMPLE RESPONSE
// {
//   "ip": "173.73.6.33",
//   "country": "United States",
//   "country_code": "US",
//   "city": "Manassas",
//   "continent": "North America",
//   "latitude": 38.6707,
//   "longitude": -77.4274,
//   "time_zone": "America/New_York",
//   "postal_code": "20112",
//   "org": "UUNET",
//   "asn": "AS701",
//   "subdivision": "Virginia",
//   "subdivision2": null
// }

/******************************* SQS ***********************************/
const sendSqsMessage = (clickParams, sqsUrl) => {
  let { country, country_code, city, subdivision } = clickParams;
  const params = {
   DelaySeconds: 10,
   MessageAttributes: {
     "country": {
       DataType: "String",
       StringValue: country
     },
     "country_code": {
       DataType: "String",
       StringValue: country_code
     },
     "city": {
      DataType: "String",
      StringValue: city
    },
    "subdivision": {
      DataType: "String",
      StringValue: subdivision
    },
    "userPK": {
      DataType: "String",
      StringValue: subdivision
    }
   },
   MessageBody: JSON.stringify({entity: "Placeholder"}),
   QueueUrl: sqsUrl
 };

  return new Promise(function(resolve, reject) {
    sqs.sendMessage(params, function(err, data) {
          if (err) reject(err);
          else resolve(data);
        })
    });
};

/******************************* DYNAMO DB ***********************************/
function getkeywordUrl(slug){
  //gets active key words
  let params = {
    TableName: process.env.TABLE_NAME,
    IndexName: 'GSI1LNK',
    KeyConditionExpression: "slug = :slug",
    ExpressionAttributeValues: {":slug": slug}
  };

    return new Promise(function(resolve, reject) {
      ddb.query(params, function(err, data) {
          if (err) reject(err);
          else resolve(data);
        })
    })
};//End getExistingTwilioNumbers

const linkParams = (slug) => {
  return {
    Key: { 'GSI1PK' : `LINK#${slug}`},
    TableName: process.env.TABLE_NAME,
    IndexName: 'GSI1LNK',
  };
};

function get(params){
  return new Promise(function(resolve, reject) {
    ddb.get(params, function(err, data) {
        if (err) reject(err);
        else resolve(data);
      })
  });
};//End getKeyword

/******************************* UTILITIES ***********************************/

const redirectResponse = (location) => {
  let httpResp = {
      "statusCode": 302,
      "headers": {
        "Access-Control-Allow-Origin": "*",
        Location: location
          
      }
    };
  return httpResp;
};