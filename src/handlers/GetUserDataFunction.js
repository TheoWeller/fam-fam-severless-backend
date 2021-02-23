/**
 * @param {Object} event - API Gateway Lambda Proxy Input Format
 * @param {Object} context
 * @returns {Object} object - API Gateway Lambda Proxy Output Format
 */
 
const aws = require('aws-sdk');
const ddb = new aws.DynamoDB.DocumentClient({maxRetries: 5, retryDelayOptions: {base: 300} });

exports.GetUserDataHandler = async (event, context, callback) => {
const body = JSON.parse(event.body);
  
  try {
    const count = await getSubscriberCount(body.userId);
    return formHttpResp(200, count.Count);
  } catch(e) {
    console.log(`ERROR: ${e}`);
    // return formHttpResp(500, e); 
  };
};

/******************************* DYNAMO DB ***********************************/
function getSubscriberCount(userId){
  let params = {
    TableName: process.env.TABLE_NAME,
    KeyConditionExpression: '#PK = :PK and begins_with(#SK, :SK)',
    ExpressionAttributeNames:{"#PK": "PK", "#SK": 'SK'},
    ExpressionAttributeValues:  {':PK' : `USR#${userId}`, ':SK': `USRSUB#`},
    Select: 'COUNT'
  };

    return new Promise(function(resolve, reject) {
      ddb.query(params, function(err, data) {
          if (err) reject(err);
          else resolve(data);
        })
    })
};//End getSubscriberCount


/******************************* UTILITIES ***********************************/

const formHttpResp = (statusCode, respBody) => {
  let httpResp = {
      "statusCode": statusCode,
      "headers": {
          "Access-Control-Allow-Origin": "*"
      },
      "body": JSON.stringify(respBody)
    };
  return httpResp;
};