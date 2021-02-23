const aws = require('aws-sdk');
const ddb = new aws.DynamoDB.DocumentClient({maxRetries: 5, retryDelayOptions: {base: 300} });

const linkConsumerEndpointUrl = process.env.LINK_CONSUMER_ENDPOINT;

exports.CreateKeywordHandler = async (event, context, callback) => {
  const keywordPayload = JSON.parse(event.body);

  try {
    const keywordParams = putKeywordParams(keywordPayload);
    const putKeywordAndInsightEntity = await transWriteItems(transWriteItemsParams(keywordParams, keywordPayload.sub));
    console.log(`TRANSACTION PUT RESULT: ${JSON.stringify(putKeywordAndInsightEntity)}`);  
  } catch(e) {
    console.log(`ERROR: ${e}`);
  }    
      let response = {
        "statusCode": 200,
        "headers": {
          "Access-Control-Allow-Origin": "*"
        },
        "body": JSON.stringify({message: "success"})
      };

      return formHttpResp(200, {status: "Success!"})
};

/******************************* DYNAMO DB ***********************************/
function putKeywordParams(keyword){
  return {
    TableName: process.env.TABLE_NAME,
    Item: {
    'PK' : `KWD#${keyword.sub}`,
    'SK' : `WORD#${keyword.keyword}`,
    'word': keyword.keyword,
    'type' : `${keyword.keywordType}`,
    'messageBody': `${keyword.messageBody}`,
    'url': `${keyword.link}`,
    'slug': `${keyword.slug}`,
    'shortenedUrl': `${linkConsumerEndpointUrl}${keyword.slug}`,
    'expired': false,
    'createdAt': new Date().getTime()
    }
  };
};//End putUserParams

function putItem(params){
    return new Promise(function(resolve, reject) {
      ddb.put(params, function(err, data) {
          if (err) reject(err);
          else resolve(true);
      })
  });//End promise
};//End putItem

const transWriteItemsParams = (putKeywordParamsOutput, userSub) => {
  //Get keyword to insert in insight SK
  let keywordInsightItem = {
    'PK' : `USR#${userSub}`,
    'SK' :`KWD#INSIGHT#${putKeywordParamsOutput.Item.word}`,
    'kwdSK': putKeywordParamsOutput.Item.SK,
    'messagesSent' : 0,
    'linkClicks': 0,
    'locations': []
  };

  let params = {
    'TransactItems': [{
      'TableName': process.env.TABLE_NAME,
      'Put': putKeywordParamsOutput
    },
    {
      'Put': {
        'TableName': process.env.TABLE_NAME,
        'Item': keywordInsightItem
      }
    }
  ]};
  return params;
};//end transaction write items.

function transWriteItems(params){
  return new Promise(function(resolve, reject) {
    ddb.transactWrite(params, function(err, data) {
        if (err) reject(err);
        else resolve(data);
      })
  });
};

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