const aws = require('aws-sdk');
const ddb = new aws.DynamoDB.DocumentClient({maxRetries: 5, retryDelayOptions: {base: 300} });
const iotdata = new aws.IotData({endpoint: process.env.IOT_ENDPOINT});
const twilioClient = require('twilio')(accountSid, authToken);
const { nanoid } = require('nanoid');

const querystring = require('querystring');
const accountSid = process.env.ACCOUNT_SID;
const authToken = process.env.AUTH_TOKEN;
const insightUrl = process.env.LINK_ENTITY_URL_ENDPOINT;

exports.KeywordConsumerHandler = async (event, context, callback) => {
  let userId = event.pathParameters.proxy;
  let sms = querystring.parse(event.body);
  let subId = sms.From;
  let keyword = sms.Body.toLocaleLowerCase().trim();
  let response = {
    "statusCode": 200,
    "headers": {
      "Access-Control-Allow-Origin": "*"
    },
    "body": JSON.stringify({message: "success"})
  };

  switch(keyword) {
    case 'stop':
    case 'quit':
    case 'stopall':
    case 'cancel':
    case 'unsubscribe':
      await handleUnsubscribe(subId, userId, sms);
      callback(null, response);
      break;
    case 'subscribe':
    case 'start':
    case 'sub':
      await handlePutSub(userId, subId, sms);
      callback(null, response);
      break;
    case 'help':
    case 'info':
      twilioClient.messages.create({body: 'Help successful.', from: sms.To, to: sms.From});
      callback(null, response);
      break;
    default:
      try {
        const creditCount = await get(userInsightParams(userId));
        if(creditCount.Item.creditCount <= 0) return callback(null, response);
        await handleUserDefinedKeyword(keyword, userId, sms);
        callback(null, response);
      } catch (e) {
        console.log(`TWILIO ERROR RESPONSE = ${JSON.stringify(e)}`);
        callback(null, response);
      };
  }//End switch statement     
};//End KeywordConsumerHandler

/******************************* IoT ***********************************/
//Publishes message to user's IoT topic when new subscriber
function publishMessage(topic, payload){
  let params = {
    topic: topic,
    payload: JSON.stringify(payload),
    qos: 0
  };

  return new Promise(function(resolve, reject) {
    iotdata.publish(params, function(err, data) {
          if (err) reject(err);
          else resolve(data);
        })
    });
};

/******************************* TWILIO ***********************************/
const twilioSendSms = (userPhone, subPhone, msgBody) => {
  return new Promise(async function(resolve, reject) {
    twilioClient.messages.create({
      body: msgBody,
      from: userPhone,
      to: subPhone
    })
    .then(r => {
      console.log(`TWILIO RESPONSE = ${JSON.stringify(r)}`);
      resolve(r)
    })
    .catch(e => {
      console.log(`TWILIO ERROR RESPONSE = ${JSON.stringify(e)}`);
      reject(e);
    });
  });//End promise
};
/******************************* DYNAMO DB ***********************************/

function putWithConditional(params){
    return new Promise(function(resolve, reject) {
      ddb.put(params, function(err, data) {
          if (err) {
            err.code === "ConditionalCheckFailedException" && resolve(false);
            reject(err);
          } else {
            resolve(true);
          };//End conditional
        });//End PUT
    });//End Promise
};//End putUsrSub

const transWriteItemsParams = (items) => {
  return {
    'TransactItems': items
  };
};//End transWriteItemsParams

function get(params){
    return new Promise(function(resolve, reject) {
      ddb.get(params, function(err, data) {
          if (err) reject(err);
          else resolve(data);
        })
    });
};//End getKeyword

const deleteLink = (params) => {
  return new Promise(function(resolve, reject) {
    ddb.delete(params, function(err, data) {
        if (err) reject(err);
        else resolve(true);
      })
  });
};//End deleteLink

function transWriteItems(params){
  return new Promise(function(resolve, reject) {
    ddb.transactWrite(params, function(err, data) {
        if (err) reject(err);
        else resolve(true);
      })
  });
};

const userInsightParams = (usrId) => {
  return {
    Key: { 'PK' : `USR#${usrId}`, 'SK' : 'INSIGHT#'},
    AttributesToGet: ['creditCount'],
    TableName: process.env.TABLE_NAME
  };
};//End userInsightParams

/******************************* HANDLERS ***********************************/
const handleUserDefinedKeyword = (keyword, userId, sms) => {
  return new Promise(async function(resolve, reject) {
    try {
      let kwd = await get(keywordParams(keyword.toUpperCase(), userId));
      if(Object.keys(kwd).length === 0) return resolve();
      if(await messageAlreadyExists(userId, keyword.toUpperCase(), sms.From)) return resolve();
      let result = await handleKeywordWithLink(kwd.Item, sms, userId);
      resolve(result);
    } catch(e) {
      console.log(`ERROR: ${JSON.stringify(e)}`);
      resolve(e);
    }//End catch
  });//End promise
};//End handleKeyword

const messageAlreadyExists = (userId, keyWord, recipient) => {
  return new Promise(async function(resolve) {
    const response = await get(keywordMsgParams(userId, keyWord, recipient));
    let result = Object.keys(response).length === 0;
    resolve(!result);
  });//End promise
};//End messageAlreadyExists

const handleKeywordWithLink = (keywordItem, sms, userId) => {
  return new Promise(async function(resolve, reject) {
    let { keyword, generatedSlug, generatedUrl, updatedMessageBody, putLinkParams } = keywordWithLinkVariables(keywordItem, sms, userId);
    
    try {
      //handlePutLink resolves to finalized slug & sms body on success
      let { confirmedSlug, confirmedSmsBody } = await handlePutLink(putLinkParams, generatedSlug, generatedUrl, updatedMessageBody);
      let handleMessageSentParams = { confirmedSlug, userId, keyWord: keyword.word, recipient: sms.From};
      let twilioResponse = await handleSendSmsWithLink(sms.To, sms.From, confirmedSmsBody, handleMessageSentParams);
      let twilioResponse = await twilioSendSms(sms.To, sms.From, confirmedSmsBody);
      await handleMessageSent({...handleMessageSentParams, sid: twilioResponse.sid});
      resolve(twilioResponse);
    } catch (e) {
      console.log(`ERROR: ${JSON.stringify(e)}`);
      reject(e);
    };//End catch

  });//End promise
};//End handleKeywordAction

const handlePutLink = (params, slug, generatedUrl, smsBody) => {
  return new Promise(async function(resolve, reject) {
    let confirmedSmsBody = smsBody;
    let confirmedSlug = slug;
    let putLinkSuccess = await putWithConditional(params);
    if(putLinkSuccess) {
      return resolve({confirmedSlug, confirmedSmsBody});
    } else {
      let { putSuccess, newSlug, updatedSmsBody } = await putLinkRetryHandler(params, generatedUrl, smsBody);
      confirmedSmsBody = updatedSmsBody;
      confirmedSlug = newSlug;
      return putSuccess ? resolve({confirmedSlug, confirmedSmsBody}) : reject("Put link operation failed");
    };//End conditional
  });//End Promise
};//End handlePutLink

const putLinkRetryHandler = (params, oldUrl, oldSmsBody, maxRetries = 5) => {
  return new Promise(async function(resolve, reject) {
    let updatedParams = params;

    for (let i = 0; i < maxRetries; i++){
      let newSlug = nanoid(6);
      updatedParams.Item.PK = `LINK#${newSlug}`;
      const retrySuccessful = await putWithConditional(updatedParams); 
      if(retrySuccessful) {
        let newUrl = `${insightUrl}${newSlug}`;
        let newBody = updateMessageBody(oldUrl, oldSmsBody, newUrl);
        return resolve({putSuccess: true, newSlug: newSlug, updatedSmsBody: newBody});
      };
    }//End for loop
    console.log("putLinkRetryHandler Failed - Requires immediate attention.");
    resolve({putSuccess: false, newSlug: null, updatedSmsBody: null});
  });//End promise
};//End putLinkRetryHandler

const handleSendSmsWithLink = async (smsTo, smsFrom, messageBody, failedMessageParams) => {
  return new Promise(async function(resolve, reject) {
    try {
      let resp = await twilioSendSms(smsTo, smsFrom, messageBody);
      return resolve(resp);
    } catch (e) {
      //Link cleanup for failed messages
      await handleMessageSentFailed(failedMessageParams);
      reject(e);
    };//End catch
  });//End Promise
};//End handleSendSmsWithLink

const handleMessageSent = async (params) => {
  return new Promise(async function(resolve, reject) {
    try {
      let transactionParams = transWriteItemsParams([
        updateKeywordInsightParams(params.keyWord, params.userId),
        updateUserCreditCountParams(params.userId),
        putKeywordMsgParams(params.recipient, params.userId, params.keyWord, params.sid)
      ]);
      await transWriteItems(transactionParams);
      resolve(true);
    } catch(e) {
      console.log(`ERROR: ${JSON.stringify(e)}`);
      resolve(false);
    };//End catch
  });//End promise
};//End handleMessageSent

const handleMessageSentFailed = (params) => {
  return new Promise(async function(resolve, reject) {
    try {
      const { slug, userId, keyWord } = params;
      resolve(await deleteLink(deleteLinkEntityParams(slug, userId, keyWord)));
    } catch (e) {
      console.log(`ERROR: ${JSON.stringify(e)}`);
      resolve(false);
    };//End catch
  });//End promise
};//End handleMessageSentFailed

/******************************* ********* ***********************************/
/******************************* UTILITIES ***********************************/
const keywordMsgParams = (userId, keyWord, recipient) => {
  return {
    Key: { 'PK' : `USR#${userId}`, 'SK' : `KWD#${keyWord}#MSG#${recipient}`},
    TableName: process.env.TABLE_NAME
  };
};//End keywordMsgParams

const updateMessageBody = (urlToReplace, messageBody, replacementUrl) => {
  return messageBody.replace(urlToReplace, replacementUrl);
};

const keywordWithLinkVariables = (keyword, sms, userId) => {
  let slug = nanoid(6);
  let url = `${insightUrl}${slug}`;
  return {
    keyword: keyword,
    generatedSlug: slug,
    generatedUrl: url,
    updatedMessageBody: updateMessageBody(keyword.shortenedUrl, keyword.messageBody, url),
    putLinkParams: linkEntityParams(userId, sms.From, keyword, slug)     
  };
};//End keywordWithLinkVariables

const keywordParams = (keyword, usrId) => {
  return {
    Key: { 'PK' : `KWD#${usrId}`, 'SK' : `WORD#${keyword}`},
    TableName: process.env.TABLE_NAME
  };
};//End keywordParams

const deleteLinkEntityParams = (linkId, userId, keyWord) => {
  const item = {
    'PK' : `LINK#${linkId}`,
    'SK' : `USR#${userId}#KWD#${keyWord}`,
  };

  return {
    TableName: process.env.TABLE_NAME,
    Key: item
  };
};

const updateKeywordInsightParams = (keyWord, userId) => {
  let item = {
    'PK' : `USR#${userId}`,
    'SK' : `KWD#INSIGHT#${keyWord}`
  };

  return {
    'Update': {
      TableName: process.env.TABLE_NAME,
      Key: item,
      UpdateExpression: 'SET messagesSent = messagesSent + :incr',
      ExpressionAttributeValues: {':incr': 1}
    }
  };
};//End keywordParams

const putKeywordMsgParams = (recipient, userId, keyWord, sid) => {

  let item = {
    'PK' : `USR#${userId}`,
    'SK' : `KWD#${keyWord}#MSG#${recipient}`,
    'recipient': recipient,
    'userId': userId,
    'sid': sid,
    'entity': 'KWD_MSG'
  };

  return {
    'Put': {
      TableName: process.env.TABLE_NAME,
      Item: item,
      ConditionExpression: 'attribute_not_exists(SK)'
    }
  };
};//End putUnSubRecordParams

const updateUserCreditCountParams = (userId) => {
  let item = {
    'PK' : `USR#${userId}`,
    'SK' : `INSIGHT#`
  };

  return {
    'Update': {
      TableName: process.env.TABLE_NAME,
      Key: item,
      UpdateExpression: 'SET creditCount = creditCount - :decr',
      ExpressionAttributeValues: {':decr': 1}
    }
  };
};

const linkEntityParams = (userId, subId, keyword, generatedSlug) => {
  return {
    TableName: process.env.TABLE_NAME,
    Item: {
      'PK' : `LINK#${generatedSlug}`,
      'SK' : `USR#${userId}#KWD#${keyword.word}`,
      'GSI1PK': `USR#${userId}#KWD#${keyword.word}`,
      'GSI1SK': `LINK#${generatedSlug}`,
      'url': keyword.url,
      'recipientPhoneNumber': subId,
      'keywordPK': keyword.PK,
      'keywordSK': keyword.SK,
      'entity': 'LINK'
    },
    ConditionExpression: 'attribute_not_exists(PK)'
  };
};//End linkEntityParams

/******************************* HANDLERS ***********************************/
const handlePutSub = (id, subId, sms) => {
  return new Promise(async function(resolve, reject) {
    try {
      await putWithConditional(putSubParams(subId));
      let putUserSubSuccess = await putWithConditional(putUserSubParams(id, subId, sms));
      putUserSubSuccess && await publishMessage(id, {NEW_SUBSCRIBER: 1});
      resolve(true);
    } catch(e) {
      //TODO: Handle failed entity PUT
      console.log(`ERROR: ${e}`);
      reject(e)
    };//end catch
  });//End Promise
};//End handlePutSub

/******************************* UTILITIES ***********************************/
const putUserSubParams = (id, subId, sms) => {
  return {
    TableName: process.env.TABLE_NAME,
    Item: {
      'PK' : `USR#${id}`,
      'SK' : `USRSUB#${id}#${subId}`,
      'entity': 'USER_SUB',
      'subMsg': JSON.stringify({...sms, userId: id})
    },
    ConditionExpression: 'attribute_not_exists(SK)'
  };
};

const putSubParams = (subId) => {
  return {
    TableName: process.env.TABLE_NAME,
    Item: {
      'PK' : `SUB#${subId}`,
      'SK' : `PROF#`,
      'entity': 'SUBSCRIBER'
    },
    ConditionExpression: 'attribute_not_exists(PK)'
  };
};//End putSubParams
/******************************* UNSUBSCRIBE: KEYWORDS ***********************************/

/******************************* ********* ***********************************/
/******************************* HANDLERS ***********************************/
const handleUnsubscribe = (subId, userId, sms) => {
  return new Promise(async function(resolve, reject) {
    try {
      let unsubParams = transWriteItemsParams([
        putUnSubRecordParams(subId, userId, JSON.stringify(sms)),
        deleteUserSubItemParams(userId, subId)
      ]);
  
      let writeSuccess = await transWriteItems(unsubParams);
      resolve();
    } catch(e) {
      console.log("ERROR-----", JSON.stringify(e));
      //TODO: Send to dead letter queue
      resolve();
    }//end catch
  });//End promise
};//End handleUnsubscribe
/******************************* ********* ***********************************/
/******************************* UTILITIES ***********************************/
const putUnSubRecordParams = (subId, userId, unSubMsg) => {

  let item = {
    'PK' : `UNSUB#${subId}`,
    'SK' : `USR#${userId}#DATE#${Date.now().toString()}`,
    'unSubMsg': unSubMsg,
    'entity': 'UNSUB_RECORD'
  };

  return {
    'Put': {
      TableName: process.env.TABLE_NAME,
      Item: item
    }
  };
};//End putUnSubRecordParams

const deleteUserSubItemParams = (usrId, subId) => {
  let item = {
    'PK' : `USR#${usrId}`,
    'SK' : `USRSUB#${usrId}#${subId}`
  };

  return {
    'Delete': {
      TableName: process.env.TABLE_NAME,
      Key: item,
      ReturnValues: 'ALL_OLD'
    }
  };
};//End deleteUserSubItemParams