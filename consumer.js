var moment = require('moment-timezone'),

    // Kafka setup
    kafka = require('kafka-node'),
    HighLevelConsumer = kafka.HighLevelConsumer,
    Offset = kafka.Offset;

module.exports = function (client, db, consumerName, payload, groupId) {
  
  var consumer = new HighLevelConsumer(
      client,
      payload,
      {
        groupId: groupId // this identifies the group id and make the offset consumption scoped to this key
      }
  );

  consumer.on('ready', function() {
    console.log('KAFKA consumer ' + consumerName + ' ready');
  });

  consumer.on('message', function (message) {

    // example message: {"topic":"my-node-topic","value":"{\"timestamp\":1425599538}","offset":0,"partition":0,"key":{"type":"Buffer","data":[]}}
    db.collection('kafka').update(
    {
      _id: 123456 //always overrite the same doc just for illustration
    },
    {
      '$set': {
        message: message,
        received_at: moment().unix()
      },
      '$inc': {
        messagesCount: 1
      }
    },
    {
      upsert: true
    }, function(err, data) {
      if(err) {
        console.log('MONGO error updating document: ' + err);
      } else {
        console.log('MONGO updating document OK:' + message.value);
      }
    });

    fetchCommits(groupId, payload[0].topic, 0);
    fetchCommits(groupId, payload[0].topic, 1);

  });

  consumer.on('error', function (err) {
    console.log('KAFKA consumer ' + consumerName + ' error:' + err);
  });

  process.on('beforeExit', function(code) {
    //force offset commit
    consumer.close(true, function(err, data) {
      console.log('KAFKA consumer ' + consumerName + ' commit ok :)');
    });
  });

  // Offset setup to check offset of curent 
  var offset = new Offset(client);

  offset.on('ready', function() {
    console.log('Zookeper ready');
  });

  offset.on('connect', function() {
    console.log('Broker ready');
  });

  var fetchCommits = function (groupId, topicName, partition) {
    offset.fetchCommits(groupId, [
          { topic: topicName, partition: partition }
      ], function (err, data) {
          console.log('Fetch Commits from consumer ' + consumerName + ' group:' + groupId + ', topic:' + topicName + ', partition:' + partition);
          console.log(data);
    });
  };
 

 return consumer;
}