var moment = require('moment-timezone'),

    // Kafka setup
    kafka = require('kafka-node'),
    client = new kafka.Client(process.env.KAFKA_URL || '192.168.59.103:2181/', process.env.KAFKA_CONSUMER_ID || 'kafka-node-consumer', {
      sessionTimeout: 1000 // this is just to enable multiple restarts in debug and avoid https://github.com/SOHU-Co/kafka-node/issues/90 - should be removed in PRD
    }),
    HighLevelConsumer = kafka.HighLevelConsumer,

    // database
    mongojs = require('mongojs'),
    db = mongojs(process.env.MONGO_URL || 'localhost:27017/kafka'),
    Offset = kafka.Offset;;


var consumer = new HighLevelConsumer(
    client,
    [
        { topic: 'my-node-topic' }
    ],
    {
      groupId: 'worker.js', // this identifies consumer and make the offset consumption scoped to this key
      // Auto commit config
      autoCommit: false,
      autoCommitIntervalMs: 5000,
      // The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued, default 100ms
      fetchMaxWaitMs: 100,
      // This is the minimum number of bytes of messages that must be available to give a response, default 1 byte
      fetchMinBytes: 1,
      // The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
      fetchMaxBytes: 1024 * 10,
      // If set true, consumer will fetch message from the given offset in the payloads
      fromOffset: false,
      // If set to 'buffer', values will be returned as raw buffer objects.
      encoding: 'utf8'
    }
);

consumer.on('ready', function() {
  console.log('KAFKA consumer ready');
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
      
      // Commit the offset after the message is acknowledge
      consumer.commit(function(err, data) {
        if(err) {
          console.log('KAFKA committing the offset error: ' + err);
        } else {
          console.log('KAFKA committing sucess:' + data);
        }

        fetchCommits('worker.js', 'my-node-topic', 0);
      });
    }
  });
});

consumer.on('error', function (err) {
  console.log('KAFKA consumer error:' + err);
});

process.on('beforeExit', function(code) {
  //force offset commit
  consumer.close(true, function(err, data) {
    console.log('KAFKA consumer commit ok :)');
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
        console.log('Fetch Commits from ' + consumerName + ' group:' + groupId + ', topic:' + topicName + ', partition:' + partition);
        console.log(data);
  });
};
