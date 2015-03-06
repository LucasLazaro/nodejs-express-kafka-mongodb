var moment = require('moment-timezone'),

    // Kafka setup
    kafka = require('kafka-node'),
    client = new kafka.Client(process.env.KAFKA_URL || '192.168.59.103:2181/', process.env.KAFKA_CONSUMER_ID || 'kafka-node-consumer', {
      sessionTimeout: 1000 // this is just to enable multiple restarts in debug and avoid https://github.com/SOHU-Co/kafka-node/issues/90 - should be removed in PRD
    }),

    // database
    mongojs = require('mongojs'),
    db = mongojs(process.env.MONGO_URL || 'localhost:27017/kafka');



// Initialize two different consumers
// Define the groupId
var groupId = 'worker.js';
// Payload for the consumer
var payload = [ { topic: 'my-node-topic'} ];
var consumerA = new require('./consumer')(client, db, "consumerA", payload, groupId);
