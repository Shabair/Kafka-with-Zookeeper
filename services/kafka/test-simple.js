// test-simple.js
import { Kafka } from 'kafkajs';

// Test with just ONE broker first
const kafka = new Kafka({
  clientId: 'test-client',
  brokers: ['localhost:9092'], // Just broker 1
});

async function test() {
  const admin = kafka.admin();
  try {
    await admin.connect();
    console.log('✅ Connected to Kafka via localhost:9092!');
    
    const clusterInfo = await admin.describeCluster();
    console.log(`Cluster has ${clusterInfo.brokers.length} brokers`);
    
    clusterInfo.brokers.forEach(broker => {
      console.log(`Broker ${broker.nodeId}: ${broker.host}:${broker.port}`);
    });
    
    await admin.disconnect();
  } catch (error) {
    console.error('❌ Connection failed:', error.message);
    console.log('\nTry these steps:');
    console.log('1. docker-compose ps (check if containers are running)');
    console.log('2. docker-compose logs kafka1 (check for errors)');
    console.log('3. docker-compose restart (restart the cluster)');
  }
}

test();