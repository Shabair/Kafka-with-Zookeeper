import { Kafka, logLevel } from 'kafkajs';
import dotenv from 'dotenv';

dotenv.config();

// Get broker configuration from environment or use defaults
const KAFKA_BROKERS = process.env.KAFKA_BROKERS 
  ? process.env.KAFKA_BROKERS.split(',') 
  : ['localhost:9091', 'localhost:9092', 'localhost:9093'];

const KAFKA_CLIENT_ID = process.env.KAFKA_CLIENT_ID || 'kafka-admin-cluster';

// Create Kafka instance with multiple brokers
const kafka = new Kafka({
  clientId: KAFKA_CLIENT_ID,
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 100,
    retries: 8,
    maxRetryTime: 30000,
  },
});

// Create admin client
const admin = kafka.admin();

// Define topics with replication factor 3 (matching our 3 brokers)
const topics = [
  {
    topic: 'payment-successful',
    numPartitions: 6, // Increased partitions for better distribution
    replicationFactor: 3, // Full replication across all brokers
    configEntries: [
      { name: 'retention.ms', value: '604800000' }, // 7 days
      { name: 'cleanup.policy', value: 'delete' },
      { name: 'min.insync.replicas', value: '2' }, // At least 2 replicas must acknowledge
    ],
  },
  {
    topic: 'order-successful',
    numPartitions: 9, // More partitions for high throughput
    replicationFactor: 3,
    configEntries: [
      { name: 'retention.ms', value: '2592000000' }, // 30 days
      { name: 'cleanup.policy', value: 'compact,delete' },
      { name: 'min.insync.replicas', value: '2' },
      { name: 'compression.type', value: 'lz4' }, // Enable compression
    ],
  },
  {
    topic: 'email-successful',
    numPartitions: 3,
    replicationFactor: 3,
    configEntries: [
      { name: 'retention.ms', value: '86400000' }, // 1 day
      { name: 'min.insync.replicas', value: '2' },
      { name: 'message.timestamp.type', value: 'LogAppendTime' },
    ],
  },
  // System topics with higher replication
  {
    topic: 'audit-logs',
    numPartitions: 3,
    replicationFactor: 3,
    configEntries: [
      { name: 'retention.ms', value: '31536000000' }, // 1 year
      { name: 'cleanup.policy', value: 'delete' },
      { name: 'min.insync.replicas', value: '3' }, // All replicas must acknowledge
    ],
  },
];

async function getClusterInfo() {
  try {
    console.log('Connecting to Kafka cluster...');
    await admin.connect();
    
    const clusterMetadata = await admin.describeCluster();
    
    console.log('\n=== Kafka Cluster Information ===');
    console.log(`Cluster ID: ${clusterMetadata.clusterId}`);
    console.log(`Controller Broker ID: ${clusterMetadata.controller}`);
    console.log(`Number of Brokers: ${clusterMetadata.brokers.length}`);
    
    console.log('\nBrokers:');
    clusterMetadata.brokers.forEach(broker => {
      console.log(`  Broker ${broker.nodeId}: ${broker.host}:${broker.port}`);
    });
    
    return clusterMetadata;
  } catch (error) {
    console.error('Error fetching cluster info:', error);
    throw error;
  } finally {
    await admin.disconnect();
  }
}

async function createTopics() {
  try {
    // Get cluster info first
    console.log('Connecting to Kafka cluster...');
    await admin.connect();
    
    const clusterInfo = await admin.describeCluster();
    const brokerCount = clusterInfo.brokers.length;
    
    console.log(`\nCluster has ${brokerCount} brokers available`);
    
    // Adjust replication factor if needed
    topics.forEach(topic => {
      if (topic.replicationFactor > brokerCount) {
        console.warn(`Warning: Topic "${topic.topic}" replication factor (${topic.replicationFactor}) exceeds available brokers (${brokerCount}). Setting to ${brokerCount}.`);
        topic.replicationFactor = brokerCount;
      }
    });

    // Check if topics already exist
    const existingTopics = await admin.listTopics();
    console.log('\nExisting topics:', existingTopics.length);

    // Filter out topics that don't exist yet
    const topicsToCreate = topics.filter(
      (topicConfig) => !existingTopics.includes(topicConfig.topic)
    );

    if (topicsToCreate.length === 0) {
      console.log('All topics already exist.');
      return;
    }

    console.log(`\nCreating ${topicsToCreate.length} topics...`);
    
    // Create topics with cluster-aware settings
    await admin.createTopics({
      topics: topicsToCreate,
      validateOnly: false,
      waitForLeaders: true,
      timeout: 60000, // 60 seconds for multi-broker setup
    });

    console.log('\n✅ Topics created successfully!');

    // Verify topics were created
    const metadata = await admin.fetchTopicMetadata({
      topics: topics.map(t => t.topic),
    });
    
    console.log('\n=== Created Topics Summary ===');
    metadata.topics.forEach((topic) => {
      const partitionCount = topic.partitions.length;
      const replicationFactor = topic.partitions[0]?.replicas?.length || 0;
      console.log(`\nTopic: ${topic.name}`);
      console.log(`  Partitions: ${partitionCount}`);
      console.log(`  Replication Factor: ${replicationFactor}`);
      
      // Show partition distribution
      const leaderDistribution = {};
      topic.partitions.forEach((partition) => {
        const leader = partition.leader;
        leaderDistribution[leader] = (leaderDistribution[leader] || 0) + 1;
      });
      
      console.log('  Partition Leaders Distribution:');
      Object.entries(leaderDistribution).forEach(([leaderId, count]) => {
        console.log(`    Broker ${leaderId}: ${count} partitions`);
      });
    });

  } catch (error) {
    console.error('\n❌ Error creating topics:', error);
    if (error.code) {
      console.error(`Error Code: ${error.code}`);
    }
    if (error.retriable !== undefined) {
      console.error(`Retriable: ${error.retriable}`);
    }
  } finally {
    await admin.disconnect();
    console.log('\nDisconnected from Kafka cluster.');
  }
}

async function describeTopic(topicName) {
  try {
    await admin.connect();
    
    console.log(`\nFetching details for topic: ${topicName}`);
    
    const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
    
    if (metadata.topics.length === 0) {
      console.log(`Topic "${topicName}" not found.`);
      return;
    }
    
    const topic = metadata.topics[0];
    
    console.log(`\n=== Topic: ${topic.name} ===`);
    console.log(`Partitions: ${topic.partitions.length}`);
    
    topic.partitions.forEach((partition) => {
      console.log(`\nPartition ${partition.partitionId}:`);
      console.log(`  Leader: Broker ${partition.leader}`);
      console.log(`  Replicas: [${partition.replicas.join(', ')}]`);
      console.log(`  ISR (In-Sync Replicas): [${partition.isr.join(', ')}]`);
    });
    
    // Get topic configuration
    const configs = await admin.describeConfigs({
      resources: [{ type: 2, name: topicName }] // Type 2 = TOPIC
    });
    
    console.log('\nTopic Configuration:');
    configs.resources.forEach(resource => {
      resource.configEntries.forEach(config => {
        if (!config.isDefault) {
          console.log(`  ${config.name}: ${config.value} (${config.isSensitive ? 'Sensitive' : 'Source: ' + config.source})`);
        }
      });
    });
    
  } catch (error) {
    console.error(`Error describing topic ${topicName}:`, error);
  } finally {
    await admin.disconnect();
  }
}

async function deleteTopic(topicName) {
  try {
    await admin.connect();
    console.log(`\nDeleting topic: ${topicName}`);
    
    await admin.deleteTopics({
      topics: [topicName],
      timeout: 60000,
    });
    
    console.log(`✅ Topic "${topicName}" deleted successfully.`);
  } catch (error) {
    console.error(`❌ Error deleting topic ${topicName}:`, error);
  } finally {
    await admin.disconnect();
  }
}

async function listTopics() {
  try {
    await admin.connect();
    const topicsList = await admin.listTopics();
    
    console.log('\n=== All Topics ===');
    console.log(`Total Topics: ${topicsList.length}`);
    
    // Get metadata for all topics
    const metadata = await admin.fetchTopicMetadata();
    
    console.log('\nTopic Details:');
    metadata.topics.forEach((topic, index) => {
      const partitions = topic.partitions.length;
      const replicationFactor = topic.partitions[0]?.replicas?.length || 0;
      console.log(`\n${index + 1}. ${topic.name}`);
      console.log(`   Partitions: ${partitions}`);
      console.log(`   Replication Factor: ${replicationFactor}`);
    });
    
  } catch (error) {
    console.error('Error listing topics:', error);
  } finally {
    await admin.disconnect();
  }
}

async function checkClusterHealth() {
  try {
    console.log('Checking cluster health...');
    await admin.connect();
    
    const clusterInfo = await admin.describeCluster();
    console.log(`\n✅ Cluster is healthy`);
    console.log(`   Cluster ID: ${clusterInfo.clusterId}`);
    console.log(`   Controller: Broker ${clusterInfo.controller}`);
    console.log(`   Active Brokers: ${clusterInfo.brokers.length}`);
    
    // Check each broker
    for (const broker of clusterInfo.brokers) {
      console.log(`   ✓ Broker ${broker.nodeId}: ${broker.host}:${broker.port}`);
    }
    
    return true;
  } catch (error) {
    console.error('\n❌ Cluster health check failed:', error.message);
    return false;
  } finally {
    await admin.disconnect();
  }
}

// CLI interface
async function main() {
  const command = process.argv[2];
  const arg = process.argv[3];
  
  console.log(`Kafka Brokers: ${KAFKA_BROKERS.join(', ')}`);
  
  switch (command) {
    case 'create':
      await createTopics();
      break;
    case 'list':
      await listTopics();
      break;
    case 'describe':
      if (!arg) {
        console.error('Please provide a topic name to describe');
        process.exit(1);
      }
      await describeTopic(arg);
      break;
    case 'delete':
      if (!arg) {
        console.error('Please provide a topic name to delete');
        process.exit(1);
      }
      await deleteTopic(arg);
      break;
    case 'health':
      await checkClusterHealth();
      break;
    case 'info':
      await getClusterInfo();
      break;
    default:
      console.log(`
🚀 Kafka Multi-Broker Admin Tool 🚀

Usage:
  node admin.js create     - Create all predefined topics
  node admin.js list       - List all topics with details
  node admin.js describe <topic> - Describe specific topic
  node admin.js delete <topic>   - Delete a specific topic
  node admin.js health     - Check cluster health
  node admin.js info       - Show cluster information

Example:
  npm run create
  npm run list
  npm run describe -- payment-successful
  npm run delete -- payment-successful
  npm run health
      `);
  }
}

// Run if called directly
if (process.argv[1] && process.argv[1].includes('admin.js')) {
  main().catch(console.error);
}

export {
  createTopics,
  listTopics,
  describeTopic,
  deleteTopic,
  checkClusterHealth,
  getClusterInfo,
  admin,
  topics,
  kafka,
};