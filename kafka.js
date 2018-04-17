
const harbor_name = 'kafka';
const dependencies = [
  'debug',
  'elytron',
  'underscore',
].join(' ');

require('child_process').execSync(`npm i ${dependencies}`);

const log = require('debug')(`${harbor_name}:log`);
const error = require('debug')(`${harbor_name}:error`);
const produce = require('elytron').produce;

log(`Dependencies installed: ${dependencies}`);

const MongoInternals = require('meteor/mongo').MongoInternals;
const mongo_url = process.env.MONGO_URL || 'mongodb://localhost:3001';
const db = MongoInternals.defaultRemoteCollectionDriver(mongo_url).mongo.db;
let collection_names = [];
let collections = db.listCollections();

collections.forEach((collection) => {
  collection_names.push(collection.name);
});

const render_input = (values = {}) => {

  const html = `
    <p>This harbor submits manifests to a Kafka broker.</p>
    <p>If configured as a Followup Lane to another lane, it will produce its 
    manifest (and prior manifest) to the Topic configured here when called.</p>
    <p>If database collections are selected here, it will observe any changes 
    to those collections, and produce them to the Topic configured here.</p>
    <p>If the same Lane is configured to do both, it will produce events from
    both scenarios to the same topic.</p>

    <label><h5>Kafka Broker Connection String:</h5>
      <input
        type=text
        name=kafka-broker-connection-string
        placeholder="kafka1,kafka2,kafka3 (etc., comma separated)"
        required
        value="${values['kafka-broker-connection-string'] || ''}"
      >
    </label>
    <label><h5>Topic:</h5>
      <input
        type=text
        name=kafka-topic
        placeholder="Example-Topic_Name.1 (alphanum, dash, underscore, dot)"
        required
        value="${values['kafka-topic'] || ''}"
      >
    </label>
    <h5>Collections found:</h5>
    <ul>
      ${
        collection_names.map(name => `
          <li>
            <label>
              <input
                name="collection:${name.replace('.', '_dot_')}"
                type=checkbox
                value="${name.replace('.', '_dot_')}"
                ${values && values[`collection:${name}`] ? 'checked' : ''}
              >
              ${name}
            </label>
          </li>
        `).join('')
      }
    </ul>
  `;

  return html;
};

const render_work_preview = () => `Nothing yet!`;

const register = () => harbor_name;

const update = (lane, values) => {
  if (! check_topic(values['kafka-topic'])) {
    error(`Invalid values passed for harbormaster-kafka config: ${values}`);
    return false;
  }

  set_hooks(values);
  return true;
};

const check_topic = (topic_name) => {
  const allowed_characters = /[a-z0-9_\.-]/gi;

  const results = topic_name.match(allowed_characters);
  return results.length == topic_name.length;
};

const set_hooks = (values) => {
  const keys = Object.keys(values);
  const collection_key = /^collection:/;
  const topic = values['kafka-topic'];

  _.each(hooks, hook => hook.stop());

  keys.forEach((key) => {
    if (!key.match(collection_key)) return;
    const collection_name = values[key].replace('_dot_', '.');
    const collection = Meteor.Collection.get(collection_name);
    hooks[collection] = collection.find().observe({
      added (doc) { return produce(topic, { type: 'added', doc }); },
      removed (old) { return produce(topic, { type: 'removed', old }); },
      changed (old, doc) {
        return produce(topic, { type: 'changed', old, doc });
      },
    });
  });
};

const hooks = {};

const work = () => {};

module.exports = {
  render_input,
  render_work_preview,
  register,
  update,
  work,
};
