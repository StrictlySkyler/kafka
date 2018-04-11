
const harbor_name = 'kafka';
const dependencies = ['debug', 'elytron'].join(' ');

require('child_process').execSync(`npm i ${dependencies}`);

const log = require('debug')(`${harbor_name}:log`);
const error = require('debug')(`${harbor_name}:error`);

log(`Dependencies installed: ${dependencies}`);

const MongoInternals = require('meteor/mongo').MongoInternals;
const mongo_url = process.env.MONGO_URL || 'mongodb://localhost:3001';
const db = MongoInternals.defaultRemoteCollectionDriver(mongo_url).mongo.db;
let collection_names = [];
let collections = db.listCollections();

collections.forEach((collection) => {
  collection_names.push(collection.name);
});

const render_input = (values) => {

  const html = `
    <p>This harbor submits manifests to a Kafka broker.</p>
    <p>If configured as a Followup Lane to another lane, it will produce its 
    manifest to the Topic configured here when called.</p>
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
      >
    </label>
    <label><h5>Topic:</h5>
      <input
        type=text
        name=kafka-topic
        placeholder="Example-Topic_Name.1 (alphanum, dash, underscore, dot)"
        required
      >
    </label>
    <h5>Collections found:</h5>
    <ul>
      ${
        collection_names.map(name => `
          <li>
            <label>
              <input
                name="${name}"
                type=checkbox
                value="${name}"
                ${values && values[name] ? 'checked' : ''}
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

const update = () => true;

const work = () => {};

module.exports = {
  render_input,
  render_work_preview,
  register,
  update,
  work,
};
