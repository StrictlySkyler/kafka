
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

const render_input = (values = {}) => {
  const html = `
    <p>This harbor submits manifests to a Kafka broker.</p>

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
  `;

  return html;
};

const render_work_preview = (manifest) => {
  return `
    <figure>
      <figcaption>
        A message will be produced to <code>${
          manifest['kafka-broker-connection-string']
        }</code> 
        on topic <code>${
          manifest['kafka-topic']
        }</code>.
      </figcaption>
    </figure>
  `;
};

const register = () => harbor_name;

const update = (lane, values) => {
  if (! check_topic(values['kafka-topic'])) {
    error(`Invalid values passed for harbormaster-kafka config: ${values}`);
    return false;
  }

  return true;
};

const check_topic = (topic_name) => {
  const allowed_characters = /[a-z0-9_\.-]/gi;

  const results = topic_name.match(allowed_characters);
  return results.length == topic_name.length;
};

const work = (lane, manifest) => {
  const topic = manifest['kafka-topic'];
  const brokers = manifest['kafka-broker-connection-string'];
  let exit_code = 1;

  produce(topic, manifest, null, $H.bindEnvironment((code) => {
    exit_code = code;

    $H.end_shipment(lane, exit_code, manifest);
  }), brokers);
};

module.exports = {
  render_input,
  render_work_preview,
  register,
  update,
  work,
};
