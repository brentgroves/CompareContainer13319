const mariadb = require("mariadb");
const mqtt = require("mqtt");
const common = require("@bgroves/common");

const {
  MQTT_SERVER,
  MYSQL_HOSTNAME,
  MYSQL_USERNAME,
  MYSQL_PASSWORD,
  MYSQL_DATABASE,
} = process.env;

/*
  const MQTT_SERVER='localhost';
  const MYSQL_HOSTNAME= "localhost";
  const MYSQL_USERNAME= "brent";
  const MYSQL_PASSWORD= "JesusLives1!";
  const MYSQL_DATABASE= "mach2";
  */
const connectionString = {
  connectionLimit: 5,
  multipleStatements: true,
  host: MYSQL_HOSTNAME,
  user: MYSQL_USERNAME,
  password: MYSQL_PASSWORD,
  database: MYSQL_DATABASE,
};

common.log(
  `user: ${MYSQL_USERNAME},password: ${MYSQL_PASSWORD}, database: ${MYSQL_DATABASE}, MYSQL_HOSTNAME: ${MYSQL_HOSTNAME}`
);

const pool = mariadb.createPool(connectionString);

async function CompareContainer(transDate) {
  common.log(`CompareContainer.transDate=${transDate}` );
  let conn;
  try {
    conn = await pool.getConnection();
    const result = await conn.query("call CompareContainer(?)",[transDate]);
    let msgString = JSON.stringify(result[0]);
    const obj = JSON.parse(msgString.toString()); // payload is a buffer
    common.log(obj);
  } catch (err) {
    // handle the error
    common.log(`CompareContainer: Error =>${err}`);
  } finally {
    if (conn) conn.release(); //release to pool
  }
}

function main() {
  common.log(`CompareContainer13319 has started`);
  const mqttClient = mqtt.connect(`mqtt://${MQTT_SERVER}`);
  mqttClient.on("connect", function () {
    mqttClient.subscribe("Alarm13319-2", function (err) {
      if (!err) {
        common.log("CompareContainer has subscribed to: Alarm13319-2");
      }
    });
  });

  // message is a buffer
  mqttClient.on("message", function (topic, message) {
    const obj = JSON.parse(message.toString()); // payload is a buffer
    let transDate = obj.TransDate;
    common.log(`TransDate => ${transDate} `);
    CompareContainer(transDate);
  });
}
main();
