const { DBSQLClient } = require('@databricks/sql');

const token          = 'dapi3509b026e18ba37790d3598c5007d9b5'
const serverHostname = 'dbc-25dae16e-9b76.cloud.databricks.com';
const httpPath       = '/sql/1.0/endpoints/6326b79d23570977';

if (!token || !serverHostname || !httpPath) {
  throw new Error("Cannot find Server Hostname, HTTP Path, or personal access token. " +
                  "Check the environment variables DATABRICKS_TOKEN, " +
                  "DATABRICKS_SERVER_HOSTNAME, and DATABRICKS_HTTP_PATH.");
}

const client = new DBSQLClient();
const connectOptions = {
  token: token,
  host: serverHostname,
  path: httpPath
};

client.connect(connectOptions)
  .then(async client => {
    const session = await client.openSession();
    const queryOperation = await session.executeStatement(
      'SELECT * FROM prod.warehouse.brands LIMIT 2',
      {
        runAsync: true,
        maxRows: 10000 // This option enables the direct results feature.
      }
    );

    const result = await queryOperation.fetchAll();

    await queryOperation.close();

    console.table(result);

    await session.close();
    await client.close();
})
.catch((error) => {
  console.log(error);
});