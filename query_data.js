const { DBSQLClient } = require('@databricks/sql');

const token = 'dapi3509b026e18ba37790d3598c5007d9b5';
const serverHostname = 'dbc-25dae16e-9b76.cloud.databricks.com';
const httpPath = '/sql/1.0/endpoints/6326b79d23570977';

if (!token || !serverHostname || !httpPath) {
  throw new Error("Cannot find Server Hostname, HTTP Path, or personal access token. " +
                  "Check the environment variables DATABRICKS_TOKEN, " +
                  "DATABRICKS_SERVER_HOSTNAME, and DATABRICKS_HTTP_PATH.");
}

const queries = [
  'SELECT * FROM prod.warehouse.brands LIMIT 2',
  'SELECT * FROM prod.warehouse.demographics LIMIT 5',
  // Add more queries as needed
];

const client = new DBSQLClient();
const connectOptions = {
  token: token,
  host: serverHostname,
  path: httpPath,
};

const executeQueries = async () => {
  try {
    await client.connect(connectOptions);

    for (const query of queries) {
      const session = await client.openSession();

      try {
        const queryOperation = await session.executeStatement(query, {
          runAsync: true,
          maxRows: 10000,
        });

        const result = await queryOperation.fetchAll();

        await queryOperation.close();

        // Print individual table for each query
        console.log(`Results for Query: ${query}`);
        console.table(result);
      } catch (error) {
        console.log(`Error executing query: ${query}`, error);
      } finally {
        await session.close();
      }
    }

    await client.close();
  } catch (error) {
    console.log(error);
  }
};

executeQueries();
