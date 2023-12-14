const { DBSQLClient } = require('@databricks/sql');
var fs = require('fs/promises');

const token = 'dapi3509b026e18ba37790d3598c5007d9b5';
const serverHostname = 'dbc-25dae16e-9b76.cloud.databricks.com';
const httpPath = '/sql/1.0/endpoints/6326b79d23570977';

if (!token || !serverHostname || !httpPath) {
  throw new Error("Cannot find Server Hostname, HTTP Path, or personal access token. " +
                  "Check the environment variables DATABRICKS_TOKEN, " +
                  "DATABRICKS_SERVER_HOSTNAME, and DATABRICKS_HTTP_PATH.");
}

const queries = [
  /*"SELECT local_authority_district_name AS District, product_type AS `Product type`, date_trunc('year', item_dispatched_at) AS Year, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' GROUP BY 1, 2, 3 ORDER BY District",*/

  //"SELECT local_authority_district_name AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' GROUP BY 1 ORDER BY District",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' GROUP BY local_authority_district_name, irish_county ORDER BY District;"

]

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

        // Save results to a JSON file asynchronously using fs.promises
        const jsonResults = JSON.stringify(result, null, 2);
        await fs.writeFile('data/total_products.json', jsonResults);
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
