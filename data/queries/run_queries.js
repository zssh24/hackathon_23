const { DBSQLClient } = require('@databricks/sql');
const fs = require('fs/promises');

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
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2014-01-01' AND '2014-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2015-01-01' AND '2015-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2016-01-01' AND '2016-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2017-01-01' AND '2017-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2018-01-01' AND '2018-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2019-01-01' AND '2019-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2020-01-01' AND '2020-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2021-01-01' AND '2021-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2022-01-01' AND '2022-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;",
  "SELECT CASE WHEN local_authority_district_name IS NOT NULL THEN local_authority_district_name WHEN irish_county IS NOT NULL THEN irish_county ELSE '' END AS District, COUNT(*) AS `Number of products` FROM prod.warehouse.sales_events INNER JOIN prod.warehouse.delivery_addresses ON sales_events.delivery_address_sk = delivery_addresses.sk WHERE product_type <> 'STI Test Result Set' AND item_dispatched_at BETWEEN '2023-01-01' AND '2023-12-31' GROUP BY local_authority_district_name, irish_county ORDER BY District;"
];


const client = new DBSQLClient();
const connectOptions = {
  token: token,
  host: serverHostname,
  path: httpPath,
};

// Declare filenames for each query
const queryFileNames = {
  query1: 'total_products.json',
  query2: '2014.json',
  query3: '2015.json',
  query4: '2016.json',
  query5: '2017.json',
  query6: '2018.json',
  query7: '2019.json',
  query8: '2020.json',
  query9: '2021.json',
  query10: '2022.json',
  query11: '2023.json'
};

const executeQueries = async () => {
  try {
    await client.connect(connectOptions);

    // Use Object.keys to get the query names from the dictionary
    const queryNames = Object.keys(queryFileNames);

    for (let i = 0; i < queries.length; i++) {
      const query = queries[i];
      const session = await client.openSession();

      try {
        const queryOperation = await session.executeStatement(query, {
          runAsync: true,
          maxRows: 10000,
        });

        const result = await queryOperation.fetchAll();

        await queryOperation.close();

        // Print individual table for each query
        //console.log(`Results for Query ${i + 1}: ${query}`);
        //console.table(result);

        // Save results to a JSON file asynchronously using fs.promises
        const jsonResults = JSON.stringify(result, null, 2);
        const fileName = queryFileNames[queryNames[i]]; // Use query value from the dictionary
        await fs.writeFile(fileName, jsonResults);
        console.log(`Query ${i + 1} results saved to: ${fileName}`);

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