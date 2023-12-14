const fs = require('fs');
const path = require('path');

// Specify the directory containing GeoJSON files
const geojsonDirectory = '.';

// Read all files in the directory
fs.readdir(geojsonDirectory, (err, files) => {
  if (err) {
    console.error('Error reading directory:', err);
    return;
  }

  // Filter only GeoJSON files
  const geojsonFiles = files.filter(file => file.endsWith('.geojson'));

  // Process each GeoJSON file
  geojsonFiles.forEach(geojsonFile => {
    const geojsonFilePath = path.join(geojsonDirectory, geojsonFile);
    const jsFileName = geojsonFile.replace('.geojson', '.js');
    const jsFilePath = path.join(geojsonDirectory, jsFileName);

    // Read the GeoJSON file
    const geojsonContent = fs.readFileSync(geojsonFilePath, 'utf8');

    // Create JavaScript file content
    const jsFileContent = `var ${jsFileName.replace('.js', 'JSON')} = ${geojsonContent};`;

    // Write the JavaScript file
    fs.writeFileSync(jsFilePath, jsFileContent, 'utf8');

    console.log(`Created ${jsFileName}`);
  });
});
