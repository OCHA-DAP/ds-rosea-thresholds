// Google Earth Engine JavaScript code for zonal population statistics
// Downloads population sums for specified African countries at admin level 2

// Define the list of target countries
var targetCountries = [
    'Burundi', 'Comoros', 'Djibouti', 'Kenya', 'Malawi',
    'Rwanda', 'Uganda', 'Tanzania', 'Zambia', 'Zimbabwe',
    'Angola', 'Eswatini', 'Lesotho', 'Madagascar', 'Namibia'
];

// Load your custom admin boundaries
var boundaries = ee.FeatureCollection("projects/ee-zackarno/assets/gaul2_asap_v05");

// Filter boundaries for target countries using name0 field
var filteredBoundaries = boundaries.filter(ee.Filter.inList('name0', targetCountries));

// Check which countries are actually present in the filtered data
var countriesInData = filteredBoundaries.distinct('name0').aggregate_array('name0');
print('Countries found in your GAUL data:', countriesInData);

// Check for any missing countries (due to spelling differences)
var targetCountriesList = ee.List(targetCountries);
var missingCountries = targetCountriesList.removeAll(countriesInData);
print('Missing countries (check spelling):', missingCountries);

// Print total admin2 units found
print('Total admin2 units found:', filteredBoundaries.size());

// Load population data (GPWv4.11 UN-adjusted population count)
var populationCollection = ee.ImageCollection('CIESIN/GPWv411/GPW_UNWPP-Adjusted_Population_Count');

// Filter for 2020 data
var population2020 = populationCollection
    .filter(ee.Filter.date('2020-01-01', '2020-12-31'))
    .first()
    .select('unwpp-adjusted_population_count');

// Function to calculate zonal statistics
var calculateZonalStats = function (feature) {
    // Calculate sum of population within each admin2 boundary
    var populationSum = population2020.reduceRegion({
        reducer: ee.Reducer.sum(),
        geometry: feature.geometry(),
        scale: 1000, // 1km resolution (adjust as needed)
        maxPixels: 1e9,
        tileScale: 4 // Helps with memory issues for large polygons
    });

    // Return feature with population statistics added
    return feature.set({
        'population_sum_2020': populationSum.get('unwpp-adjusted_population_count'),
        'area_km2': feature.geometry().area().divide(1e6) // Add area in km2 for reference
    });
};

// Apply zonal statistics calculation to all filtered boundaries
print('Calculating zonal statistics...');
var boundariesWithStats = filteredBoundaries.map(calculateZonalStats);

// Display the first few results to verify
print('Sample results (first 5 features):');
print(boundariesWithStats.limit(5));

// Visualize the boundaries on the map
Map.addLayer(filteredBoundaries, { color: 'red' }, 'Admin2 Boundaries');

// Center map on Africa
Map.setCenter(25, -5, 4);

// Export the results as a CSV file
Export.table.toDrive({
    collection: boundariesWithStats,
    description: 'worldpop_zmean_rosea_asap_l2',
    fileFormat: 'CSV',
    selectors: [
        'asap0_id', 'name0', 'name0_shr', 'asap2_id', 'name2', 'name2_shr',
        'population_sum_2020', 'area_km2', 'an_crop', 'an_range',
        'km2_crop', 'km2_range', 'km2_tot', 'water_lim'
    ]
});

// Alternative: Export as shapefile (includes geometry)
Export.table.toDrive({
    collection: boundariesWithStats,
    description: 'population_zonal_stats_asap_admin2_shapefile',
    fileFormat: 'SHP',
    selectors: [
        'asap0_id', 'name0', 'name0_shr', 'asap2_id', 'name2', 'name2_shr',
        'population_sum_2020', 'area_km2', 'an_crop', 'an_range',
        'km2_crop', 'km2_range', 'km2_tot', 'water_lim'
    ]
});

// Print summary statistics
print('Summary:');
print('Total admin2 units:', filteredBoundaries.size());
print('Export tasks created. Check the Tasks tab to run the exports.');

// Optional: Create a simple visualization of population density
// (Comment out this section if it causes display errors)
/*
var populationVis = {
  min: 0,
  max: 1000000,
  palette: ['white', 'yellow', 'orange', 'red', 'darkred']
};

Map.addLayer(population2020.clip(filteredBoundaries.geometry()), 
             populationVis, 'Population Count 2020');
*/

// Add legend
var legend = ui.Panel({
    style: {
        position: 'bottom-left',
        padding: '8px 15px'
    }
});

var legendTitle = ui.Label({
    value: 'Population Count 2020',
    style: {
        fontWeight: 'bold',
        fontSize: '18px',
        margin: '0 0 4px 0',
        padding: '0'
    }
});

legend.add(legendTitle);

var makeColorBarParams = function (palette) {
    return {
        bbox: [0, 0, 1, 0.1],
        dimensions: '100x10',
        format: 'png',
        min: 0,
        max: 1,
        palette: palette,
    };
};

var colorBar = ui.Thumbnail({
    image: ee.Image.pixelLonLat().select(0),
    params: makeColorBarParams(['white', 'yellow', 'orange', 'red', 'darkred']),
    style: { stretch: 'horizontal', margin: '0px 8px', maxHeight: '24px' },
});

var legendLabels = ui.Panel({
    widgets: [
        ui.Label('0', { margin: '4px 8px' }),
        ui.Label('500K', { margin: '4px 8px', textAlign: 'center', stretch: 'horizontal' }),
        ui.Label('1M+', { margin: '4px 8px' })
    ],
    layout: ui.Panel.Layout.flow('horizontal')
});

legend.add(colorBar);
legend.add(legendLabels);
Map.add(legend);