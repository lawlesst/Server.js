/** A VivoSparqlDatasource provides queryable access to a VIVO SPARQL endpoint. */

var Datasource = require('./Datasource'),
    SparqlDatasource = require('./SparqlDatasource'),
    N3 = require('n3'),
    LRU = require('lru-cache'),
    Readable = require('stream').Readable;

var ENDPOINT_ERROR = 'Error accessing SPARQL endpoint';
var INVALID_TURTLE_RESPONSE = 'The endpoint returned an invalid Turtle response.';
var NO_CREDENTIALS_ERROR = new Error('Error reading credentials.  Set email and password in config.json');

// Creates a VivoSparqlDatasource
function VivoSparqlDatasource(options) {
  if (!(this instanceof VivoSparqlDatasource))
    return new VivoSparqlDatasource(options);
  Datasource.call(this, options);
  this._countCache = LRU({ max: 1000, maxAge: 1000 * 60 * 60 * 3 });

  // Set endpoint URL and default graph
  options = options || {};
  this._endpoint = this._endpointUrl = (options.endpoint || '').replace(/[\?#][^]*$/, '');
  if ((!(options.email)) || (!(options.password)))
    throw NO_CREDENTIALS_ERROR;
  this._endpointUrl += '?email=' + options.email + '&password=' + options.password + '&query=';
}
SparqlDatasource.extend(VivoSparqlDatasource, ['triplePattern', 'limit', 'offset', 'totalCount']);

// Retrieves the (approximate) number of triples that match the SPARQL pattern
VivoSparqlDatasource.prototype._getPatternCount = function (sparqlPattern, callback) {
  // Try to find a cache match
  var cache = this._countCache, count = cache.get(sparqlPattern);
  if (count) return setImmediate(callback, null, count);
  // Execute the count query
  var countResponse = this._request({
    url: this._endpointUrl + encodeURIComponent(this._createCountQuery(sparqlPattern)),
    headers: { accept: 'application/sparql-results+json' },
    timeout: 7500,
  }, callback);
  countResponse.on('error', callback);

  countResponse.on('data', function (chunk) {
    //get the total from the select count query.
    var count = parseInt(JSON.parse(chunk).results.bindings[0].total.value);
    if (count > 100000) cache.set(sparqlPattern, count);
    countResponse.abort();
    callback(null, count || 10);
  });
};

// Creates a SELECT COUNT(*) query from the given SPARQL pattern
VivoSparqlDatasource.prototype._createCountQuery = function (sparqlPattern) {
  return ['SELECT (COUNT(*) as ?total)', 'WHERE', sparqlPattern].join(' ');
};

module.exports = VivoSparqlDatasource;
