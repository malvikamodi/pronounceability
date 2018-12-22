var elasticsearch = require('elasticsearch');
var ElasticsearchScrollStream = require('elasticsearch-scroll-stream');
var pronounceable = require('pronounceable');
var entropy = require('binary-shannon-entropy');
/**
 * configuration
 */
const CONFIG = {};
CONFIG.esPageSize = 10000;
//CONFIG.host = "192.168.0.15:9200"

CONFIG.hosts =  ["192.168.0.147:9200",
	              "192.168.0.162:9200",
	              "192.168.0.149:9200",
	              "192.168.0.145:9200",
	              "192.168.0.148:9200",
	              "192.168.0.146:9200",
	              "192.168.0.143:9200",
	              "192.168.0.152:9200",
	              "192.168.0.172:9200",
	              "192.168.0.169:9200",
	              "192.168.0.168:9200",
	              "192.168.0.166:9200",
	              "192.168.0.173:9200",
	              "192.168.0.165:9200",
	              "192.168.0.174:9200",
	              "192.168.0.170:9200",
	              "192.168.0.164:9200"]
var counter = 0;
var current_doc;

let elasticsearch_client = new elasticsearch.Client({
        host: CONFIG.hosts
    });

var es_stream = new ElasticsearchScrollStream(elasticsearch_client, {
  index: 'dns-data',
  scroll: '2m',
  size: CONFIG.esPageSize,
  body: {
	"query": {
        "bool": {
            "must_not": {
                "exists": {
                    "field": "domain_name_pronounceability"
                }
            }
        }
    }
  }
}, ['_id', '_score', '_routing']);

function update_document(index, _id, route, docType, p, segmented_p, entropy){

  payload  = {"script": {
    "lang": "painless",
    "source" : "ctx._source.domain_name_pronounceability = params.p; ctx._source.domain_name_segmented_pronounceability = params.segmented_p; ctx._source.domain_name_entropy = params.entropy;",
    "params": {
      "p":p,
      "segmented_p": segmented_p,
      "entropy": entropy}
  }}

  elasticsearch_client.update({
		  index: index,
		  type: docType,
		  id: _id,
		  routing: route,
		  body: payload
		});
	}

function compute_pronounceability(domain_name,id,route){
  
  split_name = domain_name.split(/[-.]+/);
  var domain_name_p = (pronounceable.score(domain_name)*100);
  var segmented_length = split_name.length; 
  var domain_name_segmented_p = 0;
  var domain_name_entropy = entropy(Buffer.from(domain_name));

  for(i = 0; i < segmented_length; i++)
	domain_name_segmented_p += (pronounceable.score(split_name[i])*100);
         
  update_document('dns-data',id, route,'RECORD',domain_name_p, domain_name_segmented_p, domain_name_entropy)
  
}


es_stream.on('data', function(data) {
  current_doc = JSON.parse(data.toString());
  domain_name = current_doc["domain_name"];
  id = current_doc["_id"];
  route = current_doc["_routing"];

  domain_name = domain_name.substring(0, domain_name.lastIndexOf(".", domain_name.length-2));
  compute_pronounceability(domain_name,id,route);
  counter++;
  console.log(counter)
});

es_stream.on('end', function() {
  console.log(counter);
});

es_stream.on('error', function(err) {
  console.log(err);
});


process.on('unhandledRejection', (reason, promise) => {
	  console.log('Unhandled Rejection at:', reason.stack || reason)
});
