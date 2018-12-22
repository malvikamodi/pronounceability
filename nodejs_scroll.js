const es = require('elasticsearch');
const async = require('async');
const amqp = require('amqplib');
const request = require('request-promise');

/**
 * configuration
 */
const CONFIG = {};
CONFIG.esPageSize = 10000;
CONFIG.hostES = "192.168.0.15"
CONFIG.portES = 9200

function esScroll(esClient){
    let collected = 0;
    let srollID = null;
    let res;
    while(!res || res.hits.total >= collected){
        if(!srollID){
            res = await esClient.search({
                index: 'dns-data',
                scroll: '1m',
                size: CONFIG.esPageSize,
                body: {"query": {"bool": {"must": [{"match": {"type": "NS"}}, {"match": {"is_registered": "true"}}]}},
                    "stored_fields": ["domain_name"]}
            });
        }else{
            res = await esClient.scroll({
                scrollId: srollID,
                scroll: '1m'
            });
        }
        srollID = res._scroll_id;
        for(const hit of res.hits.hits){
            collected++;
            yield hit;
        }
    }
}

function main(){

    let esClient = new es.Client({
        host: CONFIG.hostES
        port: CONFIG.portES
    });

    for await (const hit of esScroll(esClient)){
        try{
            console.log(hit);
        }catch(e){
            console.warn('Encounter abnormal document: %s', JSON.stringify(hit));
        }
       
    }
}

main().catch((err)=>{
    console.error(err);
    process.exit(1);
});
