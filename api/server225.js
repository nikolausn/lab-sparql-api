//author: nnp2

//http and rest nodejs library
var express = require('express');
var app = express();
//reading file system / file
var fs = require("fs");
//url parser to
var url = require('url');
//serialize querystring parameter
var querystring = require('querystring');
//var http = require('http');
var request = require("request");
//Library to make sync request call
var thenRequest = require('then-request');
//Library to call async request
var async = require('async');
//array, collection and text library
var _ = require('underscore');
//jsonld library
var jsonld = require('jsonld');
//Logger
var log = require('winston');
//log.log = console.log.bind(console); // don't forget to bind to console!
//Promise
var Q = require('q');

//Setting log
log.level = "debug";

//load jsonConfig
//var jsonConfig = require(__dirname + "/config-hashed.json");
var jsonConfig = {};

console.time("loadconfig");
//init json config from url
resourceRoot = "http://acbres225.ischool.illinois.edu/dcWSfetch_resources/"
jsonConfigFile = "config-hashed.json"
	//load configuration
async.series([
	function() {
		result = thenRequest('GET', resourceRoot + jsonConfigFile);
		result.then(function(res) {
			jsonConfig = JSON.parse(res.getBody('UTF-8'));
			//parse json Config, find any resource configuration url
			Object.keys(jsonConfig.services).forEach(function(keyConfig) {
				var configService = jsonConfig.services[keyConfig];
				log.debug("configService: " + JSON.stringify(configService));
				Object.keys(configService).forEach(function(key) {
					var methodConfig = configService[key];
					log.debug("methodConfig: " + JSON.stringify(methodConfig));
					//parse Sparql definition
					if (_.isObject(methodConfig)) {
						log.debug("getSparqlResource");
						console.time("getSparqlResource");
						if (methodConfig['query'] !== undefined) {
							result = thenRequest('GET', methodConfig['query']);
							result.then(function(resSparql) {
								console.timeEnd("getSparqlResource");
								data = resSparql.getBody('UTF-8');
								methodConfig['resQuery'] = data;
								log.debug('rdf query ' + data);
							});
						}
						log.debug("getPagingResource");
						if (methodConfig['paging'] !== undefined) {
							result = thenRequest('GET', methodConfig['paging']['query']);
							result.then(function(resSparql) {
								data = resSparql.getBody('UTF-8');
								methodConfig['resPaging'] = data;
								log.debug('rdf query ' + data);
							});
						}
						//parse frame definition
						log.debug("getFrameResource");
						if (methodConfig['frame'] !== undefined) {
							console.time("getFrameResource");
							resultFrame = thenRequest('GET', methodConfig['frame']);
							resultFrame.then(function(resSparql) {
								console.timeEnd("getFrameResource");
								data = resSparql.getBody('UTF-8');
								methodConfig['resFrame'] = JSON.parse(data);
								log.debug('frame ' + data);
							});
						}
						//parse frame definition
						log.debug("getContextResource");
						if (methodConfig['context'] !== undefined) {
							console.time("getContextResource");
							resultContext = thenRequest('GET', methodConfig['context']);
							resultContext.then(function(resSparql) {
								console.timeEnd("getContextResource");
								data = resSparql.getBody('UTF-8');
								methodConfig['resContext'] = JSON.parse(data);
								log.debug('context ' + data);
							});
						}
					}
				})
			})
			log.debug("full config: " + JSON.stringify(jsonConfig));
			console.timeEnd("loadconfig");
		})
	}
])


app.get("/dcWSfetch/:serviceMethod", function(req, res) {
	// set content-type based on output parameter
	var serviceName = "dcWSfetch";
	var serviceMethod = req.params.serviceMethod;
	log.debug('execute: '+serviceMethod);	
	//Define object for error return value
	var errorStatus = {
		errorcode: -1
	};
	var limit = jsonConfig.services[serviceName]['limit'];


	//Trap for unconfig servicename
	/*
	if (typeof(jsonConfig.services[serviceName]) == "undefined") {
		log.debug("undefined serviceName");
		throw Error("service " + serviceName + " is undefined");
	}
	*/
	try {
		//Trap for unconfig servicemethod

		if (jsonConfig.services[serviceName][serviceMethod] == undefined) {
			log.debug("undefined serviceMethod")
			throw Error("service method " + serviceMethod + " for " + serviceName + " is undefined");
			//errorStatus.message = "service method " + serviceMethod + " for " + serviceName + " is undefined";
			//res.end(JSON.stringify(errorStatus))						
		}


		var queryString = url.parse(req.url, true).query;
		//log.debug(queryString);
		requiredParam = jsonConfig.services[serviceName][serviceMethod]["required"];
		optionalParam = jsonConfig.services[serviceName][serviceMethod]["optional"];
		if(optionalParam !== undefined){
			optionalParam = [];
			optionalParam.push("offset");
			optionalParam.push("limit");
		}
		log.debug(requiredParam);
		format = "application/x-json+ld";
		output = "application/ld+json"
		params = {};
		var serviceConfig = jsonConfig.services[serviceName][serviceMethod];
		//Parse url parameters , check if there is some parameters that is not allowed
		console.time("parseparam");
		Object.keys(queryString).forEach(function(key) {
			if (!_.contains(requiredParam, key) && !_.contains(optionalParam, key)) {
				throw Error("parameter " + key + " is not allowed");
			}
			var val = queryString[key];
			if (serviceConfig[key] !== undefined && serviceConfig[key]['type'] !== undefined) {
				switch (serviceConfig[key]['type']) {
					case "url":
						urlRegex = /^(https?:\/\/(?:www\.|(?!www))[^\s\.]+\.[^\s]{2,}|www\.[^\s]+\.[^\s]{2,})\w$/;
						result = val.match(urlRegex);
						val = '<'+val+'>';
						//log.debug("result: " + JSON.stringify(result));
						if (result === null) {
							throw Error("Parameter " + key + " must be a URL, example http://hathitrust.org/id/123");
						}
						break;
					case "number":
						numberRegex = /^[0-9]*/;
						result = val.match(numberRegex);
						//log.debug("result: " + JSON.stringify(result));
						if (result === null) {
							throw Error("Parameter " + key + " must be a number");
						}
						break;
				}
			}
			//Change to params[key] only
			params[key] = val;
		});
		console.timeEnd("parseparam");
		log.debug('params: ' + params);
		log.debug('format: ' + format)
			//Set response / result header into requested format
		res.setHeader('content-type', output);

		host = "acbres225.ischool.illinois.edu";
		defaultGraphUrl = "";
		shouldSponge = "";
		timeout = 0;
		debug = "off";
		query = "";
		//log.debug("my Config: " + JSON.stringify(jsonConfig));
		var body = {};
		//	async.series([function() {
		//if not framed then the sparql file template should be there
		if (jsonConfig.services[serviceName][serviceMethod]['frame'] == undefined) {
			//Change from readfile to get sparql url
			//			fs.readFile(__dirname + "/sparql/" + serviceName + "/" + serviceMethod + ".sparql", 'utf8', function(err, data) {

			//get sparql query

			try {
				var result = {};
				/*
				console.time("getSparqlResource");				
				if (serviceConfig['query'] !== undefined) {
					result = thenRequest('GET', serviceConfig['query']);
				} else {
					result = thenRequest('GET', resourceRoot + "sparql/" + serviceName + "/" + serviceMethod + ".sparql");
				}
				result.then(function(resSparql) {
					console.timeEnd("getSparqlResource");
					data = resSparql.getBody('UTF-8');
					var query = data;
				*/
				var query = serviceConfig['resQuery'];
				//Custom query function
				if (serviceConfig.customized) {
					customQuery = new CustomQuery();
					query = customQuery[serviceConfig['function']](serviceConfig, params, query);
				} else {
					//Parse the rdfSql first and inject parameters:query:
					sqlParser = new SqlParser(query, params, serviceConfig);
					//			async.series([function() {
					sqlParser.replace();
					//log.debug('rdf query ' + sqlParser.getRdfSql());
					query = sqlParser.getRdfSql();
					//			}, function() {}]);
				}
				//log.debug('rdf query ' + query);
				var offset = 0;
				if (params['offset'] !== undefined) {
					offset = params['offset'];
					query = query + " OFFSET " + params['offset'];
				}
				if (params['limit'] !== undefined) {
					query = query + " LIMIT " + params['limit'];
				} else {
					query = query + " LIMIT " + limit;
				}

				log.debug('rdf query ' + query);

				sqlPorter = new PostCode(host, defaultGraphUrl, query, shouldSponge, format, timeout, debug);
				console.time("postQuery");
				body = sqlPorter.postQuery();
				log.debug("body: " + JSON.stringify(body));
				body.then(function(resBody) {
						//parse result into jsonConfig
						//try {
						console.timeEnd("postQuery");
						//log.debug("virtuoso: " + resBody.getBody('UTF-8'));
						var jsonldBody = JSON.parse(resBody.getBody('UTF-8'));
						postPagingDefer = Q.defer();
						log.debug("pagingDefer " + postPagingDefer);

						//Paging handler
						if (serviceConfig.paging !== undefined) {
							//log.debug("jsonldBody: "+JSON.stringify(jsonldBody));
							//log.debug("enter paging " + jsonldBody["@graph"][0][serviceConfig.paging.field] + " limit: "+limit);
							tracePage = function(serviceConfig,pOffset,pLimit){
								var tracePagePromise = Q.defer();
								pageQuery = serviceConfig.resPaging;
								pOffset = pOffset + pLimit + 1;								
								log.debug("offset: "+pOffset+" limit: "+pLimit);
								pageQuery = pageQuery + " OFFSET " + pOffset + " LIMIT " + pLimit;
								sqlParser = new SqlParser(pageQuery, params, serviceConfig);
								sqlParser.replace();
								pageQuery = sqlParser.getRdfSql();
								log.debug("query: \n"+pageQuery);
															
								pagedSqlPoster = new PostCode(host, defaultGraphUrl, pageQuery, shouldSponge, format, timeout, debug);
								pagedBody = pagedSqlPoster.postQuery();
								pagedBody.then(function(resPaged) {
									jsonPaged = JSON.parse(resPaged.getBody('UTF-8'));
									if (jsonPaged["@graph"]!==undefined&&jsonPaged["@graph"][0][serviceConfig.paging.field] !== undefined) {
										var objectPaged = jsonPaged["@graph"][0][serviceConfig.paging.field];
										nextPage = tracePage(serviceConfig,pOffset,pLimit);
										nextPage.then(function(resPage){
											log.debug("respage offset "+pOffset+" length: "+resPage.length);								
											objectPaged.push.apply(objectPaged,resPage);
											log.debug("object offset "+pOffset+" length: "+objectPaged.length);
											tracePagePromise.resolve(objectPaged);
										});										
									} else {
										log.debug("empty set");
										tracePagePromise.resolve([]);
									}
								},function(err){
									log.debug(err);
									tracePagePromise.resolve([]);
								});
								log.debug("ready to return promise");
								return tracePagePromise.promise;
							}
							
							if (jsonldBody["@graph"]!==undefined&&jsonldBody["@graph"][0][serviceConfig.paging.field].length === limit) {							
								tracePage(serviceConfig,offset,limit).then(function(resPage){
									log.debug("object length: "+resPage.length);
									jsonldBody["@graph"][0][serviceConfig.paging.field].push.apply(jsonldBody["@graph"][0][serviceConfig.paging.field],resPage);
									log.debug("finish pushing new graph "+jsonldBody["@graph"][0][serviceConfig.paging.field].length);
									postPagingDefer.resolve(jsonldBody);
								})
							}else{
								postPagingDefer.resolve(jsonldBody);
							}
								
							/*
							if (jsonldBody["@graph"][0][serviceConfig.paging.field].length === limit) {
								//reach limit
								log.debug("body reach limit");
								var objectPaged = ["a"];
								while (objectPaged.length > 0) {
									offset = offset + limit + 1;
									log.debug("object Length: " + objectPaged.length);
									log.debug("enter paging " + jsonldBody["@graph"][0][serviceConfig.paging.field].length + "offset: " + offset + " limit: " + limit);
									pageQuery = serviceConfig.resPaging;
									pageQuery = pageQuery + " OFFSET " + offset + " LIMIT " + limit;
									sqlParser = new SqlParser(pageQuery, params, serviceConfig);
									sqlParser.replace();
									pageQuery = sqlParser.getRdfSql();

									log.debug("pageQuery: " + pageQuery);
									pagedSqlPoster = new PostCode(host, defaultGraphUrl, pageQuery, shouldSponge, format, timeout, debug);
									pagedBody = pagedSqlPoster.postQuery();
									pagedBody.then(function(resPaged) {
										log.debug(resPaged.getBody('UTF-8'));
										jsonPaged = JSON.parse(resPaged.getBody('UTF-8'));
										log.debug("jsongPaged: " + JSON.stringify(jsonPaged));
										if (jsonPaged["@graph"][0][serviceConfig.paging.field] !== undefined) {
											objectPaged = jsonPaged["@graph"][0][serviceConfig.paging.field];
											jsonldBody["@graph"][0][serviceConfig.paging.field].push(objectPaged);
										} else {
											objectPaged = [];
										}
									});
								}
								postPagingDefer.resolve(jsonldBody);
							}
							*/
							
						}else{
							postPagingDefer.resolve(jsonldBody);		
						}
						
						postPagingDefer.promise.then(function(resPage) {
							if (jsonConfig.services[serviceName][serviceMethod]['context'] !== undefined) {
								/*
								do flatten and compacting jsonld
								*/
//								res.end(JSON.stringify(resPage));
//								return;
								promises = jsonld.promises;
								console.time("compactandcontext");
								log.debug("starting to refresh context");
								promise = promises.compact(resPage, serviceConfig['context']);
								//promise = promises.compact(jsonldBody, serviceConfig['context']);
								promise.then(function(compacted) {
									console.timeEnd("compactandcontext");
									//log.debug("compacted: " + JSON.stringify(compacted));
									res.end(JSON.stringify(compacted));
									return;
									/*
									console.time("flatten");
									flatenPromise = promises.flatten(compacted);
									flatenPromise.then(function(flatened) {
										console.timeEnd("flatten");
										res.end(JSON.stringify(flatened));
									}, function(err) {
										throw err;
									})
									*/
								}, function(err) {
									throw err;
								});
							} else {
								res.end(JSON.stringify(resPage));
								return;
							}
						})
					})
					/*
						}, function(errSparql) {
							throw errSparql;
						});
						*/
			} catch (err) {
				log.error(err);
				errorStatus.message = err.message;
				res.end(JSON.stringify(errorStatus));
				return;
			}

			//			})
		} else {
			//the request is for frame
			log.debug("parent method: " + jsonConfig.services[serviceName][serviceMethod]['parentMethod']);
			log.debug("query string: " + JSON.stringify(queryString));
			/*
			body = syncRequest('GET', jsonConfig.services[serviceName][serviceMethod]['parentMethod'], {
				qs: queryString
			});
			res.end(body.getBody());
			*/
			/*
			get_options = {
				url: jsonConfig.services[serviceName][serviceMethod]['parentMethod'],
				qs: queryString,
			};
				
			var post_req = request.get(get_options, function(error, response, body) {
				//				log.debug(body);
				jsonldBody = JSON.parse(body);
				if (jsonldBody.errorcode === "-1") {
					res.end(body);
				}
				promises = jsonld.promises;
				//get the frame document from url
				//			log.debug('frame: '+ JSON.stringify(jsonConfig.services[serviceName][serviceMethod]));
				//			frameBody = syncRequest('GET', jsonConfig.services[serviceName][serviceMethod]['frame']);
				//			frame = JSON.parse(frameBody.getBody('UTF-8'));
				//promise = promises.frame(jsonldBody, jsonConfig.services[serviceName][serviceMethod]['frame']);
				promise = promises.frame(jsonldBody, serviceConfig['resFrame']);
				promise.then(function(framed) {
						res.end(JSON.stringify(framed))
					}, function(error) {
						log.debug(error);
						throw error;
					})
					//		response.setEncoding('utf8');
					//		response.on('data', function(chunk) {
					//			log.debug('Response: ' + chunk);
					//		});
			});
			*/

			//asynchronous get request
			resultParent = thenRequest('GET', jsonConfig.services[serviceName][serviceMethod]['parentMethod'], {
				qs: queryString
			});

			resultParent.then(function(resJson) {
				//				log.debug(body);
				jsonldBodyFrame = JSON.parse(resJson.getBody('UTF-8'));
				if (jsonldBodyFrame.errorcode === "-1") {
					res.end(body);
					return;
				}
				jsonldFrame = require('jsonld');
				//framePromises = jsonldFrame.promises;
				log.debug("resFrame: " + serviceConfig['frame']);
				//get the frame document from url
				//			log.debug('frame: '+ JSON.stringify(jsonConfig.services[serviceName][serviceMethod]));
				//			frameBody = syncRequest('GET', jsonConfig.services[serviceName][serviceMethod]['frame']);
				//			frame = JSON.parse(frameBody.getBody('UTF-8'));
				//promise = promises.frame(jsonldBody, jsonConfig.services[serviceName][serviceMethod]['frame']);
				console.time("frame");
				log.debug("bodyFrame: " + jsonldBodyFrame);
				jsonldFrame.frame(jsonldBodyFrame, serviceConfig['frame'], function(err, framed) {
					// document transformed into a particular tree structure per the given frame 
					console.timeEnd("frame");
					log.debug("framing finished");
					if (err) {
						log.error(err);
					}
					res.end(JSON.stringify(framed));
				});
				/*
				framePromise = framePromises.frame(jsonldBodyFrame, serviceConfig['frame']);
				framePromise.then(function(framed) {
						log.debug("framing finished");
						console.timeEnd("frame");
						res.end(JSON.stringify(framed));
						return;
					}, function(error) {
						log.error(error);
						throw error;
					});
				*/
				//		response.setEncoding('utf8');
				//		response.on('data', function(chunk) {
				//			log.debug('Response: ' + chunk);
				//		});
			});



		}
		//}])
		;
	} catch (err) {
		log.error(err);
		errorStatus.message = err.message;
		res.end(JSON.stringify(errorStatus));
		return;
	}

})

//Run the server
var server = app.listen(8080, function() {
	var host = server.address().address;
	var port = server.address().port;
	server.maxConnections = 100;

	log.debug("Sparql API listening at http://%s:%s", host, port)
})


//pre defined custom function
function CustomQuery() {

}

CustomQuery.prototype.listCustom = function(config, param, query) {
	if (param['vis'] == undefined) {
		throw Error("parameter vis is missing");
	}
	condition = " VALUES ( ";
	values = "{ ( ";
	condition += " ?vis ";
	values += " \"" + param['vis'] + "\" ";
	if (param['creator'] !== undefined) {
		condition += " ?cre ";
		values += " \"" + param['creator'] + "\" ";
	}
	if (param['group'] !== undefined) {
		condition += " ?group ";
		values += " \"" + param['group'] + "\" ";
	}
	condition += " ) "
	values += " ) }"
	return query + condition + values + " }"
}

//SqlParser class
//class to parse sql query and replace particular tags with given parameters set
function SqlParser(rdfSql, params, config) {
	this.beginTag = ':=';
	this.endTag = '=:';
	this.rdfSql = rdfSql;
	this.params = params;
	this.config = config;
}

SqlParser.prototype.tagit = function(myString) {
	return this.beginTag + myString + this.endTag;
}

SqlParser.prototype.getRdfSql = function() {
	return this.rdfSql;
}

SqlParser.prototype.replace = function() {
	self = this;
	Object.keys(this.params).forEach(function(key) {
		value = self.params[key];
		if (self.config[key] !== undefined) {
			transform = self.config[key]['transform'];
			self.rdfSql = self.rdfSql.replace(transform, value)
		}
	});

}

/*
SqlParser.prototype.replace = function() {
	//var re = new RegExp(this.beginTag+'(.*)'+this.endTag);	
	//var r  = this.rdfSql.match(re);
	splitWindow = this.rdfSql.split(this.beginTag);
	if (splitWindow.length > 1) {
		splitParam = splitWindow[1].split(this.endTag);
		if (splitParam.length > 0) {
			log.debug("split param: " + splitParam[0]);
			if (typeof(params[splitParam[0]]) == "undefined") {
				throw Error("Parameter " + splitParam[0] + " is not given when calling the api");
			}
			this.rdfSql = this.rdfSql.replace(this.tagit(splitParam[0]), params[splitParam[0]]);
		}
		//recursive for replacing the tag
		this.replace();
	} else {
		//		log.debug(this.rdfSql);
	}
}
*/

//PostCode class
//class to post request into virtuoso api
function PostCode(host, defaultGraphUrl, query, shouldSponge, format, timeout, debug) {
	this.host = host;
	this.defaultGraphUrl = defaultGraphUrl;
	this.query = query;
	this.shouldSponge = shouldSponge;
	this.format = format;
	this.timeout = timeout;
	this.debug = debug;
}

PostCode.prototype.postQuery = function() {
	// Build the post string from an object
	post_data = querystring.stringify({
		'default-graph-uri': this.defaultGraphUrl,
		'query': this.query,
		'should-sponge': this.shouldSponge,
		'format': this.format,
		'timeout': this.debug
	});


	queryString = {
		'default-graph-uri': this.defaultGraphUrl,
		'query': this.query,
		//		'should-sponge': this.shouldSponge,
		'format': this.format,
		'timeout': this.timeout,
		'debug': this.debug
	}

	// An object of options to indicate where to post to

	post_options = {
		host: this.host,
		port: '8890',
		path: '/sparql',
		method: 'POST',
		headers: {
			'Content-Type': 'application/x-www-form-urlencoded'
		}
	}

	get_options = {
		url: 'http://' + this.host + ':8890/sparql',
		qs: queryString,
	};


	// asynchronous get request, not used
	//	var post_req = request.get(get_options, function(error,response,body) {
	//		myBody = body;
	//log.debug(body);
	//		response.setEncoding('utf8');
	//		response.on('data', function(chunk) {
	//			log.debug('Response: ' + chunk);
	//		});
	//	});

	// Synchronous get request
	localThenRequest = require('then-request');
	return localThenRequest('POST', 'http://' + this.host + ':8890/sparql', {
		qs: queryString
	});

}
