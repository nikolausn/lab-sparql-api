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
var syncRequest = require('sync-request');
//Library to call async request
var async = require('async');
//array, collection and text library
var _ = require('underscore');
//jsonld library
var jsonld = require('jsonld');
//load jsonConfig
var jsonConfig = require(__dirname + "/config-hashed.json");


app.get("/:serviceName/:serviceMethod", function(req, res) {
	// set content-type based on output parameter
	var serviceName = req.params.serviceName;
	var serviceMethod = req.params.serviceMethod;
	//Trap for unconfig servicename
	if (typeof(jsonConfig.services[serviceName]) == "undefined") {
		console.log("undefined serviceName");
		throw Error("service " + serviceName + " is undefined");
	}
	//Trap for unconfig servicemethod
	if (typeof(jsonConfig.services[serviceName][serviceMethod]) == "undefined") {
		console.log("undefined serviceMethod")
		throw Error("service method " + serviceMethod + " for " + serviceName + " is undefined");
	}

	var queryString = url.parse(req.url, true).query;
	//console.log(queryString);
	requiredParam = jsonConfig.services[serviceName][serviceMethod]["required"];
	optionalParam = jsonConfig.services[serviceName][serviceMethod]["optional"];
	console.log(requiredParam);
	format = "application/x-json+ld";
	output = "application/ld+json"
	params = {};
	//Parse url parameters , check if there is some parameters that is not allowed
	Object.keys(queryString).forEach(function(key) {
		if (!_.contains(requiredParam, key) && !_.contains(optionalParam, key)) {
			throw Error("parameter " + key + " is not allowed");
		}
		var val = queryString[key];
		//Change to params[key] only
		params[key] = val;
	});
	console.log('params: ' + params);
	console.log('format: ' + format)
		//Set response / result header into requested format
	res.setHeader('content-type', output);

	host = "acbres224.ischool.illinois.edu";
	defaultGraphUrl = "";
	shouldSponge = "";
	timeout = 0;
	debug = "on";
	query = "";
	console.log("my Config: " + JSON.stringify(jsonConfig));
	var body = {};
	//	async.series([function() {
	//if not framed then the sparql file template should be there
	if (typeof(jsonConfig.services[serviceName][serviceMethod]['frame']) == "undefined") {
		fs.readFile(__dirname + "/sparql/" + serviceName + "/" + serviceMethod + ".sparql", 'utf8', function(err, data) {
			if (err) {
				throw err;
			}
			var query = data;
			//Parse the rdfSql first and inject parameters
			sqlParser = new SqlParser(query, params);
			//			async.series([function() {
			sqlParser.replace();
			//console.log('rdf query '+sqlParser.getRdfSql());
			query = sqlParser.getRdfSql();
			//			}, function() {}]);
			console.log('rdf query ' + query);

			sqlPorter = new PostCode(host, defaultGraphUrl, query, shouldSponge, format, timeout, debug);
			body = sqlPorter.postQuery();
			//parse result into json
			try{
				var jsonldBody = JSON.parse(body.getBody('UTF-8'));
			}catch(err){
				console.log(err);
			}
				

			/*
			do flatten and compacting jsonld
			*/
			promises = jsonld.promises;
			promise = promises.compact(jsonldBody, jsonConfig.services[serviceName][serviceMethod]['context']);
			promise.then(function(compacted) {
				console.log("compacted: " + JSON.stringify(compacted, null, 2));
				res.end(JSON.stringify(compacted));
				promise = promises.flatten(compacted);
				promise.then(function(flatened) {
					res.end(JSON.stringify(flatened));
				}, function(err) {
					throw err;
				})
			}, function(err) {
				throw err;
			})
		})
	} else {
		//the request is for frame
		console.log("parent method: " + jsonConfig.services[serviceName][serviceMethod]['parentMethod']);
		console.log("query string: " + JSON.stringify(queryString));
		/*
		body = syncRequest('GET', jsonConfig.services[serviceName][serviceMethod]['parentMethod'], {
			qs: queryString
		});
		res.end(body.getBody());
		*/
		get_options = {
			url: jsonConfig.services[serviceName][serviceMethod]['parentMethod'],
			qs: queryString,
		};

		//asynchronous get request
		var post_req = request.get(get_options, function(error, response, body) {
			console.log(body);
			jsonldBody = JSON.parse(body);
			promises = jsonld.promises;
			//get the frame document from url
			//			console.log('frame: '+ JSON.stringify(jsonConfig.services[serviceName][serviceMethod]));
			//			frameBody = syncRequest('GET', jsonConfig.services[serviceName][serviceMethod]['frame']);
			//			frame = JSON.parse(frameBody.getBody('UTF-8'));
			promise = promises.frame(jsonldBody, jsonConfig.services[serviceName][serviceMethod]['frame']);
			promise.then(function(framed) {
					res.end(JSON.stringify(framed))
				}, function(error) {
					console.log(error);
					throw error;
				})
				//		response.setEncoding('utf8');
				//		response.on('data', function(chunk) {
				//			console.log('Response: ' + chunk);
				//		});
		});


	}
	//}])
	;

})

//Run the server
var server = app.listen(8080, function() {
	var host = server.address().address
	var port = server.address().port

	console.log("Sparql API listening at http://%s:%s", host, port)
})

//SqlParser class
//class to parse sql query and replace particular tags with given parameters set
function SqlParser(rdfSql, params) {
	this.beginTag = ':=';
	this.endTag = '=:';
	this.rdfSql = rdfSql;
	this.params = params;
}

SqlParser.prototype.tagit = function(myString) {
	return this.beginTag + myString + this.endTag;
}

SqlParser.prototype.getRdfSql = function() {
	return this.rdfSql;
}

SqlParser.prototype.replace = function() {
	//var re = new RegExp(this.beginTag+'(.*)'+this.endTag);	
	//var r  = this.rdfSql.match(re);
	splitWindow = this.rdfSql.split(this.beginTag);
	if (splitWindow.length > 1) {
		splitParam = splitWindow[1].split(this.endTag);
		if (splitParam.length > 0) {
			console.log("split param: " + splitParam[0]);
			if (typeof(params[splitParam[0]]) == "undefined") {
				throw Error("Parameter " + splitParam[0] + " is not given when calling the api");
			}
			this.rdfSql = this.rdfSql.replace(this.tagit(splitParam[0]), params[splitParam[0]]);
		}
		//recursive for replacing the tag
		this.replace();
	} else {
		//		console.log(this.rdfSql);
	}
}


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
	//console.log(body);
	//		response.setEncoding('utf8');
	//		response.on('data', function(chunk) {
	//			console.log('Response: ' + chunk);
	//		});
	//	});

	// Synchronous get request
	return syncRequest('POST', 'http://' + this.host + ':8890/sparql', {
		qs: queryString
	});

}
