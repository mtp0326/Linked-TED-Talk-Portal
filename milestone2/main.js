/**
 * 
 */

var port = 8081;

const express = require('express');
const app = new express();
const path = require("path");
const JSON5 = require("json5")
const stemmer = require("stemmer")
const pug = require('pug');

const AWS = require("aws-sdk");

const {DynamoDB, QueryCommand} = require('@aws-sdk/client-dynamodb-v2-node');

AWS.config.update({region:'us-east-1'});

const client = new AWS.DynamoDB();

app.set("view engine", "pug");
app.set("views", path.join(__dirname, "views"))

app.get('/', function(request, response) {
    response.sendFile('html/index.html', { root: __dirname });
});

app.get('/talks1', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	
	var promiseArr = new Array;
	var keywordArr = new Array;
	var keywordItemIDSet = new Set;
	var resultsTalks = new Set;
	keywordArr = request.query.keyword.split(" ");
	
	//Stem keyword and apply params
	for(let i = 0; i < keywordArr.length; i++) {
		keywordArr[i] = stemmer(keywordArr[i].toLowerCase());
		
		// Set the parameters
		const params = {
			KeyConditionExpression: "keyword = :k",
			ExpressionAttributeValues: {
			    ":k": keywordArr[i],
			},
			TableName: "inverted",
		};
		
		promiseArr[i] = docClient.query(params).promise();
	}
		
	//Add the right up to 15 keywords and corresponding items with up to 15 inxids in a set, iterating through each successful keyword
	Promise.all(promiseArr).then(
		successfulDataArray => {
			var wordCount = 0;
			while(wordCount < 15 && successfulDataArray.length > wordCount && keywordItemIDSet.size < 15) {
				var itemCount = 0;
				while(successfulDataArray[wordCount].Items.length > itemCount && keywordItemIDSet.size < 15) {
					if(!keywordItemIDSet.has(successfulDataArray[wordCount].Items[itemCount].inxid)) {
						keywordItemIDSet.add(successfulDataArray[wordCount].Items[itemCount].inxid)
					}
					itemCount++;
				}
				wordCount++;
			}
			for(let item of keywordItemIDSet) {
				// Set the parameters
				const params = {
					KeyConditionExpression: "talk_id = :k",
					ExpressionAttributeValues: {
		    			":k": item,
					},
					TableName: "ted_talks",
				};
				resultsTalks.add(docClient.query(params).promise());
			}
			
			Promise.all(resultsTalks).then(
				
				//Collect all info from 1 - 15 resulting talks into render()
				successfulTalksArr => {
					var printArr = new Array;
					let printArrIndex = 0;
					for(let sTalk of successfulTalksArr) {
						let regex = /(\d+)/g
						sTalk.Items[0].topics = JSON5.parse(sTalk.Items[0].topics)
						sTalk.Items[0].related_talks = JSON5.parse(sTalk.Items[0].related_talks.replace(regex, "\"$&\"").replace(/('s)+/g, ""))
						sTalk.Items[0].url = sTalk.Items[0].url.replace("www.ted.com", "embed.ted.com")
						
						printArr[printArrIndex] = sTalk.Items[0];
						printArrIndex++;
						console.log(sTalk.Items[0].related_talks)
					}
					
					let topLength = printArr.length;
					response.render("results", { "search": request.query.keyword, "results": printArr, "High": topLength, "Low": "1"});
				},
				
				errorArr => {
					for(let i = 0; i < errorArr.length; i++) {
						console.log(errorArr[i])
					}
				}
			);
		},
		otherErrors => {
			for(let i = 0; i < otherErrors.length; i++) {
				console.log(otherErrors[i])
			}
		}
	);
})

app.get('/talks2', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	
	var promiseArr = new Array;
	var keywordArr = new Array;
	var keywordItemIDSet = new Set;
	var resultsTalks = new Set;
	keywordArr = request.query.keyword.split(" ");
	
	//Stem keyword and apply params
	for(let i = 0; i < keywordArr.length; i++) {
		keywordArr[i] = stemmer(keywordArr[i].toLowerCase());
		
		// Set the parameters
		const params = {
			KeyConditionExpression: "keyword = :k",
			ExpressionAttributeValues: {
			    ":k": keywordArr[i],
			},
			TableName: "inverted",
		};
		
		promiseArr[i] = docClient.query(params).promise();
	}
		
	//Add the right up to 15 keywords and corresponding items with up to 15 inxids in a set, iterating through each successful keyword
	Promise.all(promiseArr).then(
		successfulDataArray => {
			var wordCount = 0;
			var totalCount = 0;
			while(wordCount < 15 && successfulDataArray.length > wordCount && keywordItemIDSet.size < 15) {
				var itemCount = 0;
				while(successfulDataArray[wordCount].Items.length > itemCount && keywordItemIDSet.size < 15) {
					if(!keywordItemIDSet.has(successfulDataArray[wordCount].Items[itemCount].inxid)) {
						totalCount++;
						if(totalCount >= 15 && totalCount < 30) {
							keywordItemIDSet.add(successfulDataArray[wordCount].Items[itemCount].inxid)
						}
					}
					itemCount++;
				}
				wordCount++;
			}
			for(let item of keywordItemIDSet) {
				// Set the parameters
				const params = {
					KeyConditionExpression: "talk_id = :k",
					ExpressionAttributeValues: {
		    			":k": item,
					},
					TableName: "ted_talks",
				};
				resultsTalks.add(docClient.query(params).promise());
			}
			
			Promise.all(resultsTalks).then(
				
				//Collect all info from 16 - 30 resulting talks into render()
				successfulTalksArr => {
					var printArr = new Array;
					let printArrIndex = 0;
					for(let sTalk of successfulTalksArr) {
						let regex = /(\d+)/g
						sTalk.Items[0].topics = JSON5.parse(sTalk.Items[0].topics)
						sTalk.Items[0].related_talks = JSON5.parse(sTalk.Items[0].related_talks.replace(regex, "\"$&\"").replace(/('s)+/g, ""))
						sTalk.Items[0].url = sTalk.Items[0].url.replace("www.ted.com", "embed.ted.com")
						
						printArr[printArrIndex] = sTalk.Items[0];
						printArrIndex++;
						console.log(sTalk.Items[0].related_talks)
					}
					
					let topLength = 15 + printArr.length;
					response.render("results", { "search": request.query.keyword, "results": printArr, "High": topLength, "Low": "16"});
				},
				
				errorArr => {
					for(let i = 0; i < errorArr.length; i++) {
						console.log(errorArr[i])
					}
				}
			);
		},
		otherErrors => {
			for(let i = 0; i < otherErrors.length; i++) {
				console.log(otherErrors[i])
			}
		}
	);
})

app.get('/talks3', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	
	var promiseArr = new Array;
	var keywordArr = new Array;
	var keywordItemIDSet = new Set;
	var resultsTalks = new Set;
	keywordArr = request.query.keyword.split(" ");
	
	//Stem keyword and apply params
	for(let i = 0; i < keywordArr.length; i++) {
		keywordArr[i] = stemmer(keywordArr[i].toLowerCase());
		
		// Set the parameters
		const params = {
			KeyConditionExpression: "keyword = :k",
			ExpressionAttributeValues: {
			    ":k": keywordArr[i],
			},
			TableName: "inverted",
		};
		
		promiseArr[i] = docClient.query(params).promise();
	}
		
	//Add the right up to 15 keywords and corresponding items with up to 15 inxids in a set, iterating through each successful keyword
	Promise.all(promiseArr).then(
		successfulDataArray => {
			var wordCount = 0;
			var totalCount = 0;
			while(wordCount < 15 && successfulDataArray.length > wordCount && keywordItemIDSet.size < 15) {
				var itemCount = 0;
				while(successfulDataArray[wordCount].Items.length > itemCount && keywordItemIDSet.size < 15) {
					if(!keywordItemIDSet.has(successfulDataArray[wordCount].Items[itemCount].inxid)) {
						totalCount++;
						if(totalCount >= 30 && totalCount < 45) {
							keywordItemIDSet.add(successfulDataArray[wordCount].Items[itemCount].inxid)
						}
					}
					itemCount++;
				}
				wordCount++;
			}
			for(let item of keywordItemIDSet) {
				// Set the parameters
				const params = {
					KeyConditionExpression: "talk_id = :k",
					ExpressionAttributeValues: {
		    			":k": item,
					},
					TableName: "ted_talks",
				};
				resultsTalks.add(docClient.query(params).promise());
			}
			
			Promise.all(resultsTalks).then(
				
				//Collect all info from 31 - 45 resulting talks into render()
				successfulTalksArr => {
					var printArr = new Array;
					let printArrIndex = 0;
					for(let sTalk of successfulTalksArr) {
						let regex = /(\d+)/g
						sTalk.Items[0].topics = JSON5.parse(sTalk.Items[0].topics)
						sTalk.Items[0].related_talks = JSON5.parse(sTalk.Items[0].related_talks.replace(regex, "\"$&\"").replace(/('s)+/g, ""))
						sTalk.Items[0].url = sTalk.Items[0].url.replace("www.ted.com", "embed.ted.com")
						
						printArr[printArrIndex] = sTalk.Items[0];
						printArrIndex++;
						console.log(sTalk.Items[0].related_talks)
					}
					
					let topLength = 30 + printArr.length;
					response.render("results", { "search": request.query.keyword, "results": printArr, "High": topLength, "Low": "31"});
				},
				
				errorArr => {
					for(let i = 0; i < errorArr.length; i++) {
						console.log(errorArr[i])
					}
				}
			);
		},
		otherErrors => {
			for(let i = 0; i < otherErrors.length; i++) {
				console.log(otherErrors[i])
			}
		}
	);
})

app.get('/talks4', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	
	var promiseArr = new Array;
	var keywordArr = new Array;
	var keywordItemIDSet = new Set;
	var resultsTalks = new Set;
	keywordArr = request.query.keyword.split(" ");
	
	//Stem keyword and apply params
	for(let i = 0; i < keywordArr.length; i++) {
		keywordArr[i] = stemmer(keywordArr[i].toLowerCase());
		
		// Set the parameters
		const params = {
			KeyConditionExpression: "keyword = :k",
			ExpressionAttributeValues: {
			    ":k": keywordArr[i],
			},
			TableName: "inverted",
		};
		
		promiseArr[i] = docClient.query(params).promise();
	}
		
	//Add the right up to 15 keywords and corresponding items with up to 15 inxids in a set, iterating through each successful keyword
	Promise.all(promiseArr).then(
		successfulDataArray => {
			var wordCount = 0;
			var totalCount = 0;
			while(wordCount < 15 && successfulDataArray.length > wordCount && keywordItemIDSet.size < 15) {
				var itemCount = 0;
				while(successfulDataArray[wordCount].Items.length > itemCount && keywordItemIDSet.size < 15) {
					if(!keywordItemIDSet.has(successfulDataArray[wordCount].Items[itemCount].inxid)) {
						totalCount++;
						if(totalCount >= 45 && totalCount < 60) {
							keywordItemIDSet.add(successfulDataArray[wordCount].Items[itemCount].inxid)
						}
					}
					itemCount++;
				}
				wordCount++;
			}
			for(let item of keywordItemIDSet) {
				// Set the parameters
				const params = {
					KeyConditionExpression: "talk_id = :k",
					ExpressionAttributeValues: {
		    			":k": item,
					},
					TableName: "ted_talks",
				};
				resultsTalks.add(docClient.query(params).promise());
			}
			
			Promise.all(resultsTalks).then(
				
				//Collect all info from 46 - 60 resulting talks into render()
				successfulTalksArr => {
					var printArr = new Array;
					let printArrIndex = 0;
					for(let sTalk of successfulTalksArr) {
						let regex = /(\d+)/g
						sTalk.Items[0].topics = JSON5.parse(sTalk.Items[0].topics)
						sTalk.Items[0].related_talks = JSON5.parse(sTalk.Items[0].related_talks.replace(regex, "\"$&\"").replace(/('s)+/g, ""))
						sTalk.Items[0].url = sTalk.Items[0].url.replace("www.ted.com", "embed.ted.com")
						
						printArr[printArrIndex] = sTalk.Items[0];
						printArrIndex++;
						console.log(sTalk.Items[0].related_talks)
					}
					
					let topLength = 45 + printArr.length;
					response.render("results", { "search": request.query.keyword, "results": printArr, "High": topLength, "Low": "46"});
				},
				
				errorArr => {
					for(let i = 0; i < errorArr.length; i++) {
						console.log(errorArr[i])
					}
				}
			);
		},
		otherErrors => {
			for(let i = 0; i < otherErrors.length; i++) {
				console.log(otherErrors[i])
			}
		}
	);
})

app.get('/talks5', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	
	var promiseArr = new Array;
	var keywordArr = new Array;
	var keywordItemIDSet = new Set;
	var resultsTalks = new Set;
	keywordArr = request.query.keyword.split(" ");
	
	//Stem keyword and apply params
	for(let i = 0; i < keywordArr.length; i++) {
		keywordArr[i] = stemmer(keywordArr[i].toLowerCase());
		
		// Set the parameters
		const params = {
			KeyConditionExpression: "keyword = :k",
			ExpressionAttributeValues: {
			    ":k": keywordArr[i],
			},
			TableName: "inverted",
		};
		
		promiseArr[i] = docClient.query(params).promise();
	}
		
	//Add the right up to 15 keywords and corresponding items with up to 15 inxids in a set, iterating through each successful keyword
	Promise.all(promiseArr).then(
		successfulDataArray => {
			var wordCount = 0;
			var totalCount = 0;
			while(wordCount < 15 && successfulDataArray.length > wordCount && keywordItemIDSet.size < 15) {
				var itemCount = 0;
				while(successfulDataArray[wordCount].Items.length > itemCount && keywordItemIDSet.size < 15) {
					if(!keywordItemIDSet.has(successfulDataArray[wordCount].Items[itemCount].inxid)) {
						totalCount++;
						if(totalCount >= 60 && totalCount < 75) {
							keywordItemIDSet.add(successfulDataArray[wordCount].Items[itemCount].inxid)
						}
					}
					itemCount++;
				}
				wordCount++;
			}
			for(let item of keywordItemIDSet) {
				// Set the parameters
				const params = {
					KeyConditionExpression: "talk_id = :k",
					ExpressionAttributeValues: {
		    			":k": item,
					},
					TableName: "ted_talks",
				};
				resultsTalks.add(docClient.query(params).promise());
			}
			
			Promise.all(resultsTalks).then(
				
				//Collect all info from 61 - 75 resulting talks into render()
				successfulTalksArr => {
					var printArr = new Array;
					let printArrIndex = 0;
					for(let sTalk of successfulTalksArr) {
						let regex = /(\d+)/g
						sTalk.Items[0].topics = JSON5.parse(sTalk.Items[0].topics)
						sTalk.Items[0].related_talks = JSON5.parse(sTalk.Items[0].related_talks.replace(regex, "\"$&\"").replace(/('s)+/g, ""))
						sTalk.Items[0].url = sTalk.Items[0].url.replace("www.ted.com", "embed.ted.com")
						
						printArr[printArrIndex] = sTalk.Items[0];
						printArrIndex++;
						console.log(sTalk.Items[0].related_talks)
					}
					
					let topLength = 60 + printArr.length;
					response.render("results", { "search": request.query.keyword, "results": printArr, "High": topLength, "Low": "61"});
				},
				
				errorArr => {
					for(let i = 0; i < errorArr.length; i++) {
						console.log(errorArr[i])
					}
				}
			);
		},
		otherErrors => {
			for(let i = 0; i < otherErrors.length; i++) {
				console.log(otherErrors[i])
			}
		}
	);
})

app.get('/talk', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	const init = parseInt(request.query.id);
	let results = new Array;
	
	// Set the parameters
	const params = {
		KeyConditionExpression: "talk_id = :k",
		ExpressionAttributeValues: {
		    ":k": init,
		},
		TableName: "ted_talks",
	};
	
	docClient.query(params, function(err, data) {
		if(err) {
			console.log(err, err.stack)
			results.push("err")
		}
		else {
			//use regex to change the number with "(number)" for results.related_talks and replace the url www. with embed.
			let regex = /(\d+)/g
			results.push(data.Items[0])
			results[0].topics = JSON5.parse(results[0].topics)
			results[0].related_talks = JSON5.parse(results[0].related_talks.replace(regex, "\"$&\""))
			results[0].url = results[0].url.replace("www.ted.com", "embed.ted.com")
			
			console.log(results[0].related_talks)
		
			response.render("results", { "search": init, "results": results })
		}
	})
});


app.get('/talks', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	
	var promiseArr = new Array;
	var keywordArr = new Array;
	var keywordItemIDSet = new Set;
	var resultsTalks = new Set;
	keywordArr = request.query.keyword.split(" ");
	
	//Stem keyword and apply params
	for(let i = 0; i < keywordArr.length; i++) {
		keywordArr[i] = stemmer(keywordArr[i].toLowerCase());
		
		// Set the parameters
		const params = {
			KeyConditionExpression: "keyword = :k",
			ExpressionAttributeValues: {
			    ":k": keywordArr[i],
			},
			TableName: "inverted",
		};
		
		promiseArr[i] = docClient.query(params).promise();
	}
		
	//Add the right up to 15 keywords and corresponding items with up to 15 inxids in a set, iterating through each successful keyword
	Promise.all(promiseArr).then(
		successfulDataArray => {
			var wordCount = 0;
			while(wordCount < 15 && successfulDataArray.length > wordCount && keywordItemIDSet.size < 15) {
				var itemCount = 0;
				while(successfulDataArray[wordCount].Items.length > itemCount && keywordItemIDSet.size < 15) {
					if(!keywordItemIDSet.has(successfulDataArray[wordCount].Items[itemCount].inxid)) {
						keywordItemIDSet.add(successfulDataArray[wordCount].Items[itemCount].inxid)
					}
					itemCount++;
				}
				wordCount++;
			}
			for(let item of keywordItemIDSet) {
				// Set the parameters
				const params = {
					KeyConditionExpression: "talk_id = :k",
					ExpressionAttributeValues: {
		    			":k": item,
					},
					TableName: "ted_talks",
				};
				resultsTalks.add(docClient.query(params).promise());
			}
			
			Promise.all(resultsTalks).then(
				
				//Collect all info from the 15 resulting talks into render()
				successfulTalksArr => {
					var printArr = new Array;
					let printArrIndex = 0;
					for(let sTalk of successfulTalksArr) {
						let regex = /(\d+)/g
						sTalk.Items[0].topics = JSON5.parse(sTalk.Items[0].topics)
						sTalk.Items[0].related_talks = JSON5.parse(sTalk.Items[0].related_talks.replace(regex, "\"$&\"").replace(/('s)+/g, ""))
						sTalk.Items[0].url = sTalk.Items[0].url.replace("www.ted.com", "embed.ted.com")
						
						printArr[printArrIndex] = sTalk.Items[0];
						printArrIndex++;
						console.log(sTalk.Items[0].related_talks)
					}
					
					let topLength = printArr.length;
					response.render("results", { "search": request.query.keyword, "results": printArr, "High": topLength, "Low": "1"});
				},
				
				errorArr => {
					for(let i = 0; i < errorArr.length; i++) {
						console.log(errorArr[i])
					}
				}
			);
		},
		otherErrors => {
			for(let i = 0; i < otherErrors.length; i++) {
				console.log(otherErrors[i])
			}
		}
	);
})

app.get('/talk', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	const init = parseInt(request.query.id);
	let results = new Array;
	
	// Set the parameters
	const params = {
		KeyConditionExpression: "talk_id = :k",
		ExpressionAttributeValues: {
		    ":k": init,
		},
		TableName: "ted_talks",
	};
	
	docClient.query(params, function(err, data) {
		if(err) {
			console.log(err, err.stack)
			results.push("err")
		}
		else {
			//use regex to change the number with "(number)" for results.related_talks and replace the url www. with embed.
			let regex = /(\d+)/g
			results.push(data.Items[0])
			results[0].topics = JSON5.parse(results[0].topics)
			results[0].related_talks = JSON5.parse(results[0].related_talks.replace(regex, "\"$&\"").replace(/('s)+/g, ""))
			results[0].url = results[0].url.replace("www.ted.com", "embed.ted.com")
			
			console.log(results[0].related_talks)
		
			response.render("results", { "search": init, "results": results })
		}
	})

})

app.listen(port, () => {
  console.log(`HW2 app listening at http://localhost:${port}`)
})