var init = require('init');
var digest = process.argv.indexOf('--digest') > -1;
var db = require('mongojs').connect(digest ? 'api' : init.db, ['analytics', 'abdigest', 'shares']);
var common = require('common');
var services = require('services').connect(init.peer);
var analytics = require('analytics')(services);
var root = require('root');
var app = root();

if (digest) {
	db.client.slaveOk = true;
}

var now = function() {
	return (Date.now() / 1000) | 0;
};
var referer = function(userid, callback) {
	var split = userid.split('-');
	var ref = ((split[0] !== 'user' && split[0] !== 'anon') ? '' : split[2]);

	if (!ref) {
		callback();
		return;
	}

	common.step([
		function(next) {
			db.shares.findOne({ sharename: ref }, { userid: 1 }, next);
		},
		function(share) {
			if (!share) {
				callback();
				return;
			}

			callback(null, share.userid);
		}
	], callback);
};

app.use(root.log(':response.statusCode :request.method :request.url :request.body'));
app.use(root.json);
app.use(root.query);
app.fn('response.ack', function() {
	this.json({ack:true});
});

app.error(function(err, req, res) {
	if (err) return res.json(err);
	res.json(404, 'Do not know what you are looking for');
});

app.branch('auth', function(request, response, next) {
	request.userid = request.query.userid;

	if (!request.userid) {
		response.json(403, 'Microsoft parental control');
		return;
	}

	next();
});
app.branch('internal', function(request, response, next) {
	var ip = request.connection.remoteAddress;

	if (ip.indexOf('10.') !== 0 && ip.indexOf('127.') !== 0 && ip !== '77.66.2.197' && request.query.key !== 'maffe') {
		response.json(403, 'Microsoft parental control');
		return;
	}
	next();
});

app.auth.get('/tests', function(request, response, onerror) {
	common.step([
		function(next) {
			db.analytics.findOne({userid:request.userid}, {'tests.endedevents':1, 'tests.name':1, 'tests.track':1}, next);
		},
		function(user) {
			var tests = (user && user.tests) || [];
			var res = {};

			tests.forEach(function(test) {
				if (test.endedevents) {
					return;
				}

				res[test.name] = test.track;
			})

			response.json(res);
		}
	], onerror);
});

app.auth.post('/tests/create', function(request, response, onerror) {
	var name = request.json.name;
	var track = request.json.track;

	common.step([
		function(next) {
			db.analytics.findAndModify({
				query: {userid:request.userid},
				update: {$set:{userid:request.userid}},
				new: true,
				upsert: true
			}, next);
		},
		function(user, next) {
			db.analytics.update({userid:request.userid, 'tests.name':{$ne:name}}, {$push:{tests:{offsetevents:user.events || {}, name:name, track:track, created:now()}}}, next);
		},
		function() {
			response.ack();
		}
	], onerror);
});

app.auth.post('/tests/track', function(request, response, onerror) {
	var name = request.json.name;
	var count = request.json.count;

	if (!name || !count) return onerror();

	common.step([
		function(next) {
			db.analytics.findAndModify({
				query: {userid:request.userid},
				update: {$set:{userid:request.userid}},
				new: true,
				upsert: true
			}, next);
		},
		function(user) {
			var track;

			(user.tests || []).some(function(test) {
				if (test.name === name) {
					track = test.track;
					return true;
				}
			});

			if (!track) {
				track = Math.floor(Math.random() * count)+1;
				db.analytics.update({
					userid: request.userid,
					'tests.name': {$ne:name}
				}, {
					$push: {tests: {
						name: name,
						track: track,
						created: now(),
						offsetevents: user.events || {}
					}}
				});
			}

			response.json({name:name, track:track});
		}
	], onerror);
});

app.auth.post('/event', function(request, response, onerror) {
	common.step([
		function(next) {
			var map = {};

			map['events.'+request.json.name] = 1;
			db.analytics.update({userid:request.userid}, {$inc:map}, next);
		},
		function() {
			response.ack();
		}
	], onerror);
});

/*app.auth.post('/user/update', function(request, response) {
	db.analytics.update({userid:request.userid}, {$set:request.json}, response.ack.bind(response));
});*/

app.auth.post('/merge', function(request, response, onerror) {
	var mergeuserid = request.json.mergeuserid;
	var userid = request.userid;

	common.step([
		function(next) {
			db.analytics.findOne({userid:mergeuserid}, {_id:1}, next);
		},
		function(exists, next) {
			if (!exists) {
				db.analytics.update({userid:userid}, {$set:{userid:mergeuserid}}, next);
				return;
			}

			db.analytics.update({userid:userid}, {$set:{mergedto:mergeuserid, merged:true}}, next.parallel());
			db.analytics.update({userid:mergeuserid}, {$set:{userid:mergeuserid}}, {upsert:true}, next.parallel());
		},
		function() {
			response.ack();
		}
	], onerror);
});

app.internal.post('/inactive', function(request, response, onerror) {
	var name = request.json.name;

	common.step([
		function(next) {
			db.analytics.find({'tests.name':name}, {events:1, userid:1}, next);
		},
		function(users, next) {
			if (!users.length) {
				response.ack();
				return;
			}

			users.forEach(function(user) {
				db.analytics.update({userid:user.userid, 'tests.name':name}, {$set: {'tests.$.endedevents':user.events||{}}}, next.parallel());
			})
		},
		function() {
			response.ack();
		}
	], onerror);
});

services.subscribe('blob/put', {readyState:'transferring'}, function(file) {
	var userid;

	common.step([
		function(next) {
			db.shares.findOne({sharename:file.sharename}, {userid:1}, next);
		},
		function(share, next) {
			userid = share.userid;

			db.analytics.findAndModify({
				query: {userid:userid},
				update: {$inc:{'events.upload':1}}
			}, next);
		},
		function(user, next) {
			if (!user || (user.events && user.events.upload)) {
				return;
			}

			referer(user.userid, next);
		},
		function(refUserid) {
			if (!refUserid) {
				return;
			}
			
			analytics(refUserid).post('/event', {name:'extUpload'});
		}
	]);
});

services.subscribe('api/download', {selfdownload:false}, function(file) {
	analytics(file.userid).post('/event', {name:'extView'});
});

services.subscribe('api/sendmail', function(mail) {
	analytics(mail.userid).post('/event', {name:'email'});
});

services.subscribe('frontend/signup', function(user) {
	// We postpone this to ensure that its called after potential merges
	// (maybe look into the whole signup/merge flow, to see if this can be fixed architectually)
	setTimeout(function() {
		analytics(user.userid).post('/event', {name:'signup'});

		common.step([
			function(next) {
				db.analytics.update({ userid: user.userid }, { $set: { signupmethod: user.signupmethod } }, next);
			},
			function(next) {
				referer(user.userid, next);
			},
			function(refUserid) {
				if (!refUserid) {
					return;
				}

				analytics(refUserid).post('/event', {name:'extSignup'});
			}
		]);
	}, 5000);
});

var digestCache = {};

app.internal.get('/digest/:type?', function(request, response, onerror) {
	if (request.headers.host === 'ec2-46-137-66-73.eu-west-1.compute.amazonaws.com:9044') {
		response.writeHead(307, {location:'http://46.4.38.148:9044'+request.url});
		response.end();
		return;
	}

	var map = function() {
		var sub = function(a,be) {
			var real = {};

			for (var i in a) {
				real[i] = a[i] - (be[i] || 0);
			}

			return real;
		};

		var events = this.events;

		if(!this.tests) {
			return;
		}

		this.tests.forEach(function(test) {
			var trackname = test.name+'.'+test.track;
			var result = {count:1,accumulated:{},created:test.created};
			var real = sub(test.endedevents || events, test.offsetevents);

			for (var event in real) {
				result[event] = real[event] ? 1 : 0;
				result.accumulated[event] = real[event];
			}

			emit(trackname, result);
		});
	};
	var reduce = function(key, values) {
		var res = values.pop();
		var inc = function(from, to) {
			for (var event in from) {
				if (event === 'created') {
					res.created = Math.min(res.created, from.created);
					continue;
				}
				if (typeof from[event] === 'number') {
					to[event] = (to[event] || 0) + from[event];				
				}
			}
		};

		values.forEach(function(val) {
			inc(val, res);
			inc(val.accumulated, res.accumulated);
		});

		return res;
	};
	var mergeProps = function(tests) {
		var props = ['count', 'upload', 'share', 'email', 'signup', 'extView', 'extUpload', 'extSignup'];
		var extraProps = [];
		var sorts = {};

		tests.forEach(function(test) {
			if (test.value.accumulated) {
				Object.keys(test.value.accumulated).forEach(function(val) {
					test.value['accumulated-'+val] = test.value.accumulated[val];
				});
			}
			Object.keys(test.value).forEach(function(val) {
				if (!val || val === 'undefined' || val === 'accumulated' || val === 'created') {
					return;
				}

				sorts[val] = Math.min(sorts[val] || test.created, test.created);
				
				if (props.indexOf(val) < 0 && extraProps.indexOf(val) < 0) {					
					extraProps.push(val);
				}
			});
		});

		return props.concat(extraProps.sort(function(a,b) {
			if (a.indexOf('accumulated') > -1 && b.indexOf('accumulated') === -1) {
				return 1;
			}
			return sorts[a] - sorts[b];
		}));
	};
	var csvify = function(tests) {
		var props = mergeProps(tests);
		var res = 'name,'+props.join(',') + '\n';

		tests.forEach(function(test) {
			res += test._id+','+props.map(function(prop) {
				return test.value[prop] || '0';
			}).join(',') + '\n';
		});

		return res;
	};
	var htmlify = function(tests) {
		var props = mergeProps(tests);
		var res = '<html><head><title>Microsoft Parental Analytics</title></head></head><body><br><br><br><center><font face=helvetica><marquee><h1>Welcome to My page on <a href="https://docs.google.com/spreadsheet/ccc?key=0AnybHzeNDQU1dFc4dlFhMVhtQ3EtdUliMXNUZ215VGc&pli=1#gid=0">Internet</a></h1></marquee><br><br><table cellpadding=10 border=1><tr><th>name</th><th>' + props.join('</th><th>') + '</th></tr>';

		tests.forEach(function(test) {
			res += '<tr><td>' + test._id + '</td><td>' + props.map(function(prop) {
				return test.value[prop] || '0';
			}).join('</td><td>') + '</td></tr>';
		});

		return res + '</table></font></center></body></html>';
	}

	var query = {
		merged: {$ne:true}
	};

	if (request.query.anon) {
		query.userid = /^anon/;
	}
	if (request.query.condition) {
		var cond = request.query.condition;

		query['events.'+cond] = {$exists:true};
	}

	var md5 = function(obj) {
		return require('crypto').createHash('md5').update(JSON.stringify(obj)).digest('hex');
	};

	var cacheKey = 'digest'+md5(query);

	response.connection.setTimeout(30*60*1000);

	common.step([
		function(next) {
			var cache = digestCache[cacheKey];

			if (cache) {
				cache(next);
				return;
			}

			var stack = [];
			var lastQuery = Date.now();
			var docs;

			var runQuery = function() {
				db.analytics.mapReduce(map, reduce, {
					query: query,
					out: {inline:true}
				}, function(err, result) {
					docs = result;

					while (stack.length) {
						stack.shift()(null, docs);
					}
				});
			};

			digestCache[cacheKey] = cache = function(callback) {
				if (docs) {
					callback(null, docs);

					if (Date.now() - lastQuery < 10*60*1000) {
						return;
					}

					lastQuery = Date.now();
					runQuery();
					return;
				}
				stack.push(callback);
			};

			cache(next);
			runQuery();
		},
		function(docs) {
			var created = {};

			docs = docs.filter(function(doc) {
				return doc._id !== 'null.null';
			});
			docs.forEach(function(doc) {
				var id = doc._id.replace(/\.\d+$/, '');
				var track = parseInt((doc._id.match(/\.(\d+)$/) || [null, 0])[1], 10);

				created[id] = created[id] || doc.value.created;
				doc.value.created = created[id]+track;
			});
			docs.sort(function(a, b) {
				return a.value.created - b.value.created;
			});

			if (request.params.type === 'html') {
				response.setHeader('content-type','text/html');
				response.end(htmlify(docs));

				return;	
			}

			response.setHeader('content-type','text/plain');
			response.end(csvify(docs));
		}
	], onerror);
});

app.listen(9044, function() {
	services.join({name:'analytics', internal:app.address().port})
});

process.on('uncaughtException', function(err) {
	console.error(err.stack);
});
