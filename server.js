var bootstrap = require('bootstrap');
var db = require('mongojs').connect(bootstrap.config.db, ['analytics', 'abdigest', 'shares']);
var common = require('common');
var services = require('services').connect(bootstrap.config.peer);
var analytics = require('analytics')(services);
var root = require('root');
var app = root();

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

app.use(root.log);
app.use(root.json);
app.use(root.query);
app.fn('response.ack', function() {
	this.json({ack:true});
});
app.use('auth', function(request, response, next) {
	request.userid = request.query.userid;

	if (!request.userid) {
		response.json(403, 'Microsoft parental control');
		return;
	}

	next();
});
app.use('internal', function(request, response, next) {
	var ip = request.connection.remoteAddress;

	if (ip.indexOf('10.') !== 0 && ip.indexOf('127.') !== 0 && ip !== '77.66.2.197' && request.query.key !== 'maffe') {
		response.json(403, 'Microsoft parental control');
		return;
	}
	next();
});

app.auth.get('/tests', function(request, response) {
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
	], response.json);
});

app.auth.post('/tests/create', function(request, response) {
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
	], response.json);
});

app.auth.post('/event', function(request, response) {
	common.step([
		function(next) {
			var map = {};

			map['events.'+request.json.name] = 1;
			db.analytics.update({userid:request.userid}, {$inc:map}, next);
		},
		function() {
			response.ack();
		}
	], response.json);
});

/*app.auth.post('/user/update', function(request, response) {
	db.analytics.update({userid:request.userid}, {$set:request.json}, response.ack.bind(response));
});*/

app.auth.post('/merge', function(request, response) {
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
	], response.json);
});

app.internal.post('/inactive', function(request, response) {
	var name = request.json.name;

	common.step([
		function(next) {
			db.analytics.find({'tests.name':name}, {events:1, userid:1}, next);
		},
		function(users, next) {
			console.log(users)
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
	], response.json);
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
});

app.internal.get(['/digest','/digest.html'], function(request, response) {
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
			var result = {count:1,accumulated:{}};
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

		tests.forEach(function(test) {
			Object.keys(test.value).forEach(function(val) {
				if (val !== 'accumulated' && props.indexOf(val) < 0 && extraProps.indexOf(val) < 0) {
					extraProps.push(val);
				}
			});
		});

		return props.concat(extraProps.sort());
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
		var res = '<html><head><title>Microsoft Parental Analytics</title></head></head><body><table><tr><th>name</th><th>' + props.join('</th><th>') + '</th></tr>';

		tests.forEach(function(test) {
			res += '<tr><td>' + test._id + '</td><td>' + props.map(function(prop) {
				return test.value[prop] || '0';
			}).join('</td><td>') + '</td></tr>';
		});

		return res + '</table></body></html>';
	}

	common.step([
		function(next) {
			db.abdigest.remove(next);
		},
		function(next) {
			db.analytics.mapReduce(map, reduce, {
				query: {
					merged: {$ne:true}
				},
				out: 'abdigest'
			}, next);
		},
		function(next) {
			db.abdigest.find(next);
		},
		function(doc) {
			if (request.url.indexOf('/digest.html') === 0) {
				response.setHeader('content-type','text/html');
				response.end(htmlify(doc));

				return;	
			}

			response.setHeader('content-type','text/plain');
			response.end(csvify(doc));
		}
	], response.json);
});

app.listen(9044, function() {
	services.join({name:'analytics', internal:app.address().port})
});
