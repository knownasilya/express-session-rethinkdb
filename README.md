Express Session RethinkDB
=================

###RethinkDB session store for Express 4.x 

*Inspired by TJ Holowaychuk's [Connect Redis](https://github.com/visionmedia/connect-redis)*
*Inspired by guillaumervls [Connect RethinkDB](https://github.com/guillaumervls/connect-rethinkdb)

##Installation

```npm install express-session-rethinkdb```

##Getting started

Note that you must already have Express Session already installed (```npm install express-session```).

```javascript
var express = require('express');
var session = require('express-session');
var RDBStore = require('express-session-rethinkdb')(session);

var rDBStore = new RDBStore({
  maxAge: 60000,
  clientOptions: {
    db: 'test'
    host: 'localhost',
    port: '28015'
  }
  table: 'session'
});

var app = express();
app.use( require('cookie-parser')() );
app.use( session({
    key: "app.sid",
    secret: "my5uperSEC537(key)!",
    cookie: { maxAge: 60000 },
    store: rDBStore 
  })
);

app.use( ... );
```

##Constructor options

###maxAge
Unlike Redis, RethinkDB does not provide a ```SETEX``` function. So we have to flush expired sessions periodically. This defines the amount of time between two flushes.
*Defaults to 60 seconds*

###clientOptions
We need these to connect to our DB. 
*See [RethinkDB's doc](http://rethinkdb.com/api/#js:accessing_rql-connect).*

###table
Name of the table in which session data will be stored.
*Defaults to 'session'*

###browserSessionsMaxAge
If you do not set ```cookie.maxAge``` in ```session``` middleware, sessions will last until the user closes his/her browser. However we cannot keep the session data infinitely (for size and security reasons). In this case, this setting defines the maximum length of a session, even if the user doesn't close his/her browser.
*Defaults to 1 day*

##License

The MIT License (MIT)

Copyright (c) <year> <copyright holders>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
