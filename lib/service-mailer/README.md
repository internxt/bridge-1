**Notice: Development on this repository is currently on pause during our v3 rearchitecture. Please see [storj/storj](https://github.com/storj/storj) for ongoing v3 development.**

Storj Service Mailer
====================

[![Build Status](https://img.shields.io/travis/Storj/service-mailer.svg?style=flat-square)](https://travis-ci.org/Storj/service-mailer)

Email dispatcher and templates for various Storj services.

Installation and Usage
----------------------

```
npm install storj-service-mailer --save
```

```js
var StorjMailer = require('storj-service-mailer');
var mailer = new StorjMailer({
  host: '127.0.0.1',
  port: 465,
  secure: true,
  auth: {
    user: 'username',
    pass: 'password'
  },
  from: 'robot@storj.io'
});

mailer.dispatch('<to_email_address>', '<template_name>', {
  template: 'variables',
  go: { in: 'here' }
}, function(err) {
  // Mail sent!
});
```

Templates
---------

To add a new email template to this package, see the `templates/` directory.
Choose a new name for the template and create 3 files:

* `templatename.subject` - Contains plain text subject text (hbs supported)
* `templatename.txt` - Contains plain text email body (hbs supported)
* `templatename.html` - Contains HTML formatted email body (hbs supported)

Once these files are added to the templates directory, you can reference them 
by `templatename` when calling `Mailer#dispatch`.

License
-------

Storj Service Mailer - Email dispatcher for various Storj services  
Copyright (C) 2016 Storj Labs, Inc

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see http://www.gnu.org/licenses/.


