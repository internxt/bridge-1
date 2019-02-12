'use strict';

const expect = require('chai').expect;
const sinon = require('sinon');
const proxyquire = require('proxyquire');
const Mailer = require('..');

describe('Mailer', function() {

  describe('@constructor', function() {

    it('should create an instance with/out the new keyword', function() {
      expect(Mailer({
        host: '127.0.0.1',
        port: 465,
        secure: true,
        auth: {
          user: 'username',
          pass: 'password'
        },
        from: 'robot@storj.io'
      })).to.be.instanceOf(Mailer);
      expect(new Mailer({
        host: '127.0.0.1',
        port: 465,
        secure: true,
        auth: {
          user: 'username',
          pass: 'password'
        },
        from: 'robot@storj.io'
      })).to.be.instanceOf(Mailer);
    });

  });

  describe('#getTemplate', function() {

    it('should return the subject, text, and markup', function(done) {
      var mailer = new Mailer({
        host: '127.0.0.1',
        port: 465,
        secure: true,
        auth: {
          user: 'username',
          pass: 'password'
        },
        from: 'robot@storj.io'
      });
      mailer.getTemplate('confirm', function(err, template) {
        expect(err).to.equal(null);
        expect(typeof template({}).subject).to.equal('string');
        expect(typeof template({}).markup).to.equal('string');
        expect(typeof template({}).plaintext).to.equal('string');
        done();
      });
    });

    it('should bubble error if no template', function(done) {
      var mailer = new Mailer({
        host: '127.0.0.1',
        port: 465,
        secure: true,
        auth: {
          user: 'username',
          pass: 'password'
        },
        from: 'robot@storj.io'
      });
      mailer.getTemplate('INVALID', function(err) {
        expect(err).to.be.instanceOf(Error);
        done();
      });
    });

  });

  describe('#dispatch', function() {

    it('should bubble error if no template', function(done) {
      var mailer = new Mailer({
        host: '127.0.0.1',
        port: 465,
        secure: true,
        auth: {
          user: 'username',
          pass: 'password'
        },
        from: 'robot@storj.io'
      });
      mailer.dispatch('user@domain.tld', 'INVALID', {}, function(err) {
        expect(err).to.be.instanceOf(Error);
        done();
      });
    });

    it('should call the mail transport sendMail method', function(done) {
      var FakeMailer = proxyquire('..', {
        nodemailer: {
          createTransport: function() {
            return {
              sendMail: sinon.stub().callsArg(1)
            };
          }
        }
      });
      var mailer = new FakeMailer({
        host: '127.0.0.1',
        port: 465,
        secure: true,
        auth: {
          user: 'username',
          pass: 'password'
        },
        from: 'robot@storj.io'
      });
      mailer.dispatch('user@domain.tld', 'confirm', {}, done);
    });

  });

});
