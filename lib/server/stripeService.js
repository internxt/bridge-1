// TODO: Use global config
const config = JSON.parse(require('fs').readFileSync(process.env.HOME + '/.inxt-bridge/config/' + process.env.NODE_ENV, 'utf8'));

const stripe = require('stripe')(config.stripe.SK_LIVE);
stripe.setApiVersion('2020-03-02');

exports.getCustomerByEmail = function (email) {
  return new Promise((resolve, reject) => {
    stripe.customers.list({
      email: email
    }, (err, customers) => {
      if (err) { reject(err.type); }
      else { resolve(customers); }
    });
  });
};

exports.getCustomerById = function (customer_id) {
  return new Promise((resolve, reject) => {
    stripe.customers.retrieve(customer_id, (err, customer) => {
      if (err) { reject(err.type); }
      else { resolve(customer); }
    });
  });
};

exports.customerExists = function (email) {
  return new Promise((resolve, reject) => {
    this.getCustomerByEmail(email).then(result => {
      if (result.data.length == 0) {
        resolve(false);
      } else {
        resolve(result);
      }
    }).catch(err => {
      reject(err);
    });
  });
};

exports.subscribeCustomerToPlan = function (customer_id, plan_id) {
  return new Promise((resolve, reject) => {
    stripe.subscriptions.create({
      customer: customer_id,
      items: [{ plan: plan_id }]
    }, (err, subscription) => {
      if (err) {
        reject(err.type);
      } else {
        resolve(subscription);
      }
    });
  });
};

exports.createNewCustomer = function (email, token) {
  return new Promise((resolve, reject) => {
    this.customerExists(email).then(customerExists => {
      if (customerExists) {
        resolve(false);
      } else {
        stripe.customers.create({
          description: 'Customer for ' + email,
          source: token,
          email: email
        }, (err, customer) => {
          if (err) { reject(false); }
          else { resolve(customer); }
        });
      }
    });
  });
};

exports.getPlansList = function () {
  return new Promise((resolve, reject) => {
    stripe.plans.list({}, (err, plans) => {
      if (err) {
        reject(err);
      } else {
        resolve(plans);
      }
    });
  });
};

exports.getPlanByName = function (plan_name) {
  return new Promise((resolve, reject) => {
    stripe.plans.list({
      nickname: plan_name
    }, (err, plans) => {
      if (err) {
        reject(err);
      } else {
        resolve(plans);
      }
    });
  });
};

exports.cancelSubscriptionById = function (subscription_id) {
  return new Promise((resolve, reject) => {
    stripe.subscriptions.del(subscription_id, (err, confirmation) => {
      if (err) {
        reject(err.type);
      } else {
        resolve(confirmation);
      }
    });
  });
};

