const stripe = require("stripe")(process.env.STRIPE_SK);
stripe.setApiVersion('2019-03-14');

function getCustomerByEmail(email) {
    return new Promise((resolve, reject) => {
        stripe.customers.list({
            email: email
        }, (err, customers) => {
            if (err) {
                reject(err.type);
            } else {
                resolve(customers);
            }
        });
    });
}

function getCustomerById(customer_id) {
    return new Promise((resolve, reject) => {
        stripe.customers.retrieve(customer_id, (err, customer) => {
            if (err) {
                reject(err.type);
            } else {
                resolve(customer);
            }
        });
    });
}

function customerExists(email) {
    return new Promise((resolve, reject) => {
        getCustomerByEmail(email).then(result => {
            if (result.data.length == 0) {
                resolve(false);
            } else {
                resolve(result);
            }
        }).catch(err => {
            reject(err);
        });
    });
}

function subscribeCustomerToPlan(customer_id, plan_id) {
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
}

function createNewCustomer(email, token) {
    return new Promise((resolve, reject) => {
        customerExists(email).then(customerExists => {
            if (!customerExists) {
                resolve(false);
            } else {
                stripe.customers.create({
                    description: 'Customer for ' + email,
                    source: token
                }, (err, customer) => {
                    if (err) {
                        reject(false);
                    } else {
                        resolve(customer);
                    }
                });
            }
        });
    });
}

function getPlansList() {
    return new Promise((resolve, reject) => {
        stripe.plans.list({}, (err, plans) => {
            if (err) {
                reject(err);
            } else {
                resolve(plans);
            }
        });
    });
}

function getPlanByName(plan_name) {
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
}

function cancelSubscriptionById(subscription_id) {
    return new Promise((resolve, reject) => {
        stripe.subscriptions.del(subscription_id, (err, confirmation) => {
            if (err) {
                reject(err.type);
            } else {
                resolve(confirmation);
            }
        });
    });
}

