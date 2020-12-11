global.XMLHttpRequest = require("xmlhttprequest").XMLHttpRequest;
const {SpaceClient} = require('@fleekhq/space-client');


const bucketName = process.env.BUCKET_NAME || 'ddocker';
// default port exposed by the daemon for client connection is 9998
const client = new SpaceClient({
    url: `http://0.0.0.0:9998`,
    defaultBucket: bucketName
});
client.initializeMasterAppToken().then((initializeMasterAppTokenRes) => {
    const token = initializeMasterAppTokenRes.getApptoken();
    console.log(`Token: ${token}`);
    client.generateKeyPairWithForce({
        authorization: `AppToken ${token}`,
    })
        .then(() => {
            console.log('keys generated');
        })
        .catch((err) => {
            console.error(err);
        });
}).catch((e) => {
    console.log(e);
});
