global.XMLHttpRequest = require("xmlhttprequest").XMLHttpRequest;
const {SpaceClient} = require('@fleekhq/space-client');


const bucketName = process.env.BUCKET_NAME || 'ddocker';
// default port exposed by the daemon for client connection is 9998
const client = new SpaceClient({
    url: `http://0.0.0.0:9998`,
    defaultBucket: bucketName
});
const token = process.env.DAEMON_TOKEN;
client
    .createBucket({
            slug: bucketName
        },
        {
            authorization: `AppToken ${token}`,
        }
    )
    .then((res) => {
        const bucket = res.getBucket();
        console.log(bucket.getPath());
    })
    .catch((err) => {
        console.error(err);
    });
