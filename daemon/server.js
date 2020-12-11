const express = require('express');
const morganBody = require('morgan-body');
const bodyParser = require('body-parser');
const {v4: uuidv4} = require('uuid');
const Promise = require('bluebird');
const fs = Promise.promisifyAll(require('fs'));
const os = require('os');
const path = require('path');
const crypto = require('crypto');
global.XMLHttpRequest = require("xmlhttprequest").XMLHttpRequest;
const {SpaceClient} = require('@fleekhq/space-client');


const app = express();
app.use(bodyParser.json());
morganBody(app);


const port = process.env.PORT || 3000;
const bucketName = process.env.BUCKET_NAME || 'ddocker';
const bucketTempDir = process.env.BUCKET_TEMP_DIR || os.tmpdir();
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


let uuidMap = {};


let checksumFile = (path) => {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash('sha256');
        const stream = fs.createReadStream(path);
        stream.on('error', err => reject(err));
        stream.on('data', chunk => hash.update(chunk));
        stream.on('end', () => resolve(hash.digest('hex')));
    });
};


let tryMkdirSync = (dir) => {
    if (fs.existsSync(dir)) {
        return
    }
    try {
        fs.mkdirSync(dir)
    } catch (err) {
        if (err.code === 'ENOENT') {
            tryMkdirSync(path.dirname(dir)); //create parent dir
            tryMkdirSync(dir); //create dir
        }
    }
};


app.get('/', (request, response) => {
    return response.send('ok');
});


app.get('/v2/', (request, response) => {
    return response.send('ok');
});


app.head('/v2/:name/blobs/:digest', async (request, response) => {
    const bucketPath = path.join('/blobs', request.params.digest);
    let fileResponse;
    try {
        fileResponse = await client.openFile({
                bucket: bucketName,
                path: bucketPath
            },
            {
                authorization: `AppToken ${token}`,
            });
    } catch (e) {
        return response.status(404).end();
    }
    let filename;
    try {
        filename = fileResponse.getLocation().toString();
    } catch (e) {
        return response.status(404).end();
    }
    const checksum = await checksumFile(filename);
    const digest = "sha256:" + checksum;
    const stats = await fs.statAsync(filename);
    response.set('Content-Length', stats.size);
    response.set('Docker-Content-Digest', digest);
    return response.status(200).end();
});


app.post('/v2/:name/blobs/uploads/', (request, response) => {
    const uniqueId = uuidv4();
    uuidMap[uniqueId] = 0;
    response.set('Location', '/v2/' + request.params.name + '/blobs/uploads/' + uniqueId);
    response.set('Docker-Upload-UUID', uniqueId);
    response.set('Range', '0-0');
    response.set('Content-Length', '0');
    return response.status(202).end();
});


app.patch('/v2/:name/blobs/uploads/:uuid', (request, response) => {
    const filename = path.join(bucketTempDir, "blobs", request.params.uuid);
    tryMkdirSync(path.dirname(filename));
    const startPoint = uuidMap[request.params.uuid] || 0;
    var length = 0;
    const stream = fs.createWriteStream(filename);
    stream.on('open', () => {
        request.on('data', (data) => {
            length += data.length;
            stream.write(data);
        });

        request.on('end', () => {
            stream.end();
            uuidMap[request.params.uuid] += length;
            response.set('Location', '/v2/' + request.params.name + '/blobs/uploads/' + request.params.uuid);
            response.set('Range', startPoint + '-' + uuidMap[request.params.uuid]);
            response.set('Content-Length', '0');
            response.set('Docker-Upload-UUID', request.params.uuid);
            return response.status(202).end();
        });
    });
    stream.on('error', e => {
        console.log(e);
        return response.status(500).send();
    });
});


app.put('/v2/:name/blobs/uploads/:uuid', async (request, response) => {
    const filename = path.join(bucketTempDir, "blobs", request.params.uuid);
    tryMkdirSync(path.dirname(filename));
    const digest = request.query.digest;
    const checksum = await checksumFile(filename);
    if ('sha256:' + checksum !== digest) {
        return response.status(500).end();
    }
    const newFileName = path.join(bucketTempDir, "blobs", digest);
    await fs.renameAsync(filename, newFileName);
    let stream;
    try {
        stream = client.addItems({
                targetPath: '/blobs/', // path in the bucket to be saved
                sourcePaths: [newFileName]
            },
            {
                authorization: `AppToken ${token}`,
            });
    } catch (e) {
        console.log(e);
        return response.status(500).end();
    }

    stream.on('data', (data) => {
        console.log('data: ', data);
    });

    stream.on('error', (error) => {
        console.error('error: ', error);
        return response.status(500).end();
    });

    stream.on('end', () => {
        response.set('Location', '/v2/' + request.params.name + '/blobs/' + digest);
        response.set('Content-Length', '0');
        response.set('Docker-Content-Digest', digest);
        return response.status(201).end();
    });
});


app.get('/v2/:name/blobs/:digest', async (request, response) => {
    const bucketPath = path.join('/blobs', request.params.digest);
    let fileResponse;
    try {
        fileResponse = await client.openFile({
                bucket: bucketName,
                path: bucketPath
            },
            {
                authorization: `AppToken ${token}`,
            });
    } catch (e) {
        console.log(e);
        return response.status(500).end();
    }
    let filename;
    try {
        filename = fileResponse.getLocation().toString();
    } catch (e) {
        console.log(e);
        return response.status(500).end();
    }
    const checksum = await checksumFile(filename);
    const digest = "sha256:" + checksum;
    const stats = await fs.statAsync(filename);
    response.set('Content-Length', stats.size);
    response.set('Docker-Content-Digest', digest);
    return response.status(200).sendFile(filename);
});


app.put('/v2/:name/manifests/:reference', (request, response) => {
    const filename = path.join(bucketTempDir, 'manifests', request.params.name, request.params.reference);
    tryMkdirSync(path.dirname(filename));
    const stream = fs.createWriteStream(filename);
    stream.on('open', () => {
        request.on('data', (data) => {
            stream.write(data);
        });

        request.on('end', async () => {
            stream.end();
            let checksum = await checksumFile(filename);
            let uploadStream;
            try {
                uploadStream = client.addItems({
                        targetPath: '/manifests/' + request.params.name + "/", // path in the bucket to be saved
                        sourcePaths: [filename]
                    },
                    {
                        authorization: `AppToken ${token}`,
                    });
            } catch (e) {
                console.log(e);
                return response.status(500).end();
            }
            uploadStream.on('data', (data) => {
                console.log('data: ', data);
            });

            uploadStream.on('error', (error) => {
                console.error('error: ', error);
                return response.status(500).end();
            });

            uploadStream.on('end', () => {
                response.set('Content-Length', 0);
                response.set('Docker-Content-Digest', 'sha256:' + checksum);
                response.set('Location', '/v2/' + request.params.name + "/manifests/" + request.params.reference);
                return response.status(201).end();
            });

        });
    });
    stream.on('error', e => {
        console.log(e);
        return response.status(500).send();
    });

});


app.get('/v2/:name/manifests/:reference', async (request, response) => {
    const bucketPath = path.join('/manifests', request.params.name, request.params.reference);
    let fileResponse;
    try {
        fileResponse = await client.openFile({
                bucket: bucketName,
                path: bucketPath
            },
            {
                authorization: `AppToken ${token}`,
            })
    } catch (e) {
        return response.status(404).send('{"errors": [{"code": "MANIFEST_UNKNOWN", "message": "MANIFEST_UNKNOWN"}]}');
    }
    let filename;
    try {
        filename = fileResponse.getLocation().toString();
    } catch (e) {
        return response.status(404).send('{"errors": [{"code": "MANIFEST_UNKNOWN", "message": "MANIFEST_UNKNOWN"}]}');
    }
    const checksum = await checksumFile(filename);
    const digest = "sha256:" + checksum;
    response.set('Content-Type', 'application/vnd.docker.distribution.manifest.v2+json');
    response.set('Docker-Content-Digest', digest);
    return response.status(200).sendFile(filename);
});


app.listen(port, (err) => {
    if (err) {
        return console.log(err)
    }
    console.log(`server is listening on ${port}`)
});