const express = require('express');
const morganBody = require('morgan-body');
const bodyParser = require('body-parser');
const {v4: uuidv4} = require('uuid');
const Promise = require('bluebird');
const fs = Promise.promisifyAll(require('fs'));
const os = require('os');
const path = require('path');
const crypto = require('crypto');
global.WebSocket = require('isomorphic-ws');
const {Buckets} = require('@textile/hub');
const {Context} = require('@textile/context');
const {Client} = require('@textile/threads-client');
const {ThreadID} = require('@textile/threads');


const app = express();
app.use(bodyParser.json());
morganBody(app);


const port = process.env.PORT || 3000;
const bucketName = process.env.BUCKET_NAME || 'ddocker';
const bucketTempDir = process.env.BUCKET_TEMP_DIR || os.tmpdir();
const isPrivate = process.env.BUCKET_ENCRYPTION === "true";
const hubHost = process.env.HUB_HOST;
const hubKey = process.env.HUB_KEY;
const hubSecret = process.env.HUB_SECRET;
const threadName = 'buckets';
const keyInfo = {
    key: hubKey,
    secret: hubSecret
};


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


let createLocalBucket = async () => {
    let ctx = new Context(hubHost);
    const threadsClient = new Client(ctx);
    let threadID = ThreadID.fromRandom();
    await threadsClient.newDB(threadID, threadName);
    ctx.withThread(threadID);
    let buckets = new Buckets(ctx);
    let bucketResponse = await buckets.create(bucketName, {threadName: threadName, encrypted: isPrivate});
    console.log(bucketResponse);
    let bucketKey = bucketResponse.root.key;
    return {buckets, bucketKey}
};


let getBucket = async () => {
    let buckets;
    try {
        if (hubHost) {
            buckets = await Buckets.withKeyInfo(keyInfo, {host: hubHost});
        } else {
            buckets = await Buckets.withKeyInfo(keyInfo);
        }
    } catch(e) {
        console.log(e);
        throw new Error('Buckets.withKeyInfo failed');
    }
    let bucketResponse;
    let bucketKey;
    try {
        if(hubKey && hubSecret) {
            bucketResponse = await buckets.getOrCreate(bucketName, {threadName: threadName, encrypted: isPrivate});
            console.log(bucketResponse);
            if (!bucketResponse.root) throw new Error('bucket not created');
            bucketKey = bucketResponse.root.key;
        }
        else{
            let ctx = new Context(hubHost);
            const threadsClient = new Client(ctx);
            const threadsList = await threadsClient.listDBs();
            let threadId;
            for (var key in threadsList) {
                if(threadsList.hasOwnProperty(key) && threadsList[key].name === threadName){
                    threadId = key;
                }
            }
            if(!threadId){
                let {buckets, bucketKey} = await createLocalBucket();
                return {buckets, bucketKey}
            }
            ctx.withThread(threadId);
            buckets = new Buckets(ctx);
            let bucketList = await buckets.list();
            bucketList.map(bucket => {
               if(bucket.name === bucketName){
                   bucketKey = bucket.key;
               }
            });
            console.log(bucketKey);
        }
    } catch(e){
        console.log(e);
        throw new Error('buckets.getOrCreate failed');
    }
    return {buckets, bucketKey}
};


app.get('/', (request, response) => {
    return response.send('ok');
});


app.get('/v2/', (request, response) => {
    return response.send('ok');
});


app.head('/v2/:name/blobs/:digest', async (request, response) => {
    const bucketPath = path.join('blobs', request.params.digest);
    let chunks;
    try {
        const {buckets, bucketKey} = await getBucket();
        chunks = await buckets.pullPath(bucketKey, bucketPath);
    } catch (e) {
        return response.status(404).end();
    }
    const filename = path.join(bucketTempDir, "get", uuidv4());
    tryMkdirSync(path.dirname(filename));
    const stream = fs.createWriteStream(filename);
    try {
        for await (const chunk of chunks) {
            stream.write(chunk)
        }
    } catch (e) {
        stream.end();
        return response.status(404).end();
    }
    stream.end();
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
    const stream = fs.createReadStream(newFileName);
    const bucketPath = path.join('blobs', digest);
    try{
        const {buckets, bucketKey} = await getBucket();
        let uploadResponse = await buckets.pushPath(bucketKey, bucketPath, stream);
        console.log(uploadResponse);
    } catch(e){
        console.log(e);
        return response.status(500).send();
    }
    response.set('Location', '/v2/' + request.params.name + '/blobs/' + digest);
    response.set('Content-Length', '0');
    response.set('Docker-Content-Digest', digest);
    return response.status(201).end();
});


app.get('/v2/:name/blobs/:digest', async (request, response) => {
    const bucketPath = path.join('blobs', request.params.digest);
    let chunks;
    try {
        const {buckets, bucketKey} = await getBucket();
        chunks = await buckets.pullPath(bucketKey, bucketPath);
    } catch (e) {
        console.log(e);
        return response.status(500).end();
    }
    const filename = path.join(bucketTempDir, "get", uuidv4());
    tryMkdirSync(path.dirname(filename));
    const stream = fs.createWriteStream(filename);
    try {
        for await (const chunk of chunks) {
            stream.write(chunk)
        }
    } catch (e) {
        stream.end();
        return response.status(500).end();
    }
    stream.end();
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
            const checksum = await checksumFile(filename);
            const uploadStream = fs.createReadStream(filename);
            const bucketPath = path.join('manifests', request.params.name, request.params.reference);
            try {
                const {buckets, bucketKey} = await getBucket();
                let uploadResponse = await buckets.pushPath(bucketKey, bucketPath, uploadStream);
                console.log(uploadResponse);
            } catch(e){
                console.log(e);
                return response.status(500).end();
            }
            response.set('Content-Length', 0);
            response.set('Docker-Content-Digest', 'sha256:' + checksum);
            response.set('Location', '/v2/' + request.params.name + "/manifests/" + request.params.reference);
            return response.status(201).end();
        })

    });

    stream.on('error', e => {
        console.log(e);
        return response.status(500).send();
    });

});


app.get('/v2/:name/manifests/:reference', async (request, response) => {
    const bucketPath = path.join('manifests', request.params.name, request.params.reference);
    let chunks;
    try {
        const {buckets, bucketKey} = await getBucket();
        chunks = await buckets.pullPath(bucketKey, bucketPath);
    } catch (e) {
        console.log(e);
        return response.status(404).send('{"errors": [{"code": "MANIFEST_UNKNOWN", "message": "MANIFEST_UNKNOWN"}]}');
    }
    const filename = path.join(bucketTempDir, "get", uuidv4());
    tryMkdirSync(path.dirname(filename));
    const stream = fs.createWriteStream(filename);
    try {
        for await (const chunk of chunks) {
            stream.write(chunk)
        }
    } catch (e) {
        stream.end();
        return response.status(404).send('{"errors": [{"code": "MANIFEST_UNKNOWN", "message": "MANIFEST_UNKNOWN"}]}');
    }
    stream.end();
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