#!/bin/sh
":" //# -*- mode: js -*-; exec /usr/bin/env node --max-old-space-size=9000 --stack-size=42000 "$0" "$@"

"use strict";

/************************************/
/* MODULE VARIABLE SECTION **********/
/************************************/

var fs = require( 'fs' );
var async = require( 'async' );
var osPath = require( 'path' );
var expandHomeDir = require( 'expand-home-dir' );
var yargs = require( 'yargs' );
//~ var lzma = require('lzma-native');
var lzma = require('xz');
//~ var lzma = require('node-liblzma');
var htmlparser = require("htmlparser2");
var domutils = require("domutils");
var url = require('url');
var csvOutput = require('csv-stringify');
var zlib = require('zlib');
var fsx = require('fs-extra');

var srcPath;
var outPath;
var src; // input file reader

var articleUrls = null;
var metadata = [];

function log (arg) {
    argv && argv.verbose && console.log.apply(this, arguments);
};

function readUInt64LE(buf, offset) {
    var low = buf.readUInt32LE(offset);
    var high = buf.readUInt32LE(offset + 4);
    return  high * 0x100000000 + low;
};

function blobPath(clusterIdx, blobIdx) {
    return osPath.join(outPath, clusterIdx + '-' + blobIdx + '-blob');
}

function articlePath(article) {
    return osPath.join(outPath, article.url);
}

//
// class Reader
//
function Reader (path, callback) {
    this.path = path;
    this.position = 0;
    this.fd = null;

    this.queue = async.queue( function (task, callback) {
        var length = task.length;
        var position = task.position;
        if (typeof position !== 'number')
            position = this.position;
        this.position = position + length;
        fs.read( this.fd, Buffer.alloc(length), 0, length, position, function (err, read, buffer) {
            callback(err, buffer, read, position);
        })
    }.bind(this));

    fs.open(path, 'r', function (err, fd) {
        this.fd = fd;
        log('init Reader');
        //~ log(this.queue);
        callback(err);
    }.bind(this));
};

Reader.prototype.read = function (length, position, callback) {
    this.queue.push({
            length: length,
            position:position
        },
        callback
    );
};

Reader.prototype.close = function (callback) {
    var fd = this.fd;
    var fdclose = function () {
        log('xxxxx');
        return fs.close(fd, callback);
    };

    if (this.queue.idle())
        fdclose()
    else
        this.queue.drain = fdclose;
};

Reader.prototype.tell = function () {
    return this.position;
};


var headerLength = 80;

var header = {
    magicNumber: 72173914,  //    integer     0   4   Magic number to recognise the file format, must be 72173914
    version: 5,             //    integer     4   4   ZIM=5, bytes 1-2: major, bytes 3-4: minor version of the ZIM file format
    uuid: 0,                //    integer     8   16  unique id of this zim file
    articleCount: 0,        //    integer     24  4   total number of articles
    clusterCount: 0,        //    integer     28  4   total number of clusters
    urlPtrPos: 0,           //    integer     32  8   position of the directory pointerlist ordered by URL
    titlePtrPos: 0,         //    integer     40  8   position of the directory pointerlist ordered by Title
    clusterPtrPos: 0,       //    integer     48  8   position of the cluster pointer list
    mimeListPos: headerLength, // integer     56  8   position of the MIME type list (also header size)
    mainPage: 0xffffffff,   //    integer     64  4   main page or 0xffffffff if no main page
    layoutPage: 0xffffffff, //    integer     68  4   layout page or 0xffffffffff if no layout page
    checksumPos: 0,         //    integer     72  8   pointer to the md5checksum of this file without the checksum itself. This points always 16 bytes before the end of the file.
    geoIndexPos: 0,         //    integer     80  8   pointer to the geo index (optional). Present if mimeListPos is at least 80.
};

function parseHeader (buf, read, position, callback) {

    header.articleCount = buf.readUInt32LE(24);
    header.clusterCount = buf.readUInt32LE(28);

    header.urlPtrPos = readUInt64LE(buf, 32);
    header.titlePtrPos = readUInt64LE(buf, 40);
    header.clusterPtrPos = readUInt64LE(buf, 48);
    header.mimeListPos = readUInt64LE(buf, 56);

    header.mainPage = buf.readUInt32LE(64);
    header.layoutPage = buf.readUInt32LE(68);

    log('header', header);

    callback();
};

function processClusterList (buf, read, position, callback) {
    //~ var offsets = Array(header.clusterCount);
    //~ for (var i=0; i < header.clusterCount; i++)
        //~ offsets[i] = readUInt64LE(buf, i * 8);
    //~ buf = null;

    //~ async.eachOfLimit(
        //~ offsets,
        //~ 8,
        //~ processCluster,
        //~ function (err, res) {
            //~ log('processClusterList', err, arguments);
            //~ callback(err);
        //~ }
    //~ );

    var i = -1;
    async.whilst(
        function () {
            i++;
            return i < header.clusterCount;
        },
        function (cb) {
            processCluster(readUInt64LE(buf, i * 8), i, cb);
        },
        function (err, res) {
            log('processClusterList', err, arguments);
            callback(err);
        }
    );
};

function processCluster(clusterOfs, clusterIdx, callback) {
    var offsets = [];
    var blobs = [];
    var blobIdx = 0;
    var nblobs;
    var isCompressed = false;
    var eof = false;

    var readCompression = function (cb) {
        var buf = slice.read(1);
        if (buf == null)
            return slice.once('readable', readCompression.bind(this, cb));

        isCompressed = buf.readUInt8(0) == 4;  // xz compressed

        log('processCluster', clusterIdx, header.clusterCount, isCompressed);

        if (isCompressed) { // xz compressed
            input = new lzma.Decompressor();
            slice.pipe(input);
        }

        cb();
    };

    var readOffsets = function (cb) {
        for (var buf; buf = input.read(4);) {
            var ofs = buf.readUInt32LE(0);
            if (offsets.length == 0) {
                nblobs = ofs / 4 - 1;
            }
            //~ log('readOffsets', clusterIdx, nblobs, offsets.length, ofs);
            offsets.push(ofs);

            if (offsets.length == nblobs + 1) {
                //~ log('readOffsets done', clusterIdx, nblobs, offsets.length, ofs);
                return cb();
            }
        }
        input.once('readable', readOffsets.bind(this, cb));
    };

    var readBlobs = function (cb) {
        while (true) {
            if (blobIdx == nblobs) {
                //~ log('readBlobs done', clusterIdx, isCompressed, nblobs, blobIdx, blobLen);
                return cb();
            }

            var blob;
            var blobLen = offsets[blobIdx+1] - offsets[blobIdx];
            //~ log('readBlobs', clusterIdx, isCompressed, nblobs, blobIdx, blobLen);
            if (blobLen === 0) {
                blob = Buffer.alloc(0);
            } else {
                blob = input.read(blobLen);
                if (! blob)
                    break;
            }
            blobs.push(blob);
            blobIdx++;
        }

        //~ log('readBlobs retry', clusterIdx, isCompressed, nblobs, blobIdx, blobLen);
        input.once('readable', readBlobs.bind(this, cb));
    };

    var writeBlobs = function (cb) {
        async.eachOf(
            blobs,
            function (blob, blobIdx, cbk) {
                //~ log('writeBlobs', clusterIdx, nblobs, blobIdx);
                fsx.outputFile(blobPath(clusterIdx, blobIdx), blob, cbk);
            },
            cb
        );
    }

    var slice = fs.createReadStream(
        null,
        {
            fd: src.fd,
            autoClose: false,
            start: clusterOfs
        }
    );

    slice.on('error', function (err) {
        console.error('processCluster', clusterIdx, 'input error', err);
        //~ process.exit(1);
    });

    slice.on('end', function () {
        log('processCluster', clusterIdx, 'input end');
        eof = true;
        //~ process.exit(1);
    });

    slice.on('close', function () {
        log('processCluster', clusterIdx, 'input closed');
        eof = true;
        //~ process.exit(1);
    });

    slice.on('open', function (fd) {
        log('processCluster', clusterIdx, 'input open', fd);
    });

    var input = slice; // input stream

    async.series([
            readCompression,
            readOffsets,
            readBlobs,
            writeBlobs
        ],
        function (err) {
            log('processCluster', clusterIdx, header.clusterCount, nblobs, offsets[nblobs] / 1024 / 1024, 'done', err);

            if (!eof) {
                slice.fd = null;
                slice.destroy();
            }

            callback(err);
        }
    );
}

function getDirEntry (article, callback) {
    var chunkLen = 512;
    var dirEntry = Buffer.alloc(0);

    var readChunk = function (pos) {
        src.read(chunkLen, pos, parseDirEntry);
    };

    var parseDirEntry = function (err, readBuf, readBytes, currPosition) {
        if (err) {
            console.error('processdirEntry read error', article.index, header.articleCount, err);
            return callback(err);
        }

        dirEntry = Buffer.concat([dirEntry, readBuf]);

        article.mimeIdx = dirEntry.readUInt16LE(0);
        article.nameSpace = dirEntry.toString('utf8', 3, 4);

        var strOfs = 16;
        if (article.mimeIdx ==  0xfffe || article.mimeIdx ==  0xfffd) {
            // linktarget or deleted entry
            return callback('finished'); // noop
        } else if (article.mimeIdx ==  0xffff ) { //redirect
            strOfs = 12;
            article.redirectIndex = dirEntry.readUInt32LE(8);
        } else {
            article.clusterIdx = dirEntry.readUInt32LE(8);
            article.blobIdx = dirEntry.readUInt32LE(12);
        }

        // read url and title
        var end = dirEntry.indexOf(0, strOfs);
        if (end != -1) {
            article.url = dirEntry.toString('utf8', strOfs, end);

            var strOfs = end + 1;
            end = dirEntry.indexOf(0, strOfs);
            if (end != -1) {
                article.title = dirEntry.toString('utf8', strOfs, end);
            }
        }

        if (end == -1) // short buffer -- read more
            return readChunk(currPosition);

        log('parseDirEntry', article.index, header.articleCount, '\n', article);

        articleUrls[article.index] = article.url;

        return callback(null, article); // continue processing
    };

    readChunk(article.offset);
}

function moveArticle(article, callback) {

    if (article.redirectIndex != null) //redirect
        return storeRedirect(article, function (err) {
                callback(err || 'finished');
        });

    var bpath = blobPath(article.clusterIdx, article.blobIdx);

    if (article.nameSpace == 'M') { // metadata
        return fs.readFile (bpath, 'utf8', function (err, data) {
            if (err)
                return callback(err);
            metadata.push([article.url.toLowerCase(), data]);
            fs.unlink(
                bpath,
                function (err) {
                    callback(err || 'finished');
            });
        });
    }

    log('moveArticle', article.index, header.articleCount, '\n', article);

    fsx.move(
        bpath,
        articlePath(article),
        {clobber: true},
        function (err) {
            //~ log('moveArticle', err, article);
            callback(err, article)
        }
    );
}

function parseArticle(article, callback) {
    if (article.nameSpace != 'A')
        return callback('finished');

    //~ log('parseArticle', article);

    var handler = new htmlparser.DomHandler(function (err, dom) {
        log('parseArticle ', err, article);
        if (err)
            return callback('finished');

        return callback(null, article, dom);
    });
    var parser = new htmlparser.Parser(
        handler,
        {decodeEntities: true}
    );

    var input = fs.createReadStream(articlePath(article));

    input.pipe(parser);
}

var nameSpaces = ['-', 'A', 'B', 'I', 'J', 'M', 'U', 'W', 'X'];

function alterLinks(article, dom, callback) {
    var nameSpaceLink = function (elem, attr) {
        if (! (elem.attribs && elem.attribs[attr]))
            return 0;

        var link;
        try {
            link = url.parse(elem.attribs[attr], true, true);
        } catch (err) {
            //~ console.error('alterLinks error', err, article, attr, elem.attribs[attr], elem);
            console.error('alterLinks', err.message, elem.attribs[attr], 'at', article.path);
            return 0;
        }
        if ( (link.protocol && link.protocol != 'http:' && link.protocol != 'https:')
                || link.host || ! link.pathname)
            return 0;

        var chunks = link.pathname.split('/');

        if (chunks[0] == '' // abs path
                || chunks[0] == '..'
                && nameSpaces.indexOf(chunks[1]) != -1) {
            chunks.shift();
            chunks.shift();
            link.pathname = chunks.join('/');
            //~ log('alterLinks', elem.attribs[attr], url.format(link));
            elem.attribs[attr] = url.format(link);
            return 1;
        }

        return 0;
    };

    var res = domutils.filter(
        function (elem) {
            // calculate both
            return !!(nameSpaceLink(elem, 'src') + nameSpaceLink(elem, 'href'));
        },
        dom,
        true
    );
    log('alterLinks', res.length);

    return callback(res.length != 0 ? null : 'finished', article, dom);
}

function saveArticle(article, dom, callback) {
    fsx.outputFile(articlePath(article), Buffer.from(domutils.getOuterHTML(dom)), callback);
}

function processArticle (articleIndex, callback) {

    if (articleUrls[articleIndex] != null)
        return callback();

    var article = newArticle (articleIndex);

    async.waterfall([
            function (cb) { // read dir entry
                getDirEntry (article, cb);
            },
            moveArticle,
            parseArticle,
            alterLinks,
            saveArticle
        ],
        function (err) {
            if(!err || err == 'finished')
                return callback();
            console.error('processArticle error', article.index, header.articleCount, err, article);
            callback();
            //~ callback(err);
        }
    );
}

var directoryBuffer;

function newArticle (articleIndex) {
    return {
        index: articleIndex,
        offset: readUInt64LE(directoryBuffer, articleIndex * 8)
    };
}

function processDirectoryList (buf, read, position, callback) {

    directoryBuffer = buf;
    articleUrls = Array(header.articleCount);

    //~ log( 'articleOffsets', articleOffsets);

    var i = -1;

    async.whilst(
        function () {
            i++;
            return i < header.articleCount;
        },
        function (cb) {
            processArticle(i, cb);
        },
        function (err, res) {
            directoryBuffer = null;
            log('processDirectoryList', err);
            callback(err);
        }
    );
}

var redirects = null;

function storeRedirect (article, callback) {
    log('storeRedirect', article);

    if (article.nameSpace == '-' && (article.url == 'favicon' || article.url == 'mainPage'))
        return callback();

    var target = articleUrls[article.redirectIndex];

    var item = [article.nameSpace, article.url, article.title, target];

    log('storeRedirect', item);

    if (! target)
        return processArticle(article.redirectIndex, storeRedirect.bind(this, article, callback));

    if (! redirects) {
        redirects = csvOutput({delimiter: '\t'});
        redirects.pipe(fs.createWriteStream(osPath.join(outPath, '..', 'redirects.csv')));
    }

    var write = function () {
        if (! redirects.write(item))
            return redirects.once('drain', write);

        callback();
    };

    write();
}

function storeMetadata (callback) {
    log('storeMetadata');
    if (metadata.length == 0)
        return callback();

    var csv = csvOutput({delimiter: ' '});
    csv.pipe(fs.createWriteStream(osPath.join(outPath, '..', 'metadata.csv')));

    var i = 0;
    var write = function () {
        while (true) {
            if (i == metadata.length) {
                log('storeMetadata finished');
                return csv.end(callback);
            }
            var item = metadata[i];
            log('storeMetadata', metadata.length, i, item);
            if (! csv.write(item))
                break;
            i++
        }
        csv.once('drain', write);
    };

    write();
};

var argv;

function main () {
    argv = yargs.usage( 'Pack a directory into a zim file\nUsage: $0'
               + '\nExample: $0 ' )
        .options({
            'v': {alias: 'verbose', type:'boolean'},
            'x': {alias: 'deflateHtml', type:'boolean'},
            'r': {alias: 'redirects', default: ''},
            'h': {alias: 'help'}
        })
        .help('help')
    //    .strict()
        .argv;

    log(argv);

    srcPath = expandHomeDir(argv._[0]);
    if (argv._[1])
        outPath = expandHomeDir(argv._[1])
    else {
        var parsed = osPath.parse(srcPath);
        outPath = parsed.name;
    }

    async.waterfall([
            function (cb) { // init Reader
                src = new Reader(srcPath, cb);
            },
            function (cb) { // read header
                log('reading header');
                src.read(headerLength, 0, cb);
            },
            parseHeader,
            function (cb) {
                log('reading ClusterPointers');
                src.read(header.clusterCount * 8, header.clusterPtrPos, cb);
            },
            processClusterList,
            function (cb) {
                log('reading DirectoryPointers');
                src.read(header.articleCount * 8, header.urlPtrPos, cb);
            },
            processDirectoryList,
            storeMetadata,
            function (callback) {
                if (! redirects)
                    return callback();
                return redirects.end(callback);
            },
            function (cb) {
                src.close(cb);
            }
        ],
        function (err) {
            if (err)
                return console.error(err);
            log('........');
            //~ process.exit(0);
        }
    );
}

main ();
