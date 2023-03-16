'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var Y = require('yjs');
var binary = require('lib0/dist/binary.cjs');
var promise = require('lib0/dist/promise.cjs');
var mongojs = require('mongojs');
var mongoist = require('mongoist');
var encoding = require('lib0/dist/encoding.cjs');
var decoding = require('lib0/dist/decoding.cjs');
var buffer = require('buffer');

function _interopDefaultLegacy (e) { return e && typeof e === 'object' && 'default' in e ? e : { 'default': e }; }

function _interopNamespace(e) {
	if (e && e.__esModule) return e;
	var n = Object.create(null);
	if (e) {
		Object.keys(e).forEach(function (k) {
			if (k !== 'default') {
				var d = Object.getOwnPropertyDescriptor(e, k);
				Object.defineProperty(n, k, d.get ? d : {
					enumerable: true,
					get: function () { return e[k]; }
				});
			}
		});
	}
	n["default"] = e;
	return Object.freeze(n);
}

var Y__namespace = /*#__PURE__*/_interopNamespace(Y);
var binary__namespace = /*#__PURE__*/_interopNamespace(binary);
var promise__namespace = /*#__PURE__*/_interopNamespace(promise);
var mongojs__default = /*#__PURE__*/_interopDefaultLegacy(mongojs);
var mongoist__default = /*#__PURE__*/_interopDefaultLegacy(mongoist);
var encoding__namespace = /*#__PURE__*/_interopNamespace(encoding);
var decoding__namespace = /*#__PURE__*/_interopNamespace(decoding);

class MongoAdapter {
	/**
	 * Create a MongoAdapter instance.
	 * @param {string} location
	 * @param {object} [opts]
	 * @param {string} [opts.collection] Name of the collection where all documents are stored.
	 * @param {boolean} [opts.multipleCollections] When set to true, each document gets an own
	 * @param {(string)=>string)} [opts.collectionNameCallback] When set, custom collection name
	 * collection (instead of all documents stored in the same one).
	 * When set to true, the option $collection gets ignored.
	 */
	constructor(location, { collection, multipleCollections, collectionNameCallback = null }) {
		this.location = location;
		this.collection = collection;
		this.multipleCollections = multipleCollections;
		this.db = null;
		this.collectionNameCallback = collectionNameCallback;
		this.open();
	}

	/**
	 * Open the connection to MongoDB instance.
	 */
	open() {
		let mongojsDb;
		if (this.multipleCollections) {
			mongojsDb = mongojs__default["default"](this.location);
		} else {
			mongojsDb = mongojs__default["default"](this.location, [this.collection]);
		}
		this.db = mongoist__default["default"](mongojsDb);
	}

	/**
	 * Get the MongoDB collection name for any docName
	 * @param {object} [opts]
	 * @param {string} [opts.docName]
	 * @returns {string} collectionName
	 */
	_getCollectionName({ docName }) {
		if (this.multipleCollections) {
			if (this.collectionNameCallback) {
				return this.collectionNameCallback(docName);
			} else {
				return docName;
			}
		} else {
			return this.collection;
		}
	}

	/**
	 * Apply a $query and get one document from MongoDB.
	 * @param {object} query
	 * @returns {Promise<object>}
	 */
	get(query) {
		return this.db[this._getCollectionName(query)].findOne(query);
	}

	/**
	 * Store one document in MongoDB.
	 * @param {object} query
	 * @param {object} values
	 * @returns {Promise<object>} Stored document
	 */
	put(query, values) {
		if (!query.docName || !query.version || !values.value) {
			throw new Error('Document and version must be provided');
		}

		// findAndModify with upsert:true should simulate leveldb put better
		return this.db[this._getCollectionName(query)].findAndModify({
			query,
			update: { ...query, ...values },
			upsert: true,
			new: true,
		});
	}

	/**
	 * Removes all documents that fit the $query
	 * @param {object} query
	 * @returns {Promise<object>} Contains status of the operation
	 */
	del(query) {
		const bulk = this.db[this._getCollectionName(query)].initializeOrderedBulkOp();
		bulk.find(query).remove();
		return bulk.execute();
	}

	/**
	 * Get all or at least $opts.limit documents that fit the $query.
	 * @param {object} query
	 * @param {object} [opts]
	 * @param {number} [opts.limit]
	 * @param {boolean} [opts.reverse]
	 * @returns {Promise<Array<object>>}
	 */
	readAsCursor(query, { limit, reverse } = {}) {
		let curs = this.db[this._getCollectionName(query)].findAsCursor(query);
		if (reverse) curs = curs.sort({ clock: -1 });
		if (limit) curs = curs.limit(limit);
		return curs.toArray();
	}

	/**
	 * Close connection to MongoDB instance.
	 */
	close() {
		this.db.close();
	}

	/**
	 * Get all collection names stored on the MongoDB instance.
	 * @returns {Promise<Array<string>>}
	 */
	getCollectionNames() {
		return this.db.getCollectionNames();
	}

	/**
	 * Delete database
	 */
	async flush() {
		await this.db.dropDatabase();
		await this.db.close();
	}

	/**
	 * Delete collection
	 * @param {string} collectionName
	 */
	dropCollection(collectionName) {
		return this.db[collectionName].drop();
	}
}

const PREFERRED_TRIM_SIZE = 400;

/**
 * Remove all documents from db with Clock between $from and $to
 *
 * @param {any} db
 * @param {string} docName
 * @param {number} from Greater than or equal
 * @param {number} to lower than (not equal)
 * @return {Promise<void>}
 */
const clearUpdatesRange = async (db, docName, from, to) => db.del({
	docName,
	clock: {
		$gte: from,
		$lt: to,
	},
});

/**
 * Create a unique key for a update message.
 * @param {string} docName
 * @param {number} clock must be unique
 * @return {Object} [opts.version, opts.docName, opts.action, opts.clock]
 */
const createDocumentUpdateKey = (docName, clock) => {
	if (clock !== undefined) {
		return {
			version: 'v1',
			action: 'update',
			docName,
			clock,
		};
	} else {
		return {
			version: 'v1',
			action: 'update',
			docName,
		};
	}
};

/**
 * We have a separate state vector key so we can iterate efficiently over all documents
 * @param {string} docName
 * @return {Object} [opts.docName, opts.version]
 */
const createDocumentStateVectorKey = (docName) => ({
	docName,
	version: 'v1_sv',
});

/**
 * @param {string} docName
 * @param {string} metaKey
 * @return {Object} [opts.docName, opts.version, opts.docType, opts.metaKey]
 */
const createDocumentMetaKey = (docName, metaKey) => ({
	version: 'v1',
	docName,
	metaKey: `meta_${metaKey}`,
});

/**
 * @param {any} db
 * @param {object} query
 * @param {object} opts
 * @return {Promise<Array<any>>}
 */
const getMongoBulkData = (db, query, opts) => db.readAsCursor(query, opts);

/**
 * @param {any} db
 * @return {Promise<any>}
 */
const flushDB = (db) => db.flush();

/**
 * Convert the mongo document array to an array of values (as buffers)
 *
 * @param {<Array<Object>>} docs
 * @return {<Array<Buffer>>}
 */
const _convertMongoUpdates = (docs) => {
	if (!Array.isArray(docs) || !docs.length) return [];

	return docs.map((update) => update.value.buffer);
};
/**
 * Get all document updates for a specific document.
 *
 * @param {any} db
 * @param {string} docName
 * @param {any} [opts]
 * @return {Promise<Array<Object>>}
 */
const getMongoUpdates = async (db, docName, opts = {}) => {
	const docs = await getMongoBulkData(db, createDocumentUpdateKey(docName), opts);
	return _convertMongoUpdates(docs);
};

/**
 * @param {any} db
 * @param {string} docName
 * @return {Promise<number>} Returns -1 if this document doesn't exist yet
 */
const getCurrentUpdateClock = (db, docName) => getMongoBulkData(
	db,
	{
		...createDocumentUpdateKey(docName, 0),
		clock: {
			$gte: 0,
			$lt: binary__namespace.BITS32,
		},
	},
	{ reverse: true, limit: 1 },
).then((updates) => {
	if (updates.length === 0) {
		return -1;
	} else {
		return updates[0].clock;
	}
});

/**
 * @param {any} db
 * @param {string} docName
 * @param {Uint8Array} sv state vector
 * @param {number} clock current clock of the document so we can determine
 * when this statevector was created
 */
const writeStateVector = async (db, docName, sv, clock) => {
	const encoder = encoding__namespace.createEncoder();
	encoding__namespace.writeVarUint(encoder, clock);
	encoding__namespace.writeVarUint8Array(encoder, sv);
	await db.put(createDocumentStateVectorKey(docName), {
		value: buffer.Buffer.from(encoding__namespace.toUint8Array(encoder)),
	});
};

/**
 * @param {any} db
 * @param {string} docName
 * @param {Uint8Array} update
 * @return {Promise<number>} Returns the clock of the stored update
 */
const storeUpdate = async (db, docName, update) => {
	const clock = await getCurrentUpdateClock(db, docName);
	if (clock === -1) {
		// make sure that a state vector is always written, so we can search for available documents
		const ydoc = new Y__namespace.Doc();
		Y__namespace.applyUpdate(ydoc, update);
		const sv = Y__namespace.encodeStateVector(ydoc);
		await writeStateVector(db, docName, sv, 0);
	}

	await db.put(createDocumentUpdateKey(docName, clock + 1), {
		value: buffer.Buffer.from(update),
	});

	return clock + 1;
};

/**
 * For now this is a helper method that creates a Y.Doc and then re-encodes a document update.
 * In the future this will be handled by Yjs without creating a Y.Doc (constant memory consumption).
 *
 * @param {Array<Uint8Array>} updates
 * @return {{update:Uint8Array, sv: Uint8Array}}
 */
const mergeUpdates = (updates) => {
	const ydoc = new Y__namespace.Doc();
	ydoc.transact(() => {
		for (let i = 0; i < updates.length; i++) {
			Y__namespace.applyUpdate(ydoc, updates[i]);
		}
	});
	return { update: Y__namespace.encodeStateAsUpdate(ydoc), sv: Y__namespace.encodeStateVector(ydoc) };
};

/**
 * @param {Uint8Array} buf
 * @return {{ sv: Uint8Array, clock: number }}
 */
const decodeMongodbStateVector = (buf) => {
	let decoder;
	if (buffer.Buffer.isBuffer(buf)) {
		decoder = decoding__namespace.createDecoder(buf);
	} else if (buffer.Buffer.isBuffer(buf?.buffer)) {
		decoder = decoding__namespace.createDecoder(buf.buffer);
	} else {
		throw new Error('No buffer provided at decodeMongodbStateVector()');
	}
	const clock = decoding__namespace.readVarUint(decoder);
	const sv = decoding__namespace.readVarUint8Array(decoder);
	return { sv, clock };
};

/**
 * @param {any} db
 * @param {string} docName
 */
const readStateVector = async (db, docName) => {
	const doc = await db.get({ ...createDocumentStateVectorKey(docName) });
	if (!doc?.value) {
		// no state vector created yet or no document exists
		return { sv: null, clock: -1 };
	}
	return decodeMongodbStateVector(doc.value);
};

const getAllSVDocs = async (db) => db.readAsCursor({ version: 'v1_sv' });

/**
 * Merge all MongoDB documents of the same yjs document together.
 * @param {any} db
 * @param {string} docName
 * @param {Uint8Array} stateAsUpdate
 * @param {Uint8Array} stateVector
 * @return {Promise<number>} returns the clock of the flushed doc
 */
const flushDocument = async (db, docName, stateAsUpdate, stateVector) => {
	const clock = await storeUpdate(db, docName, stateAsUpdate);
	await writeStateVector(db, docName, stateVector, clock);
	await clearUpdatesRange(db, docName, 0, clock);
	return clock;
};

class MongodbPersistence {
	/**
	 * Create a y-mongodb persistence instance.
	 * @param {string} location The connection string for the MongoDB instance.
	 * @param {object} [opts=] Additional optional parameters.
	 * @param {string} [opts.collectionName="yjs-writings"] Name of the collection where all
	 * documents are stored. Default: "yjs-writings"
	 * @param {boolean} [opts.multipleCollections=false] When set to true, each document gets
	 * an own collection (instead of all documents stored in the same one). When set to true,
	 * the option collectionName gets ignored. Default: false
	 * @param {number} [opts.flushSize=400] The number of stored transactions needed until
	 * they are merged automatically into one Mongodb document. Default: 400
	 * @param {(string)=>string)} [opts.collectionNameCallback] When set, custom collection name
	 */
	constructor(location, { collectionName = 'yjs-writings', multipleCollections = false, flushSize = 400, collectionNameCallback = nul } = {}) {
		if (typeof collectionName !== 'string' || !collectionName) {
			throw new Error('Constructor option "collectionName" is not a valid string. Either dont use this option (default is "yjs-writings") or use a valid string! Take a look into the Readme for more information: https://github.com/MaxNoetzold/y-mongodb-provider#persistence--mongodbpersistenceconnectionlink-string-options-object');
		}
		if (typeof multipleCollections !== 'boolean') {
			throw new Error('Constructor option "multipleCollections" is not a boolean. Either dont use this option (default is "false") or use a valid boolean! Take a look into the Readme for more information: https://github.com/MaxNoetzold/y-mongodb-provider#persistence--mongodbpersistenceconnectionlink-string-options-object');
		}
		if (typeof flushSize !== 'number' || flushSize <= 0) {
			throw new Error('Constructor option "flushSize" is not a valid number. Either dont use this option (default is "400") or use a valid number larger than 0! Take a look into the Readme for more information: https://github.com/MaxNoetzold/y-mongodb-provider#persistence--mongodbpersistenceconnectionlink-string-options-object');
		}
		const db = new MongoAdapter(location, {
			collection: collectionName,
			multipleCollections,
			collectionNameCallback,
		});
		this.flushSize = flushSize ?? PREFERRED_TRIM_SIZE;
		this.multipleCollections = multipleCollections;

		// scope the queue of the transaction to each docName
		// -> this should allow concurrency for different rooms
		// Idea and adjusted code from: https://github.com/fadiquader/y-mongodb/issues/10
		this.tr = {};

		/**
		 * Execute an transaction on a database. This will ensure that other processes are
		 * currently not writing.
		 *
		 * This is a private method and might change in the future.
		 *
		 * @template T
		 *
		 * @param {function(any):Promise<T>} f A transaction that receives the db object
		 * @return {Promise<T>}
		 */
		this._transact = (docName, f) => {
			if (!this.tr[docName]) {
				this.tr[docName] = promise__namespace.resolve();
			}

			const currTr = this.tr[docName];

			this.tr[docName] = (async () => {
				await currTr;

				let res = /** @type {any} */ (null);
				try {
					res = await f(db);
				} catch (err) {
					console.warn('Error during saving transaction', err);
				}
				return res;
			})();
			return this.tr[docName];
		};
	}

	/**
	 * Create a Y.Doc instance with the data persistet in mongodb.
	 * Use this to temporarily create a Yjs document to sync changes or extract data.
	 *
	 * @param {string} docName
	 * @return {Promise<Y.Doc>}
	 */
	getYDoc(docName) {
		return this._transact(docName, async (db) => {
			const updates = await getMongoUpdates(db, docName);
			const ydoc = new Y__namespace.Doc();
			ydoc.transact(() => {
				for (let i = 0; i < updates.length; i++) {
					Y__namespace.applyUpdate(ydoc, updates[i]);
				}
			});
			if (updates.length > this.flushSize) {
				await flushDocument(db, docName, Y__namespace.encodeStateAsUpdate(ydoc), Y__namespace.encodeStateVector(ydoc));
			}
			return ydoc;
		});
	}

	/**
	 * Store a single document update to the database.
	 *
	 * @param {string} docName
	 * @param {Uint8Array} update
	 * @return {Promise<number>} Returns the clock of the stored update
	 */
	storeUpdate(docName, update) {
		return this._transact(docName, (db) => storeUpdate(db, docName, update));
	}

	/**
	 * The state vector (describing the state of the persisted document - see https://github.com/yjs/yjs#Document-Updates) is maintained in a separate field and constantly updated.
	 *
	 * This allows you to sync changes without actually creating a Yjs document.
	 *
	 * @param {string} docName
	 * @return {Promise<Uint8Array>}
	 */
	getStateVector(docName) {
		return this._transact(docName, async (db) => {
			const { clock, sv } = await readStateVector(db, docName);
			let curClock = -1;
			if (sv !== null) {
				curClock = await getCurrentUpdateClock(db, docName);
			}
			if (sv !== null && clock === curClock) {
				return sv;
			} else {
				// current state vector is outdated
				const updates = await getMongoUpdates(db, docName);
				const { update, sv: newSv } = mergeUpdates(updates);
				await flushDocument(db, docName, update, newSv);
				return newSv;
			}
		});
	}

	/**
	 * Get the differences directly from the database.
	 * The same as Y.encodeStateAsUpdate(ydoc, stateVector).
	 * @param {string} docName
	 * @param {Uint8Array} stateVector
	 */
	async getDiff(docName, stateVector) {
		const ydoc = await this.getYDoc(docName);
		return Y__namespace.encodeStateAsUpdate(ydoc, stateVector);
	}

	/**
	 * Delete a document, and all associated data from the database.
	 * When option multipleCollections is set, it removes the corresponding collection
	 * @param {string} docName
	 * @return {Promise<void>}
	 */
	clearDocument(docName) {
		return this._transact(docName, async (db) => {
			if (!this.multipleCollections) {
				await db.del(createDocumentStateVectorKey(docName));
				await clearUpdatesRange(db, docName, 0, binary__namespace.BITS32);
			} else {
				await db.dropCollection(docName);
			}
		});
	}

	/**
	 * Persist some meta information in the database and associate it
	 * with a document. It is up to you what you store here.
	 * You could, for example, store credentials here.
	 *
	 * @param {string} docName
	 * @param {string} metaKey
	 * @param {any} value
	 * @return {Promise<void>}
	 */
	setMeta(docName, metaKey, value) {
		/*	Unlike y-leveldb, we simply store the value here without encoding
	 		 it in a buffer beforehand. */
		return this._transact(docName, async (db) => {
			await db.put(createDocumentMetaKey(docName, metaKey), { value });
		});
	}

	/**
	 * Retrieve a store meta value from the database. Returns undefined if the
	 * metaKey doesn't exist.
	 *
	 * @param {string} docName
	 * @param {string} metaKey
	 * @return {Promise<any>}
	 */
	getMeta(docName, metaKey) {
		return this._transact(docName, async (db) => {
			const res = await db.get({ ...createDocumentMetaKey(docName, metaKey) });
			if (!res?.value) {
				return undefined;
			}
			return res.value;
		});
	}

	/**
	 * Delete a store meta value.
	 *
	 * @param {string} docName
	 * @param {string} metaKey
	 * @return {Promise<any>}
	 */
	delMeta(docName, metaKey) {
		return this._transact(docName, (db) => db.del({
			...createDocumentMetaKey(docName, metaKey),
		}));
	}

	/**
	 * Retrieve the names of all stored documents.
	 *
	 * @return {Promise<Array<string>>}
	 */
	getAllDocNames() {
		return this._transact('global', async (db) => {
			if (this.multipleCollections) {
				// get all collection names from db
				return db.getCollectionNames();
			} else {
				// when all docs are stored in the same collection we just need to get all
				//  statevectors and return their names
				const docs = await getAllSVDocs(db);
				return docs.map((doc) => doc.docName);
			}
		});
	}

	/**
	 * Retrieve the state vectors of all stored documents.
	 * You can use this to sync two y-leveldb instances.
	 * !Note: The state vectors might be outdated if the associated document
	 * is not yet flushed. So use with caution.
	 * @return {Promise<Array<{ name: string, sv: Uint8Array, clock: number }>>}
	 * @todo may not work?
	 */
	getAllDocStateVectors() {
		return this._transact('global', async (db) => {
			const docs = await getAllSVDocs(db);
			return docs.map((doc) => {
				const { sv, clock } = decodeMongodbStateVector(doc.value);
				return { name: doc.docName, sv, clock };
			});
		});
	}

	/**
	 * Internally y-mongodb stores incremental updates. You can merge all document
	 * updates to a single entry. You probably never have to use this.
	 * It is done automatically every $options.flushsize (default 400) transactions.
	 *
	 * @param {string} docName
	 * @return {Promise<void>}
	 */
	flushDocument(docName) {
		return this._transact(docName, async (db) => {
			const updates = await getMongoUpdates(db, docName);
			const { update, sv } = mergeUpdates(updates);
			await flushDocument(db, docName, update, sv);
		});
	}

	/**
	 * Delete the whole yjs mongodb
	 * @return {Promise<void>}
	 */
	flushDB() {
		return this._transact('global', async (db) => {
			await flushDB(db);
		});
	}
}

exports.MongodbPersistence = MongodbPersistence;
//# sourceMappingURL=y-mongodb.cjs.map
