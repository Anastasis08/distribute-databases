// Executed once by the official mongo image when the data volume is empty.
const dbName = process.env.MONGO_APP_DATABASE || 'product_catalog';
const appUser = process.env.MONGO_APP_USERNAME || 'sync_app';
const appPassword = process.env.MONGO_APP_PASSWORD || 'sync_app_pwd';

const catalog = db.getSiblingDB(dbName);

// Least-privilege account for the consumer service.
catalog.createUser({
  user: appUser,
  pwd: appPassword,
  roles: [{ role: 'readWrite', db: dbName }],
});

// Read model of the catalog. _id is the stable SQL identifier (products.id).
catalog.createCollection('products', {
  validator: {
    $jsonSchema: {
      bsonType: 'object',
      required: ['_id', 'sku', 'name', 'category', 'price', 'quantity', 'createdAt', 'updatedAt', 'sync'],
      properties: {
        _id: { bsonType: ['int', 'long'], description: 'products.id from PostgreSQL' },
        sku: { bsonType: 'string' },
        name: { bsonType: 'string' },
        category: { bsonType: 'string' },
        price: { bsonType: 'decimal' },
        quantity: { bsonType: ['int', 'long'], minimum: 0 },
        createdAt: { bsonType: 'date' },
        updatedAt: { bsonType: 'date' },
        sync: {
          bsonType: 'object',
          required: ['version', 'eventId', 'syncedAt'],
          properties: {
            version: { bsonType: ['int', 'long'], description: 'updated_at in microseconds; guards against stale writes' },
            eventId: { bsonType: 'string' },
            syncedAt: { bsonType: 'date' },
          },
        },
      },
    },
  },
  validationLevel: 'strict',
  validationAction: 'error',
});

catalog.products.createIndex({ sku: 1 }, { name: 'ix_sku' });
catalog.products.createIndex({ category: 1 }, { name: 'ix_category' });
