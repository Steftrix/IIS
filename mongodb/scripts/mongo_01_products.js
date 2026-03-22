// ============================================================
//  IIS Project — MongoDB 6 (DS_4)
//  Script 01: Import products + optimise schema
//
//  Steps:
// 1.
// docker exec iis-mongodb mongoimport `
//  --username iis_admin --password iis_pass --authenticationDatabase admin `
//  --db iis_db --collection products `
//  --type csv --headerline `
//  --file /csv/products.csv
// 2.
// Get-Content mongodb/scripts/mongo_01_products.js | docker exec -i iis-mongodb mongosh --username iis_admin --password iis_pass --authenticationDatabase admin
// ============================================================

const db = db.getSiblingDB('iis_db');
const col = db.products;

print("=== Step 1: Checking document count after mongoimport ===");
print("Documents in collection: " + col.countDocuments());

// ── Step 2: Parse attributes string → native BSON subdocument ─
// mongoimport loads CSV fields as strings. The attributes column
// contains JSON like: {"material": "100% linen", "colours": ["black"]}
// We parse it so MongoDB can index into attribute fields directly.

print("\n=== Step 2: Parsing attributes to native BSON ===");

let parsed = 0;
let failed = 0;

col.find({ attributes: { $type: "string" } }).forEach(doc => {
    try {
        const attrObj = JSON.parse(doc.attributes);
        col.updateOne(
            { _id: doc._id },
            { $set: { attributes: attrObj } }
        );
        parsed++;
    } catch (e) {
        print("  Failed to parse attributes for _id: " + doc._id + " — " + e.message);
        failed++;
    }
});

print("  Parsed: " + parsed + " documents");
print("  Failed: " + failed + " documents");


// ── Step 3: Fix scalar field types ───────────────────────────
print("\n=== Step 3: Fixing scalar field types ===");

col.find({}).forEach(doc => {
    const updates = {};

    // price_usd: string → Double
    if (typeof doc.price_usd === "string") {
        updates.price_usd = parseFloat(doc.price_usd);
    }

    // is_active: "True"/"False" string → Boolean
    if (typeof doc.is_active === "string") {
        updates.is_active = doc.is_active.toLowerCase() === "true";
    }

    // created_at / updated_at: ISO string → Date
    if (typeof doc.created_at === "string") {
        updates.created_at = new Date(doc.created_at);
    }
    if (typeof doc.updated_at === "string") {
        updates.updated_at = new Date(doc.updated_at);
    }

    if (Object.keys(updates).length > 0) {
        col.updateOne({ _id: doc._id }, { $set: updates });
    }
});

print("  Scalar types fixed.");


// ── Step 4: Create indexes ────────────────────────────────────
print("\n=== Step 4: Creating indexes ===");

// Text index on name + description for full-text search
col.createIndex(
    { name: "text", description: "text" },
    { name: "idx_products_text_search", weights: { name: 10, description: 5 } }
);
print("  Created: text index on (name, description)");

// Compound index for seller queries sorted by newest first
col.createIndex(
    { seller_id: 1, created_at: -1 },
    { name: "idx_products_seller_created" }
);
print("  Created: compound index (seller_id, created_at DESC)");

// Index for filtering by product type
col.createIndex(
    { product_type: 1 },
    { name: "idx_products_type" }
);
print("  Created: index on product_type");

// Index for price range queries
col.createIndex(
    { price_usd: 1 },
    { name: "idx_products_price" }
);
print("  Created: index on price_usd");

// Index into attributes subdocument — now possible because
// attributes is a native BSON subdocument, not a string
col.createIndex(
    { "attributes.requires_shipping": 1 },
    { name: "idx_products_requires_shipping" }
);
print("  Created: index on attributes.requires_shipping");


// ── Step 5: Verify ────────────────────────────────────────────
print("\n=== Step 5: Verification ===");
print("Total documents: " + col.countDocuments());
print("Active products: " + col.countDocuments({ is_active: true }));
print("\nSample document (first product):");
printjson(col.findOne({}, {
    name: 1, price_usd: 1, is_active: 1,
    product_type: 1, attributes: 1, _id: 0
}));

print("\nIndexes created:");
col.getIndexes().forEach(idx => print("  - " + idx.name));
print("\n=== Done ===");
