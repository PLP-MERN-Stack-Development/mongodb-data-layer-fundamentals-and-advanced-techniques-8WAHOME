// queries.js
// Run with: mongosh "mongodb://127.0.0.1:27017/plp_bookstore" --file queries.js
// Or open mongosh, run: use plp_bookstore, then paste commands below.

// -------------------------
// Task 1: Ensure DB & Collection
// -------------------------
use plp_bookstore;
db.createCollection("books"); // safe to run: if exists it will throw; it's optional because insert will auto-create

// -------------------------
// Task 2: Basic CRUD Operations
// -------------------------

// A) Find all books in a specific genre (example: Fantasy)
db.books.find({ genre: "Fantasy" }).pretty();

// With projection to show only title, author, price
db.books.find({ genre: "Fantasy" }, { title: 1, author: 1, price: 1, _id: 0 }).pretty();

// B) Find books published after a certain year (example: after 2015)
db.books.find({ published_year: { $gt: 2015 } }).pretty();

// C) Find books by a specific author (example: Maria Njeri)
db.books.find({ author: "Maria Njeri" }).pretty();

// D) Update the price of a specific book (example: update "The Silent River" price to 14.99)
db.books.updateOne(
  { title: "The Silent River" },
  { $set: { price: 14.99 } }
);

// Verify update
db.books.find({ title: "The Silent River" }, { title: 1, price: 1, _id: 0 }).pretty();

// E) Delete a book by its title (example: delete "Rust and Roses")
db.books.deleteOne({ title: "Rust and Roses" });

// Verify deletion
db.books.find({ title: "Rust and Roses" }).pretty();

// -------------------------
// Task 3: Advanced Queries
// -------------------------

// A) Find books that are both in stock and published after 2010
db.books.find(
  { in_stock: true, published_year: { $gt: 2010 } },
  { title: 1, author: 1, published_year: 1, _id: 0 }
).pretty();

// B) Projection only title, author, price for all books
db.books.find({}, { title: 1, author: 1, price: 1, _id: 0 }).pretty();

// C) Sorting by price ascending
db.books.find({}, { title: 1, price: 1, _id: 0 }).sort({ price: 1 }).pretty();

// Sorting by price descending
db.books.find({}, { title: 1, price: 1, _id: 0 }).sort({ price: -1 }).pretty();

// D) Pagination (5 books per page). Example helper — change page variable
const pageSize = 5;
function showPage(page) {
  const skip = (page - 1) * pageSize;
  return db.books.find({}, { title: 1, author: 1, price: 1, _id: 0 })
    .sort({ title: 1 })
    .skip(skip)
    .limit(pageSize)
    .toArray();
}

// Example: show page 1 and page 2
print('--- Page 1 ---'); printjson(showPage(1));
print('--- Page 2 ---'); printjson(showPage(2));

// -------------------------
// Task 4: Aggregation Pipeline
// -------------------------

// A) Average price of books by genre
db.books.aggregate([
  {
    $group: {
      _id: "$genre",
      avgPrice: { $avg: "$price" },
      count: { $sum: 1 }
    }
  },
  { $sort: { avgPrice: -1 } }
]).pretty();

// B) Find the author with the most books in the collection
db.books.aggregate([
  { $group: { _id: "$author", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 1 }
]).pretty();

// C) Group books by publication decade and count them
db.books.aggregate([
  {
    $project: {
      title: 1,
      published_year: 1,
      decade: { $subtract: [ "$published_year", { $mod: [ "$published_year", 10 ] } ] }
    }
  },
  {
    $group: {
      _id: "$decade",
      count: { $sum: 1 }
    }
  },
  { $sort: { _id: 1 } }
]).pretty();

// -------------------------
// Task 5: Indexing & explain()
// -------------------------

// A) Create an index on the title field for faster searches
db.books.createIndex({ title: 1 });

// B) Create a compound index on author and published_year (author asc, year desc)
db.books.createIndex({ author: 1, published_year: -1 });

// C) Use explain() to demonstrate performance improvement
// Before running explain: (if you want a baseline, temporarily drop indexes; ONLY do this on dev/test)
// db.books.dropIndexes(); // WARNING: Do not drop indexes on production collections.

// Example explain for searching by title (recommended use "executionStats")
print("=== explain BEFORE creating index (if you want to compare) ===");
printjson(db.books.find({ title: "The Silent River" }).explain("executionStats"));

// After index created (run the createIndex commands above), re-run explain:
print("=== explain AFTER creating index on title ===");
printjson(db.books.find({ title: "The Silent River" }).explain("executionStats"));

// Look at executionStats.executionTimeMillis, totalDocsExamined, totalKeysExamined to compare

// -------------------------
// End of queries.js
// -------------------------
