# Bucket Format Specification

TODO: make this less bad

A bucket consists of a series of concatenated transactions.
Each transaction consists of a series of key-value pairs (where keys must all be unique) which have been modified.

## Transaction Format

A transaction consists of a series of key-value pairs.
Each key-value pair is stored as follows:
- A non-zero variable-width integer indicating the size of the key
- A variable-width integer indicating the size of the value
- The key data
- The value data

After the last pair of the transaction, a zero integer is used to mark the termination of the transaction.

If a zero-sized value is specified, the pair is treated as a deletion.

A transaction must not contain multiple pairs with the same key.
If there are duplicate pairs with the same key, the reader must produce an error.

If a transaction is not complete (data is missing), then the reader must ignore it (and truncate it away before writing).

## Variable-width integer format

The bucket stores integers in a variable-width little-endian format.
The maximum supported value is 2^30-1.

The integer is stored in 1-4 bytes.
The bottom 2 bits of the first byte are used to store the number of following bytes.
All remaining bits are used to encode the number.
