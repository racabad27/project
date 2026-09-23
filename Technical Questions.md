15. Technical Questions
1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it? ``(Example was provided Gemini AI)``
- ***record_hash provides a unique deterministic fingerprint of a record's underlying business content. It allows the database loader to execute conditional upserts such as (ON CONFLICT ... WHERE record_hash IS DISTINCT FROM EXCLUDED.record_hash) in order to update rows only if teh actual payload changes, ensuring a safe rerun. With that said, including it with volatile metadata is not a good idea since it can make it so that the hash changes on every execution.***

2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?
- Raw data is should always be preserved because it serves as a "Origin" point or a reference point from where you started from. It is the undisputable truth/fact. Preserving raw data allows for reingestion, reparsing, and other necessary changes that can need to be addressed to satisfy the current business rules.

3. What is the difference between a data-quality rejection and a system exception?
- A data-quality rejection is caused by having dirty or invalid payload records such as (missing customer_ids, negative prices). This is why it was an appropriate move to create the quarantine table, where invalid records end up in, while the rest proceed as normal. On the other hand, a system exception refers to an infrastructure problem, one that comes in many forms like not being able to connect to the DB's, memory issues, missing files. 

4. Why might Parquet outperform CSV for 
selected analytical workloads even if both contain the same rows?
- Parquet has a combination of columnar setup and compressions that allow it to support analytical queries for easier scanning and reading, unlike CSV's which are gone through line-by-line.

5. Why is a DAG that contains all transformation logic directly considered harder to maintain?
- It can become huge wall of code with combined logic from SQL, Python, and etc. Making changes that are considered "Small" are still considered big since they are linked to other functions as well, which is an addition to think about other than what the person wants to change. 

6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.
- Retries basically depend on idempotency to safely rereun tasks after failing. An example would be appending rows to a DB using insert statements without constraints or deduplication.

7. What trade-off is introduced by partitioning too aggressively?
- Partitioning too aggressively causes too many bits or pieces of files to be created. The engine running will have to inspect and open each and every one of those, which despite being small, will still take lots of valuable time.
8. How would you adapt the pipeline if the source became an API or database instead of local files?
- I would replace the file-reading logic part with an HTTP client or DB connection, and introduce watermark parameters for extracting new or modified data. The rest of the downstream processes will remain as is.