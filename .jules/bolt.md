
## 2023-10-19 - Cassandra Prepared Statements Unnecessary Roundtrips
**Learning:** Re-preparing the same statements dynamically on every function call triggers unnecessary network roundtrips to the Cassandra cluster, leading to performance bottlenecks when querying.
**Action:** Always maintain a static cache or globally map prepared statements on application startup / first usage when working with cassandra-driver so it only gets prepared once per session.
