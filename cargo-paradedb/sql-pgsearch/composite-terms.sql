SELECT process->>'name' AS process_name,
       cloud->>'region' AS cloud_region,
       COUNT(*) AS doc_count
FROM benchmark_eslogs
WHERE benchmark_eslogs @@@ paradedb.range(
  field => 'timestamp',
  range => '[2023-01-02T00:00:00Z,2023-01-02T10:00:00Z)'::tstzrange
)
GROUP BY 1, 2
ORDER BY process_name DESC, cloud_region ASC;
