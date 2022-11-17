
Basic functionality
- Add support for non direct conversion (i.e int to time.Time, int to string etc ...)
- Add support for time.Time in criterion 
- Add support to conversion in criterion 
- Add support to multi level output
- Add IN/NOT IN translation to go expression

Extended functionality
- Add query options
  - at hoc select
  - indexed data select

- Add multi level output (currently mapper work ony leaf level)
- Add SQL function support
  - COALESCE
  - DATE (FORMAT/SUB/ADD)
  - CURRENT_TIMESTAMP
  - UNNEST
  - 
- Add GROUP BY support
  - ARRAY_AGG
  - STRING_AGG
  