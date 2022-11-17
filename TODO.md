
Basic functionality
- Add support for non direct conversion (i.e int to time.Time, int to string etc ...)
- Add support for time.Time in criterion 
- Add support for conversion in criterion 
- Add support formulti level output
- Add IN/NOT IN translation to go expression
- Add support for psedo column expr i.e FieldX + 10 / FieldZ

Extended functionality
- Add query options
  - at hoc select
  - indexed data select

- Add multi level output (currently mapper work ony leaf level)
- Add SQL function support
  - COALESCE
  - CASE/IF
  - DATE (FORMAT/SUB/ADD)
  - CURRENT_TIMESTAMP
  - UNNEST
  - 
- Add GROUP BY support
  - ARRAY_AGG
  - STRING_AGG
  