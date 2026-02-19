# rnd_double_array

Generates a random array of doubles.

## Syntax

rnd_double_array(nDims)
rnd_double_array(nDims, nanRate)
rnd_double_array(nDims, nanRate, maxDimLength)
rnd_double_array(nDims, nanRate, 0, dim1Len, dim2Len, ...)


## Parameters

- `nDims` — number of dimensions  
- `nanRate` — controls the probability that a generated value will be `NaN`.  
  Example: `0.1` → about 10% of elements are `NaN`.  
- `maxDimLength` — maximum size of each dimension  
- additional arguments — fixed lengths for each dimension

## Example

```sql
SELECT rnd_double_array(5, 0.2);
