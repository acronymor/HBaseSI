# Command

```bash
mvn clean pmd:check
mvn clean package -DskipTests
```


# Table Struct

## dataTable

tableName | rowKey | qualifier[f:c1] | qualifier[f:c2]       
:--------:|:------:|:-------------:|:---------
test      | row01  |   value0101   |   value0102
test      | row02  |   value0201   |   value0202
test      | row03  |   value0301   |   value0302

## idxTable

tableName      | rowKey
:-------------:|:----------------
test_f_c1_sidx | salt + value0101 + row01
test_f_c1_sidx | salt + value0201 + row02
test_f_c1_sidx | salt + value0301 + row03

## idxTable

tableName      | rowKey
:-------------:|:----------------
test_f_c2_sidx | salt + value0102 + row01
test_f_c2_sidx | salt + value0202 + row02
test_f_c2_sidx | salt + value0302 + row03
